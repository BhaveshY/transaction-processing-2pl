#include "transactions/impl/TwoPLScheduler.h"
#include "transactions/Transaction.h"
#include "transactions/Operations.h"
#include "transactions/impl/NaiveOpExecutor.h"
#include "storage/Types.h"
#include "system/Logger.h"

#include <charconv>
#include <chrono>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <sstream>
#include <boost/json.hpp>
#include <boost/asio.hpp>

namespace tp_project::transactions::impl {

using tp_project::logger::error;

// Message type constants for distributed coordination
namespace MessageType {
    constexpr const char* LOCK_REQUEST = "lock_req";
    constexpr const char* LOCK_GRANTED = "lock_granted";
    constexpr const char* LOCK_DENIED = "lock_denied";
    constexpr const char* LOCK_RELEASE = "lock_rel";
    constexpr const char* OP_REQUEST = "op_req";
    constexpr const char* OP_RESPONSE = "op_resp";
    constexpr const char* TXN_PREPARE = "txn_prep";
    constexpr const char* TXN_COMMIT = "txn_commit";
    constexpr const char* TXN_ABORT = "txn_abort";
}

TwoPLScheduler::TwoPLScheduler(storage::KVStore::pointer&& store,
                               TransactionServer::pointer&& srv) noexcept
    : TransactionManager(std::move(store), std::move(srv))
    , lock_manager(std::make_unique<LockManager>())
    , coordinator(nullptr)
{
}

TwoPLScheduler::~TwoPLScheduler()
{
    stop_listener = true;
    if (message_listener_thread.joinable()) {
        message_listener_thread.join();
    }
}

auto TwoPLScheduler::start(std::string_view nodeID,
                           const std::vector<std::string>& peers) -> bool
{
    // Extract local node ID from nodeID string (format: "n0", "n1", etc.)
    if (!nodeID.empty() && nodeID[0] == 'n') {
        const auto to_int = nodeID.substr(1);
        if (std::from_chars(to_int.data(), to_int.data() + to_int.size(), _id).ec != std::errc{}) {
            return false;
        }
        local_node_id = _id;
    }

    // Set up coordinator for distributed mode
    if (!peers.empty()) {
        // Build list of all node IDs
        std::vector<NodeID> node_ids;
        node_ids.push_back(local_node_id);
        for (const auto& peer : peers) {
            if (!peer.empty() && peer[0] == 'n') {
                node_ids.push_back(std::stoi(std::string(peer.substr(1))));
            }
        }

        // Sort and deduplicate
        std::sort(node_ids.begin(), node_ids.end());
        node_ids.erase(std::unique(node_ids.begin(), node_ids.end()), node_ids.end());

        num_nodes = node_ids.size();

        // Create coordinator
        coordinator = std::make_unique<DistributedCoordinator>(local_node_id, node_ids);

        // Start message listener for distributed coordination
        start_message_listener();
    }

    // Start the server AFTER _id is set
    _server->startup(_id, {});

    return true;
}

void TwoPLScheduler::start_message_listener()
{
    // For Maelstrom-based distributed mode, messages are handled via callbacks
    // through the TransactionServer. This thread is kept for potential future
    // use with direct peer-to-peer communication.
    stop_listener = false;
    message_listener_thread = std::thread([this]() {
        while (!stop_listener) {
            // Sleep and wait for stop signal
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });
}

void TwoPLScheduler::stop_message_listener()
{
    stop_listener = true;
    if (message_listener_thread.joinable()) {
        message_listener_thread.join();
    }
}

void TwoPLScheduler::handle_peer_message(system::Peer::id peer_id, const boost::json::object& msg)
{
    if (!msg.contains("type")) {
        return;
    }

    std::string type = msg.at("type").as_string().c_str();

    // Route message to appropriate handler
    if (type == MessageType::LOCK_REQUEST) {
        handle_lock_request(peer_id, msg);
    } else if (type == MessageType::LOCK_GRANTED || type == MessageType::LOCK_DENIED) {
        handle_lock_response(peer_id, msg);
    } else if (type == MessageType::LOCK_RELEASE) {
        handle_lock_release(peer_id, msg);
    } else if (type == MessageType::OP_REQUEST) {
        handle_operation_request(peer_id, msg);
    } else if (type == MessageType::OP_RESPONSE) {
        handle_operation_response(peer_id, msg);
    } else if (type == MessageType::TXN_PREPARE) {
        handle_prepare_request(peer_id, msg);
    } else if (type == MessageType::TXN_COMMIT) {
        handle_commit_request(peer_id, msg);
    } else if (type == MessageType::TXN_ABORT) {
        handle_abort_request(peer_id, msg);
    }
}

void TwoPLScheduler::handle_lock_request(system::Peer::id peer_id, const boost::json::object& msg)
{
    try {
        if (!msg.contains("tx_id") || !msg.contains("key")) {
            return;
        }

        TransactionId tx_id = msg.at("tx_id").as_uint64();
        KeyType key = static_cast<KeyType>(msg.at("key").as_int64());

        // Try to acquire the lock locally using our LockManager
        auto result = lock_manager->acquire(key, tx_id);

        // Send response back to requesting node
        boost::json::object response;
        response["type"] = (result == LockManager::AcquireResult::ACQUIRED) ?
                          MessageType::LOCK_GRANTED : MessageType::LOCK_DENIED;
        response["tx_id"] = tx_id;
        response["key"] = key;

        _server->send(peer_id, response);
    } catch (const std::exception& e) {
        error("Error handling lock request: {}", e.what());
    }
}

void TwoPLScheduler::handle_lock_response(system::Peer::id peer_id, const boost::json::object& msg)
{
    try {
        if (!msg.contains("tx_id") || !msg.contains("key")) {
            return;
        }

        TransactionId tx_id = msg.at("tx_id").as_uint64();
        KeyType key = static_cast<KeyType>(msg.at("key").as_int64());
        bool granted = (msg.at("type").as_string().c_str() == MessageType::LOCK_GRANTED);

        std::lock_guard lock(distributed_mutex);

        if (granted) {
            // Remove from pending locks
            if (pending_remote_locks.count(tx_id) &&
                pending_remote_locks[tx_id].count(peer_id) &&
                pending_remote_locks[tx_id][peer_id].count(key)) {
                pending_remote_locks[tx_id][peer_id].erase(key);
                if (pending_remote_locks[tx_id][peer_id].empty()) {
                    pending_remote_locks[tx_id].erase(peer_id);
                }
                if (pending_remote_locks[tx_id].empty()) {
                    remote_lock_cv.notify_all();
                }
            }
        } else {
            // Lock denied - abort transaction
            pending_remote_locks.erase(tx_id);
            remote_lock_cv.notify_all();
        }
    } catch (const std::exception& e) {
        error("Error handling lock response: {}", e.what());
    }
}

void TwoPLScheduler::handle_lock_release(system::Peer::id peer_id, const boost::json::object& msg)
{
    try {
        if (!msg.contains("tx_id") || !msg.contains("key")) {
            return;
        }

        TransactionId tx_id = msg.at("tx_id").as_uint64();
        KeyType key = static_cast<KeyType>(msg.at("key").as_int64());

        // Release the lock locally
        lock_manager->release(key, tx_id);
    } catch (const std::exception& e) {
        error("Error handling lock release: {}", e.what());
    }
}

void TwoPLScheduler::handle_operation_request(system::Peer::id peer_id, const boost::json::object& msg)
{
    try {
        if (!msg.contains("tx_id") || !msg.contains("op_type") || !msg.contains("key")) {
            return;
        }

        TransactionId tx_id = msg.at("tx_id").as_uint64();
        std::string op_type = msg.at("op_type").as_string().c_str();
        KeyType key = static_cast<KeyType>(msg.at("key").as_int64());

        // Execute the operation locally
        auto visitor = NaiveOpExecutor(store);
        std::optional<ValueContainer> result;

        if (op_type == "append") {
            if (!msg.contains("value")) {
                return;
            }
            ValueType value = static_cast<ValueType>(msg.at("value").as_int64());
            auto op = AppendOp(key, value);
            op.Accept(&visitor);
        } else if (op_type == "read") {
            auto op = ReadOp(key);
            op.Accept(&visitor);
            result = visitor.result();
        }

        // Send response back to requesting node
        boost::json::object response;
        response["type"] = MessageType::OP_RESPONSE;
        response["tx_id"] = tx_id;
        response["key"] = key;

        if (result) {
            boost::json::array result_arr;
            for (const auto& val : *result) {
                result_arr.push_back(val);
            }
            response["value"] = result_arr;
        } else {
            response["value"] = nullptr;
        }

        _server->send(peer_id, response);
    } catch (const std::exception& e) {
        error("Error handling operation request: {}", e.what());
    }
}

void TwoPLScheduler::handle_operation_response(system::Peer::id peer_id, const boost::json::object& msg)
{
    try {
        if (!msg.contains("tx_id") || !msg.contains("key")) {
            return;
        }

        TransactionId tx_id = msg.at("tx_id").as_uint64();
        KeyType key = static_cast<KeyType>(msg.at("key").as_int64());

        std::lock_guard lock(distributed_mutex);

        // Store the result for this transaction
        if (msg.contains("value") && !msg.at("value").is_null()) {
            const auto& value_arr = msg.at("value").as_array();
            ValueContainer result;
            for (const auto& val : value_arr) {
                result.push_back(static_cast<ValueType>(val.as_int64()));
            }
            remote_results[tx_id][key] = result;
        } else {
            // Empty/null result - key doesn't exist yet
            remote_results[tx_id][key] = ValueContainer{};
        }

        // Mark that we received a response for this key
        pending_remote_ops[tx_id].erase({peer_id, key});
        if (pending_remote_ops[tx_id].empty()) {
            remote_op_cv.notify_all();
        }
    } catch (const std::exception& e) {
        error("Error handling operation response: {}", e.what());
    }
}

void TwoPLScheduler::handle_prepare_request(system::Peer::id peer_id, const boost::json::object& msg)
{
    // Handle prepare phase of two-phase commit
    // For now, we always vote yes
    boost::json::object response;
    response["type"] = MessageType::TXN_COMMIT;

    // Get tx_id if present, otherwise use 0
    if (msg.contains("tx_id")) {
        response["tx_id"] = msg.at("tx_id");
    } else {
        response["tx_id"] = static_cast<uint64_t>(0);
    }
    _server->send(peer_id, response);
}

void TwoPLScheduler::handle_commit_request(system::Peer::id peer_id, const boost::json::object& msg)
{
    // Handle commit request - transaction is committed
}

void TwoPLScheduler::handle_abort_request(system::Peer::id peer_id, const boost::json::object& msg)
{
    // Handle abort request - transaction is aborted
}

auto TwoPLScheduler::execute_transaction(Transaction&& tx)
    -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>>
{
    // Get a transaction ID for this attempt
    TransactionId tx_id = next_tx_id.fetch_add(1, std::memory_order_relaxed);

    // For distributed transactions, we need to coordinate with other nodes
    // But first, try the basic single-node approach
    return execute_with_distributed_support(tx, tx_id);
}

std::vector<std::pair<KeyType, std::optional<ValueContainer>>> TwoPLScheduler::execute_with_distributed_support(
    const Transaction& tx, TransactionId tx_id)
{
    std::unordered_map<KeyType, NodeID> key_to_node;
    std::unordered_map<NodeID, std::unordered_set<KeyType>> node_to_keys;

    // Categorize keys by their primary node
    for (const auto& op : tx) {
        KeyType key = op->key();
        NodeID primary = is_local_key(key) ? local_node_id : coordinator->get_primary_node(key);
        key_to_node[key] = primary;
        node_to_keys[primary].insert(key);
    }

    // Phase 1: Acquire all locks (local and remote)
    if (!acquire_all_locks(node_to_keys, tx_id)) {
        // Lock acquisition failed - return empty
        return {};
    }

    try {
        std::vector<std::pair<KeyType, std::optional<ValueContainer>>> results;

        // Phase 2: Execute operations
        for (const auto& op : tx) {
            KeyType key = op->key();
            NodeID primary = key_to_node[key];

            if (primary == local_node_id) {
                // Execute locally
                auto visitor = NaiveOpExecutor(store);
                op->Accept(&visitor);
                if (op->type() == Operation::Type::READ) {
                    results.emplace_back(key, visitor.result());
                }
            } else {
                // Execute remotely and get result
                auto remote_result = execute_remote_operation(primary, tx_id, op);
                if (remote_result.has_value()) {
                    results.emplace_back(key, remote_result.value());
                }
            }
        }

        // Phase 3: Release all locks
        release_all_locks(node_to_keys, tx_id);

        return results;

    } catch (...) {
        // Exception occurred - clean up locks
        release_all_locks(node_to_keys, tx_id);
        throw;
    }
}

bool TwoPLScheduler::acquire_all_locks(
    const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
    TransactionId tx_id)
{
    // Separate local and remote keys
    std::unordered_set<KeyType> local_keys;
    std::unordered_map<NodeID, std::unordered_set<KeyType>> remote_keys;

    for (const auto& [node_id, keys] : node_to_keys) {
        if (node_id == local_node_id) {
            for (const auto& key : keys) {
                local_keys.insert(key);
            }
        } else {
            remote_keys[node_id] = keys;
        }
    }

    // Acquire local locks
    for (const auto& key : local_keys) {
        auto result = lock_manager->acquire(key, tx_id);
        if (result == LockManager::AcquireResult::ABORTED) {
            lock_manager->release_all(tx_id);
            return false;
        }
    }

    // Acquire remote locks
    if (!remote_keys.empty()) {
        if (!acquire_remote_locks_with_retry(remote_keys, tx_id)) {
            lock_manager->release_all(tx_id);
            return false;
        }
    }

    return true;
}

bool TwoPLScheduler::acquire_remote_locks_with_retry(
    const std::unordered_map<NodeID, std::unordered_set<KeyType>>& remote_keys,
    TransactionId tx_id)
{
    std::unique_lock lock(distributed_mutex);

    // Initialize pending lock tracking
    for (const auto& [node_id, keys] : remote_keys) {
        for (const auto& key : keys) {
            pending_remote_locks[tx_id][node_id].insert(key);
        }
    }

    // Send lock requests to all relevant nodes
    for (const auto& [node_id, keys] : remote_keys) {
        for (const auto& key : keys) {
            boost::json::object request;
            request["type"] = MessageType::LOCK_REQUEST;
            request["tx_id"] = tx_id;
            request["key"] = key;
            _server->send(node_id, request);
        }
    }

    // Wait for all responses with timeout
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

    while (!pending_remote_locks[tx_id].empty()) {
        if (remote_lock_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
            // Timeout - transaction should abort
            // Send lock releases for any acquired locks
            for (const auto& [node_id, keys] : pending_remote_locks[tx_id]) {
                for (const auto& key : keys) {
                    boost::json::object request;
                    request["type"] = MessageType::LOCK_RELEASE;
                    request["tx_id"] = tx_id;
                    request["key"] = key;
                    _server->send(node_id, request);
                }
            }
            pending_remote_locks.erase(tx_id);
            return false;
        }
    }

    return true;
}

void TwoPLScheduler::release_all_locks(
    const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
    TransactionId tx_id)
{
    // Release local locks
    lock_manager->release_all(tx_id);

    // Release remote locks
    for (const auto& [node_id, keys] : node_to_keys) {
        if (node_id != local_node_id) {
            for (const auto& key : keys) {
                boost::json::object request;
                request["type"] = MessageType::LOCK_RELEASE;
                request["tx_id"] = tx_id;
                request["key"] = key;
                _server->send(node_id, request);
            }
        }
    }
}

std::optional<ValueContainer> TwoPLScheduler::execute_remote_operation(
    NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
{
    try {
        boost::json::object request;
        request["type"] = MessageType::OP_REQUEST;
        request["tx_id"] = tx_id;
        request["key"] = op->key();

        if (op->type() == Operation::Type::APPEND) {
            request["op_type"] = "append";
            request["value"] = op->value();
        } else {
            request["op_type"] = "read";
        }

        // Track that we're waiting for this operation
        {
            std::lock_guard lock(distributed_mutex);
            pending_remote_ops[tx_id].insert({target_node, op->key()});
        }

        // Send request
        _server->send(target_node, request);

        // Wait for response with timeout
        {
            std::unique_lock lock(distributed_mutex);
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

            while (pending_remote_ops.count(tx_id) &&
                   pending_remote_ops[tx_id].count({target_node, op->key()})) {
                if (remote_op_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
                    // Timeout - operation failed
                    pending_remote_ops[tx_id].erase({target_node, op->key()});
                    return std::nullopt;
                }
            }
        }

        // Retrieve result
        std::lock_guard lock(distributed_mutex);
        if (remote_results.count(tx_id) && remote_results[tx_id].count(op->key())) {
            return remote_results[tx_id][op->key()];
        }

        return std::nullopt;
    } catch (...) {
        return std::nullopt;
    }
}

auto TwoPLScheduler::execute_with_id(const Transaction& tx, TransactionId tx_id)
    -> std::pair<std::vector<std::pair<KeyType, std::optional<ValueContainer>>>, bool>
{
    // For single-node mode or when coordinator is not set, use the simple approach
    std::unordered_set<KeyType> held_locks;
    std::vector<std::pair<KeyType, std::optional<ValueContainer>>> results;

    try {
        // Phase 1: Acquire all locks
        std::unordered_set<KeyType> keys_to_lock;
        for (const auto& op : tx) {
            keys_to_lock.insert(op->key());
        }

        for (const auto& key : keys_to_lock) {
            auto acquire_result = lock_manager->acquire(key, tx_id);

            if (acquire_result == LockManager::AcquireResult::ABORTED) {
                lock_manager->release_all(tx_id);
                return {std::vector<std::pair<KeyType, std::optional<ValueContainer>>>{}, false};
            }
            held_locks.insert(key);
        }

        // Phase 2: Execute all operations
        auto visitor = NaiveOpExecutor(store);
        for (const auto& op : tx) {
            op->Accept(&visitor);
            if (op->type() == Operation::Type::READ) {
                results.emplace_back(op->key(), visitor.result());
            }
        }

        // Phase 3: Release all locks
        lock_manager->release_all(tx_id);

        return {std::move(results), true};

    } catch (...) {
        lock_manager->release_all(tx_id);
        throw;
    }
}

auto TwoPLScheduler::is_local_key(KeyType key) const -> bool
{
    if (coordinator) {
        return coordinator->is_local(key);
    }
    return true;
}

auto TwoPLScheduler::execute_remote_op(NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
    -> std::future<std::optional<ValueContainer>>
{
    // Wrapper for the new execute_remote_operation method
    // Note: op must remain valid for the lifetime of the returned future
    return std::async(std::launch::deferred, [this, target_node, tx_id, &op]() {
        return execute_remote_operation(target_node, tx_id, op);
    });
}

} // namespace tp_project::transactions::impl
