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
    if (!nodeID.empty() && nodeID[0] == 'n') {
        const auto to_int = nodeID.substr(1);
        if (std::from_chars(to_int.data(), to_int.data() + to_int.size(), _id).ec != std::errc{}) {
            return false;
        }
        local_node_id = _id;
    }

    if (!peers.empty()) {
        std::vector<NodeID> node_ids;
        node_ids.push_back(local_node_id);
        for (const auto& peer : peers) {
            if (!peer.empty() && peer[0] == 'n') {
                node_ids.push_back(std::stoi(std::string(peer.substr(1))));
            }
        }

        std::sort(node_ids.begin(), node_ids.end());
        node_ids.erase(std::unique(node_ids.begin(), node_ids.end()), node_ids.end());

        num_nodes = node_ids.size();
        coordinator = std::make_unique<DistributedCoordinator>(local_node_id, node_ids);
        start_message_listener();
    }

    _server->startup(_id, {});

    return true;
}

void TwoPLScheduler::start_message_listener()
{
    stop_listener = false;
    message_listener_thread = std::thread([this]() {
        while (!stop_listener) {
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

        auto result = lock_manager->acquire(key, tx_id);

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

        if (msg.contains("value") && !msg.at("value").is_null()) {
            const auto& value_arr = msg.at("value").as_array();
            ValueContainer result;
            for (const auto& val : value_arr) {
                result.push_back(static_cast<ValueType>(val.as_int64()));
            }
            remote_results[tx_id][key] = result;
        } else {
            remote_results[tx_id][key] = ValueContainer{};
        }

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
    boost::json::object response;
    response["type"] = MessageType::TXN_COMMIT;

    if (msg.contains("tx_id")) {
        response["tx_id"] = msg.at("tx_id");
    } else {
        response["tx_id"] = static_cast<uint64_t>(0);
    }
    _server->send(peer_id, response);
}

void TwoPLScheduler::handle_commit_request(system::Peer::id peer_id, const boost::json::object& msg)
{
}

void TwoPLScheduler::handle_abort_request(system::Peer::id peer_id, const boost::json::object& msg)
{
}

auto TwoPLScheduler::execute_transaction(Transaction&& tx)
    -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>>
{
    TransactionId tx_id = next_tx_id.fetch_add(1, std::memory_order_relaxed);
    return execute_with_distributed_support(tx, tx_id);
}

std::vector<std::pair<KeyType, std::optional<ValueContainer>>> TwoPLScheduler::execute_with_distributed_support(
    const Transaction& tx, TransactionId tx_id)
{
    std::unordered_map<KeyType, NodeID> key_to_node;
    std::unordered_map<NodeID, std::unordered_set<KeyType>> node_to_keys;

    for (const auto& op : tx) {
        KeyType key = op->key();
        NodeID primary = is_local_key(key) ? local_node_id : coordinator->get_primary_node(key);
        key_to_node[key] = primary;
        node_to_keys[primary].insert(key);
    }

    if (!acquire_all_locks(node_to_keys, tx_id)) {
        return {};
    }

    try {
        std::vector<std::pair<KeyType, std::optional<ValueContainer>>> results;

        for (const auto& op : tx) {
            KeyType key = op->key();
            NodeID primary = key_to_node[key];

            if (primary == local_node_id) {
                auto visitor = NaiveOpExecutor(store);
                op->Accept(&visitor);
                if (op->type() == Operation::Type::READ) {
                    results.emplace_back(key, visitor.result());
                }
            } else {
                auto remote_result = execute_remote_operation(primary, tx_id, op);
                if (remote_result.has_value()) {
                    results.emplace_back(key, remote_result.value());
                }
            }
        }

        release_all_locks(node_to_keys, tx_id);

        return results;

    } catch (...) {
        release_all_locks(node_to_keys, tx_id);
        throw;
    }
}

bool TwoPLScheduler::acquire_all_locks(
    const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
    TransactionId tx_id)
{
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

    for (const auto& key : local_keys) {
        auto result = lock_manager->acquire(key, tx_id);
        if (result == LockManager::AcquireResult::ABORTED) {
            lock_manager->release_all(tx_id);
            return false;
        }
    }

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

    for (const auto& [node_id, keys] : remote_keys) {
        for (const auto& key : keys) {
            pending_remote_locks[tx_id][node_id].insert(key);
        }
    }

    for (const auto& [node_id, keys] : remote_keys) {
        for (const auto& key : keys) {
            boost::json::object request;
            request["type"] = MessageType::LOCK_REQUEST;
            request["tx_id"] = tx_id;
            request["key"] = key;
            _server->send(node_id, request);
        }
    }

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

    while (!pending_remote_locks[tx_id].empty()) {
        if (remote_lock_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
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
    lock_manager->release_all(tx_id);

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

        {
            std::lock_guard lock(distributed_mutex);
            pending_remote_ops[tx_id].insert({target_node, op->key()});
        }

        _server->send(target_node, request);

        {
            std::unique_lock lock(distributed_mutex);
            auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);

            while (pending_remote_ops.count(tx_id) &&
                   pending_remote_ops[tx_id].count({target_node, op->key()})) {
                if (remote_op_cv.wait_until(lock, deadline) == std::cv_status::timeout) {
                    pending_remote_ops[tx_id].erase({target_node, op->key()});
                    return std::nullopt;
                }
            }
        }

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
    std::unordered_set<KeyType> held_locks;
    std::vector<std::pair<KeyType, std::optional<ValueContainer>>> results;

    try {
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

        auto visitor = NaiveOpExecutor(store);
        for (const auto& op : tx) {
            op->Accept(&visitor);
            if (op->type() == Operation::Type::READ) {
                results.emplace_back(op->key(), visitor.result());
            }
        }

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
    return std::async(std::launch::deferred, [this, target_node, tx_id, &op]() {
        return execute_remote_operation(target_node, tx_id, op);
    });
}

} // namespace tp_project::transactions::impl
