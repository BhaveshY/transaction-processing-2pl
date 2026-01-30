#pragma once

#include "transactions/DistributedCoordinator.h"
#include "transactions/LockManager.h"
#include "transactions/TransactionManager.h"
#include "transactions/Operations.h"

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <future>
#include <condition_variable>
#include <thread>
#include <utility>

namespace tp_project {

namespace storage {
class KVStore;
}

namespace transactions {

class TransactionServer;

namespace impl {

class TwoPLScheduler final : public TransactionManager {
    std::unique_ptr<LockManager> lock_manager;
    std::unique_ptr<DistributedCoordinator> coordinator;
    std::atomic<TransactionId> next_tx_id{1};
    static constexpr int MAX_ACQUIRE_ATTEMPTS = 100;
    NodeID local_node_id = 0;
    size_t num_nodes = 1;
    mutable std::mutex distributed_mutex;
    std::unordered_map<TransactionId, std::unordered_map<NodeID, std::unordered_set<KeyType>>> pending_remote_locks;
    std::condition_variable remote_lock_cv;
    std::unordered_map<TransactionId, std::unordered_set<KeyType>> granted_remote_locks;
    std::unordered_map<TransactionId, std::unordered_map<KeyType, ValueContainer>> remote_results;

    struct PairHash {
        std::size_t operator()(const std::pair<NodeID, KeyType>& p) const noexcept {
            return std::hash<NodeID>{}(p.first) ^ (std::hash<KeyType>{}(p.second) << 1);
        }
    };

    std::unordered_map<TransactionId, std::unordered_set<std::pair<NodeID, KeyType>, PairHash>> pending_remote_ops;
    std::condition_variable remote_op_cv;
    std::thread message_listener_thread;
    std::atomic<bool> stop_listener{false};

public:
    TwoPLScheduler(storage::KVStore::pointer&& store,
                   TransactionServer::pointer&& srv) noexcept;

    ~TwoPLScheduler() override;

    TwoPLScheduler(const TwoPLScheduler&) = delete;
    TwoPLScheduler& operator=(const TwoPLScheduler&) = delete;
    TwoPLScheduler(TwoPLScheduler&&) = delete;
    TwoPLScheduler& operator=(TwoPLScheduler&&) = delete;

    auto execute_transaction(Transaction&& tx)
        -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>> override;

    [[nodiscard]] auto is_local_key(KeyType key) const -> bool;

    auto start(std::string_view nodeID,
               const std::vector<std::string>& peers) -> bool override;

private:
    auto execute_with_distributed_support(const Transaction& tx, TransactionId tx_id)
        -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>>;

    auto execute_with_id(const Transaction& tx, TransactionId tx_id)
        -> std::pair<std::vector<std::pair<KeyType, std::optional<ValueContainer>>>, bool>;

    auto acquire_all_locks(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
                           TransactionId tx_id) -> bool;

    auto acquire_remote_locks_with_retry(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& remote_keys,
                                         TransactionId tx_id) -> bool;

    void release_all_locks(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
                           TransactionId tx_id);

    auto execute_remote_operation(NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
        -> std::optional<ValueContainer>;

    auto execute_remote_op(NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
        -> std::future<std::optional<ValueContainer>>;

    void start_message_listener();
    void stop_message_listener();
    void handle_peer_message(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_lock_request(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_lock_response(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_lock_release(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_operation_request(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_operation_response(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_prepare_request(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_commit_request(system::Peer::id peer_id, const boost::json::object& msg);
    void handle_abort_request(system::Peer::id peer_id, const boost::json::object& msg);
};

} // namespace impl
} // namespace transactions
} // namespace tp_project
