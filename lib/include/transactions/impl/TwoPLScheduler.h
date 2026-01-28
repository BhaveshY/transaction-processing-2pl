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

/**
 * Two-Phase Locking (2PL) Scheduler with per-key locking and distributed support.
 *
 * Implements true concurrent transaction execution:
 * - Growing Phase: Acquire locks for all keys before executing operations
 * - Shrinking Phase: Release all locks after all operations complete
 *
 * Uses LockManager for fine-grained, per-key locking, allowing multiple
 * transactions to execute concurrently when they access different keys.
 *
 * When a transaction cannot acquire a required lock (contention), it aborts
 * without executing any operations, ensuring atomicity.
 *
 * For distributed mode, uses DistributedCoordinator to route operations to
 * primary nodes and coordinate across multiple nodes using a two-phase commit protocol.
 */
class TwoPLScheduler final : public TransactionManager {
    /**
     * Lock manager for per-key locking.
     */
    std::unique_ptr<LockManager> lock_manager;

    /**
     * Distributed coordinator for multi-node transactions.
     */
    std::unique_ptr<DistributedCoordinator> coordinator;

    /**
     * Atomic counter for generating transaction IDs.
     * Also serves as timestamps for wait-die (smaller = older).
     */
    std::atomic<TransactionId> next_tx_id{1};

    /**
     * Maximum number of lock acquisition attempts before giving up.
     */
    static constexpr int MAX_ACQUIRE_ATTEMPTS = 100;

    /**
     * Local node ID in the cluster.
     */
    NodeID local_node_id = 0;

    /**
     * Total number of nodes in the cluster.
     */
    size_t num_nodes = 1;

    /**
     * Mutex for distributed transaction coordination.
     */
    mutable std::mutex distributed_mutex;

    /**
     * Map to track pending remote lock requests.
     * TransactionId -> map of node_id to set of keys we're waiting for.
     */
    std::unordered_map<TransactionId, std::unordered_map<NodeID, std::unordered_set<KeyType>>> pending_remote_locks;

    /**
     * Condition variable for waiting on remote lock responses.
     */
    std::condition_variable remote_lock_cv;

    /**
     * Map to track granted remote locks.
     * TransactionId -> set of keys granted by remote nodes.
     */
    std::unordered_map<TransactionId, std::unordered_set<KeyType>> granted_remote_locks;

    /**
     * Map to store remote operation results.
     * TransactionId -> map of key to result value.
     */
    std::unordered_map<TransactionId, std::unordered_map<KeyType, ValueContainer>> remote_results;

    /**
     * Hash function for std::pair used in pending_remote_ops.
     */
    struct PairHash {
        std::size_t operator()(const std::pair<NodeID, KeyType>& p) const noexcept {
            return std::hash<NodeID>{}(p.first) ^ (std::hash<KeyType>{}(p.second) << 1);
        }
    };

    /**
     * Map to track pending remote operations.
     * TransactionId -> set of (node_id, key) pairs we're waiting for.
     */
    std::unordered_map<TransactionId, std::unordered_set<std::pair<NodeID, KeyType>, PairHash>> pending_remote_ops;

    /**
     * Condition variable for waiting on remote operation responses.
     */
    std::condition_variable remote_op_cv;

    /**
     * Background thread for listening to incoming messages from peers.
     */
    std::thread message_listener_thread;

    /**
     * Flag to stop the message listener thread.
     */
    std::atomic<bool> stop_listener{false};

public:
    /**
     * Construct a new TwoPLScheduler.
     *
     * @param store The key-value store for persistent storage
     * @param srv The transaction server for distributed communication
     */
    TwoPLScheduler(storage::KVStore::pointer&& store,
                   TransactionServer::pointer&& srv) noexcept;

    ~TwoPLScheduler() override;

    // Non-copyable, non-movable
    TwoPLScheduler(const TwoPLScheduler&) = delete;
    TwoPLScheduler& operator=(const TwoPLScheduler&) = delete;
    TwoPLScheduler(TwoPLScheduler&&) = delete;
    TwoPLScheduler& operator=(TwoPLScheduler&&) = delete;

    /**
     * Execute a transaction using Two-Phase Locking with per-key locking.
     *
     * Algorithm:
     * 1. Assign unique transaction ID
     * 2. Separate local and remote operations (based on key routing)
     * 3. Growing Phase: Acquire all locks (local via LockManager, remote via RPC)
     * 4. Execute Phase: Execute operations (local directly, remote via RPC)
     * 5. Shrinking Phase: Release all locks
     *
     * @param tx The transaction to execute
     * @return Vector of (key, value) pairs for read operations
     */
    auto execute_transaction(Transaction&& tx)
        -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>> override;

    /**
     * Check if a key is managed locally (this node is the primary).
     *
     * @param key The key to check
     * @return true if this node is the primary for this key
     */
    [[nodiscard]] auto is_local_key(KeyType key) const -> bool;

    /**
     * Start the scheduler with the given node ID and peers.
     *
     * @param nodeID This node's identifier
     * @param peers List of peer node addresses
     * @return true if started successfully
     */
    auto start(std::string_view nodeID,
               const std::vector<std::string>& peers) -> bool override;

private:
    /**
     * Execute transaction with distributed support.
     *
     * @param tx The transaction to execute
     * @param tx_id The transaction ID to use
     * @return Vector of (key, value) pairs for read operations
     */
    auto execute_with_distributed_support(const Transaction& tx, TransactionId tx_id)
        -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>>;

    /**
     * Internal execution with explicit transaction ID (single-node fallback).
     *
     * @param tx The transaction to execute
     * @param tx_id The transaction ID to use
     * @return Pair of (results vector, success flag)
     */
    auto execute_with_id(const Transaction& tx, TransactionId tx_id)
        -> std::pair<std::vector<std::pair<KeyType, std::optional<ValueContainer>>>, bool>;

    /**
     * Acquire all locks (local and remote) for a transaction.
     *
     * @param node_to_keys Map of node ID to set of keys to lock
     * @param tx_id The transaction ID
     * @return true if all locks acquired, false otherwise
     */
    auto acquire_all_locks(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
                           TransactionId tx_id) -> bool;

    /**
     * Acquire locks for remote keys via RPC with retry logic.
     *
     * @param remote_keys Map of target node to set of keys to lock
     * @param tx_id The transaction ID
     * @return true if all locks acquired, false otherwise
     */
    auto acquire_remote_locks_with_retry(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& remote_keys,
                                         TransactionId tx_id) -> bool;

    /**
     * Release all locks (local and remote) for a transaction.
     *
     * @param node_to_keys Map of node ID to set of keys to release
     * @param tx_id The transaction ID
     */
    void release_all_locks(const std::unordered_map<NodeID, std::unordered_set<KeyType>>& node_to_keys,
                           TransactionId tx_id);

    /**
     * Execute a remote operation on another node (synchronous).
     *
     * @param target_node The node to execute the operation on
     * @param tx_id The transaction ID
     * @param op The operation to execute
     * @return Optional value container with result
     */
    auto execute_remote_operation(NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
        -> std::optional<ValueContainer>;

    /**
     * Execute a remote operation on another node (returns future).
     *
     * @param target_node The node to execute the operation on
     * @param tx_id The transaction ID
     * @param op The operation to execute
     * @return Future containing the result
     */
    auto execute_remote_op(NodeID target_node, TransactionId tx_id, const Operation::pointer& op)
        -> std::future<std::optional<ValueContainer>>;

    /**
     * Start the background message listener thread.
     */
    void start_message_listener();

    /**
     * Stop the background message listener thread.
     */
    void stop_message_listener();

    /**
     * Handle an incoming message from a peer.
     *
     * @param peer_id The peer that sent the message
     * @param msg The message body
     */
    void handle_peer_message(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a remote lock request.
     *
     * @param peer_id The peer requesting the lock
     * @param msg The message containing lock request details
     */
    void handle_lock_request(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a remote lock response (granted or denied).
     *
     * @param peer_id The peer responding to the lock request
     * @param msg The message containing lock response details
     */
    void handle_lock_response(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a remote lock release.
     *
     * @param peer_id The peer releasing the lock
     * @param msg The message containing lock release details
     */
    void handle_lock_release(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a remote operation request.
     *
     * @param peer_id The peer requesting the operation
     * @param msg The message containing operation details
     */
    void handle_operation_request(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a remote operation response.
     *
     * @param peer_id The peer responding with the operation result
     * @param msg The message containing operation result details
     */
    void handle_operation_response(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a prepare phase request for two-phase commit.
     *
     * @param peer_id The peer requesting prepare
     * @param msg The message containing prepare details
     */
    void handle_prepare_request(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle a commit request for two-phase commit.
     *
     * @param peer_id The peer requesting commit
     * @param msg The message containing commit details
     */
    void handle_commit_request(system::Peer::id peer_id, const boost::json::object& msg);

    /**
     * Handle an abort request for two-phase commit.
     *
     * @param peer_id The peer requesting abort
     * @param msg The message containing abort details
     */
    void handle_abort_request(system::Peer::id peer_id, const boost::json::object& msg);
};

} // namespace impl
} // namespace transactions
} // namespace tp_project
