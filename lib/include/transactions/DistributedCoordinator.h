#pragma once

#include "transactions/LockManager.h"
#include "transactions/TransactionServer.h"

#include <cstdint>
#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

namespace tp_project {

namespace system {
struct Peer;
}

namespace transactions {

/**
 * Message types for distributed transaction coordination.
 */
enum class DistTxMessage : uint8_t {
    LOCK_REQUEST,      ///< Request lock on remote key
    LOCK_GRANTED,      ///< Lock granted successfully
    LOCK_DENIED,       ///< Lock denied (transaction should abort)
    LOCK_RELEASE,      ///< Release lock on remote key
    OPERATION_REQUEST, ///< Execute operation on remote key
    OPERATION_RESULT,  ///< Result of remote operation
};

/**
 * Transaction ID for distributed coordination.
 */
using TransactionId = uint64_t;

/**
 * Node ID type.
 */
using NodeID = system::Peer::id;

/**
 * Coordinator for distributed transactions across multiple nodes.
 * 
 * Uses consistent hashing to route each key to a primary node:
 * - Each key has exactly one primary node (hash(key) % num_nodes)
 * - The primary node manages all locks for that key
 * - Remote lock requests are sent to the key's primary node
 * 
 * This design allows parallelism across different keys while ensuring
 * serializable execution through the distributed lock protocol.
 */
class DistributedCoordinator {
public:
    /**
     * Information about a distributed transaction.
     */
    struct DistributedTxInfo {
        TransactionId tx_id;
        NodeID originating_node;
        std::vector<NodeID> participating_nodes;
    };

    /**
     * Result of a remote lock request.
     */
    struct LockResult {
        bool granted;
        std::string reason;
    };

private:
    NodeID local_node_id;
    std::vector<NodeID> all_nodes;

public:
    /**
     * Construct a DistributedCoordinator.
     * 
     * @param local_id This node's ID
     * @param nodes All node IDs in the cluster
     */
    DistributedCoordinator(NodeID local_id, std::vector<NodeID> nodes) noexcept;

    ~DistributedCoordinator() = default;

    // Non-copyable, non-movable
    DistributedCoordinator(const DistributedCoordinator&) = delete;
    DistributedCoordinator& operator=(const DistributedCoordinator&) = delete;
    DistributedCoordinator(DistributedCoordinator&&) = delete;
    DistributedCoordinator& operator=(DistributedCoordinator&&) = delete;

    /**
     * Check if a key is managed locally.
     *
     * @param key The key to check
     * @return true if this node is the primary for this key
     */
    [[nodiscard]] auto is_local(KeyType key) const noexcept -> bool;

    /**
     * Determine the primary node for a given key.
     * Uses deterministic hash-based routing for consistency.
     *
     * @param key The key to route
     * @return The node ID that is primary for this key
     */
    [[nodiscard]] NodeID get_primary_node(KeyType key) const noexcept;

    /**
     * Get the local node ID.
     */
    [[nodiscard]] auto get_local_node_id() const noexcept -> NodeID {
        return local_node_id;
    }

    /**
     * Get all nodes in the cluster.
     */
    [[nodiscard]] auto get_all_nodes() const noexcept 
        -> const std::vector<NodeID>& {
        return all_nodes;
    }

    /**
     * Set the node list (for dynamic reconfiguration).
     * 
     * @param nodes The new list of all node IDs
     */
    void set_nodes(const std::vector<NodeID>& nodes) noexcept;

    /**
     * Create a lock request message for a remote node.
     * 
     * @param tx_id The transaction ID
     * @param key The key to lock
     * @param timestamp The transaction timestamp (for wait-die)
     * @return JSON message to send
     */
    [[nodiscard]] static auto create_lock_request(
        TransactionId tx_id,
        KeyType key,
        uint64_t timestamp) -> boost::json::value;

    /**
     * Create a lock response message.
     * 
     * @param tx_id The transaction ID
     * @param granted Whether the lock was granted
     * @return JSON message to send
     */
    [[nodiscard]] static auto create_lock_response(
        TransactionId tx_id,
        bool granted) -> boost::json::value;

    /**
     * Create a lock release message.
     * 
     * @param tx_id The transaction ID
     * @param key The key to unlock
     * @return JSON message to send
     */
    [[nodiscard]] static auto create_lock_release(
        TransactionId tx_id,
        KeyType key) -> boost::json::value;

    /**
     * Parse a lock request message.
     * 
     * @param msg The JSON message
     * @param out_tx_id Output for transaction ID
     * @param out_key Output for key
     * @param out_timestamp Output for timestamp
     * @return true if parsing succeeded
     */
    [[nodiscard]] static auto parse_lock_request(
        const boost::json::value& msg,
        TransactionId& out_tx_id,
        KeyType& out_key,
        uint64_t& out_timestamp) -> bool;

    /**
     * Parse a lock response message.
     * 
     * @param msg The JSON message
     * @param out_tx_id Output for transaction ID
     * @param out_granted Output for granted flag
     * @return true if parsing succeeded
     */
    [[nodiscard]] static auto parse_lock_response(
        const boost::json::value& msg,
        TransactionId& out_tx_id,
        bool& out_granted) -> bool;
};

} // namespace transactions
} // namespace tp_project
