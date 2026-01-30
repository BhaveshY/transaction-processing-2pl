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

enum class DistTxMessage : uint8_t {
    LOCK_REQUEST,
    LOCK_GRANTED,
    LOCK_DENIED,
    LOCK_RELEASE,
    OPERATION_REQUEST,
    OPERATION_RESULT,
};

using TransactionId = uint64_t;
using NodeID = system::Peer::id;

class DistributedCoordinator {
public:
    struct DistributedTxInfo {
        TransactionId tx_id;
        NodeID originating_node;
        std::vector<NodeID> participating_nodes;
    };

    struct LockResult {
        bool granted;
        std::string reason;
    };

private:
    NodeID local_node_id;
    std::vector<NodeID> all_nodes;

public:
    DistributedCoordinator(NodeID local_id, std::vector<NodeID> nodes) noexcept;

    ~DistributedCoordinator() = default;

    DistributedCoordinator(const DistributedCoordinator&) = delete;
    DistributedCoordinator& operator=(const DistributedCoordinator&) = delete;
    DistributedCoordinator(DistributedCoordinator&&) = delete;
    DistributedCoordinator& operator=(DistributedCoordinator&&) = delete;

    [[nodiscard]] auto is_local(KeyType key) const noexcept -> bool;

    [[nodiscard]] NodeID get_primary_node(KeyType key) const noexcept;

    [[nodiscard]] auto get_local_node_id() const noexcept -> NodeID {
        return local_node_id;
    }

    [[nodiscard]] auto get_all_nodes() const noexcept
        -> const std::vector<NodeID>& {
        return all_nodes;
    }

    void set_nodes(const std::vector<NodeID>& nodes) noexcept;

    [[nodiscard]] static auto create_lock_request(
        TransactionId tx_id,
        KeyType key,
        uint64_t timestamp) -> boost::json::value;

    [[nodiscard]] static auto create_lock_response(
        TransactionId tx_id,
        bool granted) -> boost::json::value;

    [[nodiscard]] static auto create_lock_release(
        TransactionId tx_id,
        KeyType key) -> boost::json::value;

    [[nodiscard]] static auto parse_lock_request(
        const boost::json::value& msg,
        TransactionId& out_tx_id,
        KeyType& out_key,
        uint64_t& out_timestamp) -> bool;

    [[nodiscard]] static auto parse_lock_response(
        const boost::json::value& msg,
        TransactionId& out_tx_id,
        bool& out_granted) -> bool;
};

} // namespace transactions
} // namespace tp_project
