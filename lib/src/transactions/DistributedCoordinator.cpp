#include "transactions/DistributedCoordinator.h"
#include <boost/json.hpp>

namespace tp_project::transactions {

DistributedCoordinator::DistributedCoordinator(
    NodeID local_id, std::vector<NodeID> nodes) noexcept
    : local_node_id(local_id)
    , all_nodes(std::move(nodes))
{
}

auto DistributedCoordinator::get_primary_node(KeyType key) const noexcept -> NodeID
{
    size_t hash_value = std::hash<KeyType>{}(key);
    return static_cast<NodeID>(hash_value % all_nodes.size());
}

auto DistributedCoordinator::is_local(KeyType key) const noexcept -> bool
{
    return get_primary_node(key) == local_node_id;
}

void DistributedCoordinator::set_nodes(const std::vector<NodeID>& nodes) noexcept
{
    all_nodes = nodes;
}

auto DistributedCoordinator::create_lock_request(
    TransactionId tx_id,
    KeyType key,
    uint64_t timestamp) -> boost::json::value
{
    boost::json::object msg;
    msg["type"] = static_cast<uint8_t>(DistTxMessage::LOCK_REQUEST);
    msg["tx_id"] = tx_id;
    msg["key"] = key;
    msg["timestamp"] = timestamp;
    return msg;
}

auto DistributedCoordinator::create_lock_response(
    TransactionId tx_id,
    bool granted) -> boost::json::value
{
    boost::json::object msg;
    msg["type"] = static_cast<uint8_t>(DistTxMessage::LOCK_GRANTED);
    msg["tx_id"] = tx_id;
    msg["granted"] = granted;
    return msg;
}

auto DistributedCoordinator::create_lock_release(
    TransactionId tx_id,
    KeyType key) -> boost::json::value
{
    boost::json::object msg;
    msg["type"] = static_cast<uint8_t>(DistTxMessage::LOCK_RELEASE);
    msg["tx_id"] = tx_id;
    msg["key"] = key;
    return msg;
}

auto DistributedCoordinator::parse_lock_request(
    const boost::json::value& msg,
    TransactionId& out_tx_id,
    KeyType& out_key,
    uint64_t& out_timestamp) -> bool
{
    try {
        if (!msg.is_object()) {
            return false;
        }

        const auto& obj = msg.as_object();

        if (!obj.contains("tx_id") || !obj.contains("key") || !obj.contains("timestamp")) {
            return false;
        }

        out_tx_id = obj.at("tx_id").as_uint64();
        out_key = static_cast<KeyType>(obj.at("key").as_int64());
        out_timestamp = obj.at("timestamp").as_uint64();

        return true;
    } catch (...) {
        return false;
    }
}

auto DistributedCoordinator::parse_lock_response(
    const boost::json::value& msg,
    TransactionId& out_tx_id,
    bool& out_granted) -> bool
{
    try {
        if (!msg.is_object()) {
            return false;
        }

        const auto& obj = msg.as_object();

        if (!obj.contains("tx_id") || !obj.contains("granted")) {
            return false;
        }

        out_tx_id = obj.at("tx_id").as_uint64();
        out_granted = obj.at("granted").as_bool();

        return true;
    } catch (...) {
        return false;
    }
}

} // namespace tp_project::transactions
