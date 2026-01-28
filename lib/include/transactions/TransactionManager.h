#pragma once

#include "TransactionServer.h"
#include "storage/KVStore.h"
#include "system/Peer.h"

#include <memory>
#include <optional>
#include <unordered_map>
#include <utility>
#include <vector>

namespace tp_project::transactions {
class Transaction;

class TransactionManager {
    std::future<void> server_future;

public:
    using NodeID = system::Peer::id;
    using pointer = std::shared_ptr<TransactionManager>;
protected:
    NodeID _id;
    storage::KVStore::pointer store;
    TransactionServer::pointer _server;

public:
    [[nodiscard]] NodeID id() const { return _id; }

    explicit TransactionManager(storage::KVStore::pointer &&store,
        TransactionServer::pointer &&srv) noexcept;

    virtual ~TransactionManager() = default;

    virtual auto execute_transaction(Transaction &&tx)
        -> std::vector<std::pair<KeyType, std::optional<ValueContainer>>> = 0;

    [[nodiscard]] auto peers() const
        -> const std::unordered_map<system::Peer::id, system::Peer::pointer> &;

    virtual auto start(std::string_view nodeID,
        const std::vector<std::string> &peers) -> bool;

    [[nodiscard]] auto server() const -> const TransactionServer &;

    template <typename TransactionManagerType, typename... Args>
    static auto makeTransactionManager(Args &&...args) -> pointer
    {
        return std::make_unique<TransactionManagerType>(
            std::forward<Args>(args)...);
    }
};
}