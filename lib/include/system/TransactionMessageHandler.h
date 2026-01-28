#pragma once
#include <boost/json/object.hpp>

#include "storage/Types.h"
#include "system/maelstrom.h"
#include "transactions/TransactionManager.h"

namespace tp_project::transactions {
class TransactionManager;
}

namespace tp_project::system {
class TransactionMessageHandler final : public maelstrom::MessageHandler {
    transactions::TransactionManager::pointer manager;

    static auto createReply(boost::json::object &inBody,
        const std::vector<std::pair<KeyType, std::optional<ValueContainer>>>
            &reads) -> boost::json::object &;

public:
    explicit TransactionMessageHandler(
        transactions::TransactionManager::pointer tm);

    [[nodiscard]] constexpr auto name() const -> std::string override
    {
        return "txn";
    }

    void handle(const maelstrom::Message &request) override;
};
}