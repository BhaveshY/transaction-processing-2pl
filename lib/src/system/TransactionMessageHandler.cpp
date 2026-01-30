#include "system/Logger.h"
#include "system/TransactionMessageHandler.h"
#include "transactions/Transaction.h"
#include "transactions/TransactionManager.h"
namespace tp_project::system {
auto
TransactionMessageHandler::createReply(boost::json::object &inBody,
    const std::vector<std::pair<KeyType, std::optional<ValueContainer>>> &reads)
    -> boost::json::object &
{
    auto curRead = reads.begin();
    for (auto &opsRef = inBody.at("txn").as_array(); auto &val : opsRef) {
        if (val.as_array().at(0).as_string().compare("r") == 0) {
            if (curRead != reads.end() && curRead->first == val.as_array().at(1).as_int64()) {
                if (const auto &insertArr = curRead->second) {
                    auto &arr = val.as_array().at(2).emplace_array();
                    arr.insert(arr.begin(), insertArr->begin(), insertArr->end());
                } else
                    val.as_array().at(2).emplace_null();
                ++curRead;
            } else {
                val.as_array().at(2).emplace_null();
            }
        }
    }

    return inBody;
}

TransactionMessageHandler::TransactionMessageHandler(
    transactions::TransactionManager::pointer tm)
    : manager(std::move(tm))
{
}

void
TransactionMessageHandler::handle(const maelstrom::Message &request)
{
    const auto sender = request.getRecipient();
    const auto recipient = request.getSender();
    auto msgBody = request.getBody();

    try {
        const auto reads = manager->execute_transaction(
            transactions::Transaction(msgBody.at("txn").as_array()));
        constexpr auto msgId = boost::none;
        const auto msgInReplyTo = request.getMsgId();
        const auto responseMsgBody = createReply(msgBody, reads);

        const maelstrom::Message response { sender, recipient, "txn_ok",
            responseMsgBody, msgId, msgInReplyTo };
        response.send();
    } catch (std::exception &e) {
        logger::error("Caught exception during transaction handling: {}",
            e.what());

        const maelstrom::Message response { sender, recipient, "error",
            boost::json::object(), boost::none, request.getMsgId() };
        response.send();
    }
}
}
