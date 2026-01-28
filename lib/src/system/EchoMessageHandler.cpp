#include "system/EchoMessageHandler.h"
#include "system/maelstrom.h"

namespace tp_project::system {
constexpr auto
EchoMessageHandler::name() const -> std::string
{
    return "echo";
}

void
EchoMessageHandler::handle(const maelstrom::Message &request)
{
    constexpr auto msgType = "echo_ok";

    const auto sender = request.getRecipient();
    const auto recipient = request.getSender();
    const auto &msgBody = request.getBody();
    constexpr auto msgId = boost::none;
    const auto msgInReplyTo = request.getMsgId();

    const maelstrom::Message response { sender, recipient, msgType, msgBody,
        msgId, msgInReplyTo };
    response.send();
}
}