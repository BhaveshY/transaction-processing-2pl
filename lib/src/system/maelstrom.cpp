#include <boost/asio/post.hpp>

#include "system/maelstrom.h"

#include <future>
#include <iostream>
#include <numeric>
#include <mutex>
#include <utility>
#include <vector>

namespace maelstrom {

// Mutex for thread-safe console output (replaces osyncstream on macOS)
static std::mutex cout_mutex;
//
//  Message
//
Message::Message(const std::string &strMessage)
{
    auto jsonValue = boost::json::parse(strMessage);
    if (!jsonValue.is_object()) {
        raiseRuntimeError("Invalid message: ", strMessage);
    }

    const auto &jsonObject = jsonValue.as_object();

    _sender = jsonObject.at("src").as_string().c_str();
    _recipient = jsonObject.at("dest").as_string().c_str();
    _body = jsonObject.at("body").as_object();
    _type = _body.at("type").as_string().c_str();

    if (_body.contains("msg_id")) {
        _msgId = _body.at("msg_id").as_int64();
    }

    if (_body.contains("in_reply_to")) {
        _inReplyTo = _body.at("in_reply_to").as_int64();
    }
}

Message::Message(std::string sender, std::string recipent, std::string type,
    boost::json::object body, const boost::optional<int> &msgId,
    const boost::optional<int> &inReplyTo)
    : _sender(std::move(sender))
    , _recipient(std::move(recipent))
    , _type(std::move(type))
    , _body(std::move(body))
    , _msgId(msgId)
    , _inReplyTo(inReplyTo)
{
    _body["type"] = _type;

    if (msgId && inReplyTo) {
        raiseRuntimeError("'msg_Id' and 'in_reply_to' cannot be set together");
    }

    if (_msgId) {
        _body["msg_id"] = *_msgId;
        if (_body.contains("in_reply_to")) {
            _body.erase("in_reply_to");
        }
    }

    if (_inReplyTo) {
        _body["in_reply_to"] = *_inReplyTo;
        if (_body.contains("msg_id")) {
            _body.erase("msg_id");
        }
    }
}

auto
Message::getSender() const -> std::string
{
    return _sender;
}

auto
Message::getRecipient() const -> std::string
{
    return _recipient;
}

auto
Message::getType() const -> std::string
{
    return _type;
}

auto
Message::getBody() const -> const boost::json::object &

{
    return _body;
}

auto
Message::getMsgId() const -> boost::optional<int>
{
    return _msgId;
}

auto
Message::getInReplyTo() const -> boost::optional<int>
{
    return _inReplyTo;
}

auto
Message::send() const -> void
{
    boost::json::object jsonMessage;
    jsonMessage["src"] = _sender;
    jsonMessage["dest"] = _recipient;
    jsonMessage["body"] = _body;
    std::lock_guard lock(cout_mutex);
    std::cout << jsonMessage << std::endl;
}

//
//  Node
//
auto
Node::run() -> void
{
    for (;;) {
        std::string inputLine;
        std::getline(std::cin, inputLine);
        if (inputLine.empty()) {
            break;
        }
        Message request { inputLine };
        handleMessage(request);
    }
}

auto
Node::registerMessageHandler(MessageHandlerPtr msgHandler) -> void
{
    _messageHandlers[msgHandler->name()] = std::move(msgHandler);
}
auto
Node::registerInitHandler(
    std::function<bool(std::string_view, std::vector<std::string>)> initFunc)
    -> void
{
    _initHandler.push_back(std::move(initFunc));
}

auto
Node::_init(const Message &message) -> void
{
    if (_nodeId) {
        raiseRuntimeError("Node: ", *_nodeId, " already initialized");
    }

    auto requestBody = message.getBody();
    _nodeId = requestBody.at("node_id").as_string().c_str();

    _peerIds = {};
    for (const auto &item : requestBody.at("node_ids").as_array()) {
        if (item.as_string() != *_nodeId) // exclude self
        {
            _peerIds.emplace_back(item.as_string().c_str());
        }
    }

    const auto init_state = std::ranges::all_of(_initHandler,
        [this](const auto &handler) -> auto {
            return handler(*nodeId(), peerIds());
        });

    const auto msgType = init_state ? "init_ok" : "error";
    const auto sender = message.getRecipient();
    const auto recipient = message.getSender();
    const auto responseBody = boost::json::object();
    constexpr auto msgId = boost::none;
    const auto msgInReplyTo = message.getMsgId();

    const Message response { sender, recipient, msgType, responseBody, msgId,
        msgInReplyTo };
    response.send();
}

auto
Node::handleMessage(const Message &message) -> void
{
    auto msgType = message.getType();

    if (msgType == "init") {
        _init(message);
        return;
    }

    if (const auto handlersIter = _messageHandlers.find(msgType);
        handlersIter != _messageHandlers.end()) {
        boost::asio::post(_pool, [message, handlersIter]() -> void {
            handlersIter->second->handle(message);
        });
    } else {
        raiseRuntimeError("No handler found for message type", msgType);
    }
}
}