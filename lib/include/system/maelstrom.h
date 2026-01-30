#pragma once

#include <boost/asio/thread_pool.hpp>
#include <boost/json.hpp>
#include <boost/optional.hpp>

#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace maelstrom {

using namespace std;

template <typename... Args>
auto
raiseRuntimeError(Args &&...args) -> void
{
    std::stringstream errorStream;
    (errorStream << ... << args);
    const auto error = errorStream.str();
    throw std::runtime_error(error);
}

class Message {
public:
    explicit Message(const std::string &strMessage);

    Message(std::string sender, std::string recipent, std::string type,
        boost::json::object body = boost::json::object(),
        const boost::optional<int> &msgId = boost::none,
        const boost::optional<int> &inReplyTo = boost::none);

    [[nodiscard]] auto getSender() const -> std::string;
    [[nodiscard]] auto getRecipient() const -> std::string;
    [[nodiscard]] auto getType() const -> std::string;
    [[nodiscard]] auto getBody() const -> const boost::json::object &;
    [[nodiscard]] auto getMsgId() const -> boost::optional<int>;
    [[nodiscard]] auto getInReplyTo() const -> boost::optional<int>;

    auto send() const -> void;

private:
    std::string _sender;
    std::string _recipient;
    std::string _type;
    boost::json::object _body;
    boost::optional<int> _msgId;
    boost::optional<int> _inReplyTo;
};

class MessageHandler {
public:
    virtual ~MessageHandler() = default;

    [[nodiscard]] virtual constexpr auto name() const -> std::string = 0;
    virtual auto handle(const Message &message) -> void = 0;
};

using MessageHandlerPtr = std::unique_ptr<MessageHandler>;

class Node {
public:
    auto run() -> void;
    auto registerMessageHandler(MessageHandlerPtr msgHandler) -> void;
    auto registerInitHandler(
        std::function<bool(std::string_view, std::vector<std::string>)>
            initFunc) -> void;

    auto nodeId() -> boost::optional<std::string> { return _nodeId; }
    auto peerIds() -> std::vector<std::string> { return _peerIds; }

private:
    void _init(const Message &message);
    void handleMessage(const Message &message);

    std::unordered_map<std::string, MessageHandlerPtr> _messageHandlers;
    boost::optional<std::string> _nodeId;
    std::vector<std::string> _peerIds;
    std::vector<std::function<bool(std::string_view, std::vector<std::string>)>>
        _initHandler;
    boost::asio::thread_pool _pool;
};
}
