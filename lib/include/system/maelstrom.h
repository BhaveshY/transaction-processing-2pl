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

// Helper function for raising runtime errors.
template <typename... Args>
auto
raiseRuntimeError(Args &&...args) -> void
{
    std::stringstream errorStream;
    (errorStream << ... << args);
    const auto error = errorStream.str();
    throw std::runtime_error(error);
}

/**
 * Message class that is used to encapsulate 'Maelstrom' messages.
 */
class Message {
public:
    explicit Message(const std::string &strMessage);

    Message(std::string sender, std::string recipent, std::string type,
        boost::json::object body = boost::json::object(),
        const boost::optional<int> &msgId = boost::none,
        const boost::optional<int> &inReplyTo = boost::none);

    // Returns the sender of the message.
    [[nodiscard]] auto getSender() const -> std::string;

    // Returns the recipient of the message.
    [[nodiscard]] auto getRecipient() const -> std::string;

    // Returns the type of the message.
    [[nodiscard]] auto getType() const -> std::string;

    // Returns the body of the message.
    [[nodiscard]] auto getBody() const -> const boost::json::object &;

    // Returns the message-id if present, otherwise returns 'boost::none'.
    [[nodiscard]] auto getMsgId() const -> boost::optional<int>;

    // Returns the reply message-id if present, otherwise returns 'boost::none'.
    [[nodiscard]] auto getInReplyTo() const -> boost::optional<int>;

    // Sends the message to the 'Maelstrom'.
    auto send() const -> void;

private:
    // Field 'src' of the message.
    std::string _sender;

    // Field 'dest' of the message.
    std::string _recipient;

    // Field 'type' of the message.
    std::string _type;

    // Field 'body' of the message.
    boost::json::object _body;

    // Field 'msg_id' of the message if present.
    boost::optional<int> _msgId;

    // Field 'in_reply_to' of the message if present.
    boost::optional<int> _inReplyTo;
};

/**
 * Abstract base class for writing custom message handler.
 */
class MessageHandler {
public:
    virtual ~MessageHandler() = default;

    // Returns the type of message associated with this handler.
    [[nodiscard]] virtual constexpr auto name() const -> std::string = 0;

    // Handles the provided message which is guaranteed to be of the expected
    // type.
    virtual auto handle(const Message &message) -> void = 0;
};

using MessageHandlerPtr = std::unique_ptr<MessageHandler>;

/**
 * Node class that interacts with the 'Maelstrom'.
 */
class Node {
public:
    // Starts receiving messages from the 'Maelstrom' and dispatches them to the
    // appropriate message handler. The user should call this method.
    auto run() -> void;

    // Registers a message handler with the node. The user should call this
    // method before calling 'run()'.
    auto registerMessageHandler(MessageHandlerPtr msgHandler) -> void;

    auto registerInitHandler(
        std::function<bool(std::string_view, std::vector<std::string>)>
            initFunc) -> void;

    auto nodeId() -> boost::optional<std::string> { return _nodeId; }

    auto peerIds() -> std::vector<std::string> { return _peerIds; }

private:
    // Initializes the node with 'nodeId' and 'peerIds'.
    void _init(const Message &message);

    // Calls the appropriate message handler for the provided message.
    void handleMessage(const Message &message);

    // Maps message type to message handler.
    std::unordered_map<std::string, MessageHandlerPtr> _messageHandlers;

    // Node ID of this node.
    boost::optional<std::string> _nodeId;

    // Node IDs of the peers of this node.
    std::vector<std::string> _peerIds;

    std::vector<std::function<bool(std::string_view, std::vector<std::string>)>>
        _initHandler;
    boost::asio::thread_pool _pool;
};
}