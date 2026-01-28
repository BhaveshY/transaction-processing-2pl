#pragma once
#include <boost/asio.hpp>
#include <boost/json.hpp>

namespace tp_project::transactions {
class TransactionServer;
}

namespace tp_project::system {
struct JSONMessage;

class Peer {
public:
    using pointer = std::shared_ptr<Peer>;
    using id = uint16_t;
    constexpr static int DEFAULT_CONN_RETRIES = 10;
    constexpr static auto DEFAULT_CONN_WAIT = std::chrono::milliseconds(200);
    explicit Peer(id nodeID, id peerId, boost::asio::io_context &context);

    virtual ~Peer();

    /** sends a message and returns the response to the message. This function
     * is asynchronous but thread-safe **/
    auto send_receive(const boost::json::value &message)
        -> boost::asio::awaitable<JSONMessage>;

    void send(const boost::json::value &message) const;

    auto receive() -> boost::asio::awaitable<JSONMessage>;

private:
    id peerId { };
    boost::asio::strand<boost::asio::io_context::executor_type> strand;
    std::unique_ptr<boost::asio::ip::tcp::socket> in_socket;
    std::unique_ptr<boost::asio::ip::tcp::socket> out_socket;
    std::once_flag incoming_init;
    boost::asio::steady_timer ready;

    // this should only be called once to catch the incoming connection from the
    // node. The outgoing connection is established upon construction of the
    // Peer
    void setup_incoming(std::unique_ptr<boost::asio::ip::tcp::socket> &&socket);

    void connect(int maxRetries = DEFAULT_CONN_RETRIES,
        std::chrono::milliseconds delay = DEFAULT_CONN_WAIT);
    friend transactions::TransactionServer;
};
}