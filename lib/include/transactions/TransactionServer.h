#pragma once
#include <boost/asio/ip/tcp.hpp>

#include "system/Peer.h"

namespace tp_project {
namespace system {
struct JSONMessage;
}

namespace transactions {
class TransactionServer {
public:
    using pointer = std::unique_ptr<TransactionServer>;
    constexpr static uint16_t NUM_THREADS { 4 };
    constexpr static uint16_t BASE_PORT_RANGE_START = 40000;
    // hardcoded because we don't ever want to expose this server anywhere!
    constexpr static std::string SERVER_IP = "127.0.0.1";

    explicit TransactionServer();

    virtual ~TransactionServer();

    void startup(system::Peer::id id,
        const std::vector<system::Peer::id> peers);

    void send(system::Peer::id peerID, const boost::json::value &message) const;

    void broadcast(const boost::json::value &message);

    auto receive(system::Peer::id peerID) -> std::future<system::JSONMessage>;
    auto async_receive(system::Peer::id peerID)
        -> boost::asio::awaitable<system::JSONMessage>;

    [[nodiscard]] auto peers() const
        -> const std::unordered_map<system::Peer::id, system::Peer::pointer> &
    {
        return peers_;
    }

    template <typename TransactionServerType = TransactionServer, typename... Args>
    static auto makeTransactionServer(Args &&...args) -> pointer
    {
        return std::make_unique<TransactionServerType>(
            std::forward<Args>(args)...);
    }

private:
    uint16_t server_port;
    system::Peer::id nodeID;
    boost::asio::io_context context_;
    boost::asio::executor_work_guard<
        boost::asio::io_context::basic_executor_type<std::allocator<void>, 0>>
        work;
    // we have nodes 0-n, hence we are only wasting one place here every time
    // but have better performance, the nodes are ordered after their node id
    // Please don't modify the content, once the peers are initialized.
    // This could lead to data races
    std::unordered_map<system::Peer::id, system::Peer::pointer> peers_;

    std::once_flag startup_flag;
    std::unique_ptr<boost::asio::ip::tcp::acceptor> acceptor_;
    boost::asio::thread_pool _pool;
    bool stopped = false;

    // this function has to wait for all but its own node to connect and add the
    // socket to the correct peer as incoming socket. The connecting client has
    // to identify itself by sending a connect message with its own id like
    //  {"src": 0, "dest": 1, "body": {"type":"connect"}}
    void start_accept();

    auto read_new_conn(boost::asio::ip::tcp::socket &server_socket) const
        -> system::Peer::id;
};
}
}