#define BOOST_THREAD_PROVIDES_FUTURE
#include <boost/json/parse.hpp>
#include <boost/thread/future.hpp>

#include "system/JSONMessage.h"
#include "system/Logger.h"
#include "system/Peer.h"
#include "transactions/TransactionServer.h"
namespace tp_project::system {
Peer::Peer(const id nodeID, const id peerId, boost::asio::io_context &context)
    : peerId { peerId }
    , strand(boost::asio::make_strand(context))
    , out_socket(std::make_unique<boost::asio::ip::tcp::socket>(strand))
    , ready(strand, std::chrono::steady_clock::time_point::max())
{
    const auto msg = JSONMessage { .src = nodeID,
        .dest = peerId,
        .body = boost::json::parse(R"({"type": "connect"})").as_object() };

    connect();
    send(boost::json::value_from(msg));
}

Peer::~Peer()
{
    try {
        if (in_socket) {
            in_socket->shutdown(boost::asio::socket_base::shutdown_both);
        }
        out_socket->shutdown(boost::asio::socket_base::shutdown_both);
    } catch (const boost::system::system_error &e) {
        if (e.code() != boost::asio::error::not_connected &&
            e.code() != boost::asio::error::operation_aborted) {
            logger::error(
                "Exception occurred while closing sockets of Peer object p{}: {}",
                peerId, e.what());
        }
    }
}

auto
Peer::send_receive(const boost::json::value &message)
    -> boost::asio::awaitable<JSONMessage>
{
    send(message);
    co_return co_await receive();
}

auto
Peer::send(const boost::json::value &message) const -> void
{
    const std::string payload = serialize(message);

    if (payload.size() >= std::numeric_limits<uint32_t>::max()) {
        throw std::logic_error("Message size too large");
    }

    const auto len = static_cast<uint32_t>(payload.size());
    const uint32_t len_be = htonl(len);

    std::vector<uint8_t> out(4 + len);
    std::memcpy(out.data(), &len_be, sizeof(len_be));
    std::memcpy(out.data() + sizeof(len_be), payload.data(), len);

    boost::asio::write(*out_socket, boost::asio::buffer(out));
}

auto
Peer::receive() -> boost::asio::awaitable<JSONMessage>
{
    try {
        // check if socket was already initialized and if not wait for it
        if (in_socket == nullptr) {
            co_await ready.async_wait(boost::asio::use_awaitable);
        }

        uint32_t response_len_be = 0;

        {
            co_await boost::asio::async_read(*in_socket,
                boost::asio::buffer(&response_len_be, 4),
                boost::asio::use_awaitable);
        }

        const uint32_t response_len = ntohl(response_len_be);

        std::vector<char> payload_buf(response_len);

        {
            co_await boost::asio::async_read(*in_socket,
                boost::asio::buffer(payload_buf), boost::asio::use_awaitable);
        }

        co_return JSONMessage::parse(
            { payload_buf.data(), payload_buf.size() });
    } catch (boost::system::system_error &e) {
        if (e.code() == boost::asio::error::operation_aborted) {
            logger::error("Receive Operation aborted: {}", e.what());
}
        throw;


    }
}

void
Peer::connect(const int maxRetries, const std::chrono::milliseconds delay)
{
    boost::asio::io_service io_service;
    boost::asio::ip::tcp::resolver resolver(strand);
    const boost::asio::ip::tcp::resolver::results_type endpoints =
        resolver.resolve(transactions::TransactionServer::SERVER_IP,
            std::to_string(
                transactions::TransactionServer::BASE_PORT_RANGE_START +
                peerId));

    logger::info("Establish outgoing connection to n{}", peerId);
    for (auto i = 1; i <= maxRetries; ++i) {
        try {
            boost::asio::connect(*out_socket, endpoints);
            logger::info("Connection to n{} successful", peerId);
            return;
        } catch (boost::system::system_error &e) {
            if (e.code() == boost::asio::error::connection_refused ||
                e.code() == boost::asio::error::timed_out) {
                logger::info("Attempt {} of {}. Connect failed to n{}: {}", i,
                    maxRetries, peerId, e.what());
                std::this_thread::sleep_for(delay);
                continue;
            }

            logger::error("Could not establish connection to n{}: {}", peerId,
                e.what());
            throw;
        }
    }
    logger::error("Could not establish connection to n{}, giving up", peerId);
    std::exit(EXIT_FAILURE);
}

void
Peer::setup_incoming(std::unique_ptr<boost::asio::ip::tcp::socket> &&socket)
{
    std::call_once(incoming_init, [&]() -> void {
        ready.expires_at(std::chrono::steady_clock::now());
        in_socket = std::move(socket);
    });
}
}