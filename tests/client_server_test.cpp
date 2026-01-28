#define BOOST_TEST_MODULE TransactionServerTests
#include <boost/asio.hpp>
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>

#include "system/JSONMessage.h"
#include "system/Peer.h"
#include "transactions/TransactionServer.h"
namespace tp_project::test {
using namespace tp_project::system;
using namespace tp_project::transactions;
// Helpers
auto
make_connect_msg(Peer::id src, Peer::id dest) -> boost::json::value
{
    return { { "src", src }, { "dest", dest },
        { "body", { { "type", "connect" } } } };
}

auto
make_message(Peer::id src, Peer::id dest, const std::string &type)
    -> boost::json::value
{
    return { { "src", src }, { "dest", dest },
        { "body", { { "type", type } } } };
}

BOOST_AUTO_TEST_CASE(StartupCreatesPeers)
{
    TransactionServer s0;
    TransactionServer s1;
    TransactionServer s2;

    auto s0Start = std::async([&]() -> void { s0.startup(0, { 1, 2 }); });
    auto s1Start = std::async([&]() -> void { s1.startup(1, { 0, 2 }); });
    auto s2Start = std::async([&]() -> void { s2.startup(2, { 0, 1 }); });

    BOOST_CHECK_NO_THROW(s0Start.get());
    BOOST_CHECK_NO_THROW(s1Start.get());
    BOOST_CHECK_NO_THROW(s2Start.get());

    BOOST_TEST(s0.peers().size() == 2UL);
    BOOST_TEST(s1.peers().size() == 2UL);
    BOOST_TEST(s2.peers().size() == 2UL);

    BOOST_TEST(s0.peers().count(1) == 1UL);
    BOOST_TEST(s0.peers().count(2) == 1UL);
}

BOOST_AUTO_TEST_CASE(SendReceiveMessage)
{
    try {
        TransactionServer s0;
        TransactionServer s1;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0 }); });

        s0Start.get();
        s1Start.get();

        auto msg = make_message(0, 1, "hello");

        s0.send(1, msg);

        auto [src, dest, body] = s1.receive(0).get();

        BOOST_TEST(body.at("type").as_string() == "hello");
        BOOST_TEST(src == 0);
        BOOST_TEST(dest == 1);
    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}

BOOST_AUTO_TEST_CASE(BidirectionalSendReceive)
{
    try {
        TransactionServer s0;
        TransactionServer s1;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0 }); });
        s0Start.wait();
        s1Start.wait();
        s0.send(1, make_message(0, 1, "A"));
        s1.send(0, make_message(1, 0, "B"));

        JSONMessage r1 = s1.receive(0).get();
        JSONMessage r0 = s0.receive(1).get();

        BOOST_TEST(!r0.body.empty());
        BOOST_TEST(!r1.body.empty());
        BOOST_TEST(r1.body.at("type").as_string() == "A");
        BOOST_TEST(r0.body.at("type").as_string() == "B");
    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}

BOOST_AUTO_TEST_CASE(AsyncReceiveWorks)
{
    try {
        TransactionServer s0;
        TransactionServer s1;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0 }); });
        s0Start.wait();
        s1Start.wait();
        auto fut = s1.receive(0);

        s0.send(1, make_message(0, 1, "async"));

        JSONMessage result = fut.get();

        BOOST_TEST(result.body.at("type").as_string() == "async");
    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}

BOOST_AUTO_TEST_CASE(BroadcastWorks)
{
    try {
        TransactionServer s0;
        TransactionServer s1;
        TransactionServer s2;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1, 2 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0, 2 }); });
        auto s2Start = std::async([&]() -> void { s2.startup(2, { 0, 1 }); });
        s0Start.wait();
        s1Start.wait();
        s2Start.wait();
        s0.broadcast(make_message(0, 0, "bcast"));

        auto r1 = s1.receive(0).get();
        auto r2 = s2.receive(0).get();

        BOOST_TEST(!r1.body.empty());
        BOOST_TEST(!r2.body.empty());
        BOOST_TEST(r1.body.at("type").as_string() == "bcast");
        BOOST_TEST(r2.body.at("type").as_string() == "bcast");
    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}

BOOST_AUTO_TEST_CASE(ConnectHandshake)
{
    try {
        TransactionServer s0;
        TransactionServer s1;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0 }); });
        s0Start.wait();
        s1Start.wait();

        auto connect_msg = make_connect_msg(0, 1);

        s0.send(1, connect_msg);

        auto [src, dest, body] = s1.receive(0).get();

        BOOST_TEST(body.at("type").as_string() == "connect");
        BOOST_TEST(src == 0);
        BOOST_TEST(dest == 1);

    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}

BOOST_AUTO_TEST_CASE(StressManyMessages)
{
    try {
        TransactionServer s0;
        TransactionServer s1;

        auto s0Start = std::async([&]() -> void { s0.startup(0, { 1 }); });
        auto s1Start = std::async([&]() -> void { s1.startup(1, { 0 }); });
        s0Start.wait();
        s1Start.wait();
        const int N = 100;

        for (int i = 0; i < N; ++i)
            s0.send(1, make_message(0, 1, "ping"));

        for (int i = 0; i < N; ++i) {
            JSONMessage r = s1.receive(0).get();
            BOOST_TEST(r.body.at("type").as_string() == "ping");
        }
    } catch (boost::system::system_error &e) {
        if (e.code() != boost::system::errc::operation_canceled)
            throw;
    }
}
}