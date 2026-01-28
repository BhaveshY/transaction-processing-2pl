#pragma once
#include <boost/describe/class.hpp>

#include "Peer.h"

#include "boost/json/parse.hpp"

namespace tp_project::system {
struct JSONMessage {
    Peer::id src{};
    Peer::id dest{};
    boost::json::object body;

    static auto parse(const std::string_view messageString) -> JSONMessage
    {
        const auto msg = boost::json::parse(messageString);
        return boost::json::value_to<JSONMessage>(msg);
    }
};
BOOST_DESCRIBE_STRUCT(JSONMessage, (), (src, dest, body));
}