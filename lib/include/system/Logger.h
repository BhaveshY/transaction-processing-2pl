#pragma once
#include <boost/core/null_deleter.hpp>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>

#include <format>
#include <iostream>
#include <mutex>
#include <sstream>

namespace tp_project::logger {
namespace logging = boost::log;
namespace sinks = boost::log::sinks;
namespace expr = boost::log::expressions;

using log_level = logging::trivial::severity_level;

inline auto
init() -> void
{
    static std::once_flag init_flag;
    std::call_once(init_flag, [] {
        using text_sink = sinks::synchronous_sink<sinks::text_ostream_backend>;
        const auto sink = boost::make_shared<text_sink>();

        // Log to stderr
        sink->locked_backend()->add_stream(
            boost::shared_ptr<std::ostream>(&std::cerr, boost::null_deleter()));

        sink->set_formatter(expr::stream << "[" << logging::trivial::severity
                                         << "] " << expr::smessage);

        logging::core::get()->add_sink(sink);
        logging::add_common_attributes();
    });
}

inline auto
get_logger()
    -> boost::log::sources::severity_logger<logging::trivial::severity_level> &

{
    static boost::log::sources::severity_logger<
        logging::trivial::severity_level>
        lg;
    init();
    return lg;
}

inline auto
log_msg(log_level level, std::string formatted_string) -> void
{
    auto &lg = get_logger();
    BOOST_LOG_SEV(lg, level) << formatted_string;
}

template <typename... Args>
auto
trace(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::trace,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}
template <typename... Args>
auto
debug(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::debug,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}
template <typename... Args>
auto
info(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::info,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}
template <typename... Args>
auto
warn(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::warning,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}
template <typename... Args>
auto
error(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::error,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}
template <typename... Args>
auto
fatal(const std::format_string<Args...> fmt, Args &&...args) -> void
{
    log_msg(log_level::fatal,
        std::vformat(fmt.get(), std::make_format_args(args...)));
}

inline auto
set_level(boost::log::trivial::severity_level level) -> void
{
    boost::log::core::get()->set_filter(boost::log::trivial::severity >= level);
}
}
