add_subdirectory(src)
add_subdirectory(include)

target_sources(tx_system PUBLIC main.cpp)

target_compile_features(tx_system PRIVATE cxx_std_20)
#include_directories(include)

target_link_libraries(tx_system PRIVATE Boost::asio Boost::json Boost::log system transactions storage)