cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(transaction_processing_programming_project VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)


include(cmake/CPM.cmake)

message(STATUS "Detected compiler: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "System name      : ${CMAKE_SYSTEM_NAME}")
message(STATUS "Triplet          : ${VCPKG_TARGET_TRIPLET}")
message(STATUS "Module Path      : ${CMAKE_MODULE_PATH}")

foreach (lang IN ITEMS C CXX)
    add_compile_definitions(
            #Exclude rarely-used stuff from Windows headers

            "$<$<COMPILE_LANGUAGE:${lang}>:WIN32_LEAN_AND_MEAN;NOMINMAX;VC_EXTRALEAN>"
    )
endforeach ()

CPMAddPackage(
        NAME Boost
        VERSION 1.86.0 # Versions less than 1.85.0 may need patches for installation targets.
        URL https://github.com/boostorg/boost/releases/download/boost-1.86.0/boost-1.86.0-cmake.tar.xz
        URL_HASH SHA256=2c5ec5edcdff47ff55e27ed9560b0a0b94b07bd07ed9928b476150e16b0efc57
        OPTIONS "BOOST_ENABLE_CMAKE ON" "BOOST_SKIP_INSTALL_RULES ON" # Set `OFF` for installation
        "BUILD_SHARED_LIBS OFF" "BOOST_INCLUDE_LIBRARIES thread\\\;json\\\;asio\\\;log\\\;test" # Note the escapes!
)

CPMAddPackage(
        NAME Maelstrom
        GIT_REPOSITORY https://github.com/jepsen-io/maelstrom.git
        GIT_TAG main
        DOWNLOAD_ONLY
)

if (CMAKE_GENERATOR MATCHES "Visual Studio")
    add_compile_options("$<$<CXX_COMPILER_ID:MSVC>:/MP>")
endif ()

add_executable(tx_system)

enable_testing()

add_executable(transaction_server_tests)

option(ENABLE_CLANG_TIDY "Enable clang-tidy static analysis" ON)
if (ENABLE_CLANG_TIDY)
    include(cmake/ClangTidy.cmake)

    enable_clang_tidy(tx_system)
    enable_clang_tidy(transaction_server_tests)
endif ()

include(cmake/CompilerWarnings.cmake)
include(cmake/Sanitizers.cmake)

set_project_warnings(tx_system)
enable_sanitizers(tx_system)

set_project_warnings(transaction_server_tests)
enable_sanitizers(transaction_server_tests)

add_subdirectory(tests)

# Register test with CTest
add_test(NAME ClientServerTests COMMAND transaction_server_tests)

add_test(NAME single_node_serializability
        COMMAND lein run test -w txn-list-append
        --bin $<TARGET_FILE:tx_system>
        --time-limit 15
        --log-stderr
        --node-count 1
        --max-txn-length 255
        --concurrency 10
        --rate 10000
        WORKING_DIRECTORY ${Maelstrom_SOURCE_DIR}
)

add_test(NAME distributed_serializability
        COMMAND lein run test -w txn-list-append
        --bin $<TARGET_FILE:tx_system>
        --time-limit 10
        --log-stderr
        --node-count 3
        --max-txn-length 20
        --rate 10000
        WORKING_DIRECTORY ${Maelstrom_SOURCE_DIR}
)

set(passRegex "Everything looks good!")
set(failRegex "Analysis invalid!")

set_property(TEST single_node_serializability
        PROPERTY PASS_REGULAR_EXPRESSION "${passRegex}")
set_property(TEST single_node_serializability
        PROPERTY FAIL_REGULAR_EXPRESSION "${failRegex}")

set_property(TEST distributed_serializability
        PROPERTY PASS_REGULAR_EXPRESSION "${passRegex}")
set_property(TEST distributed_serializability
        PROPERTY FAIL_REGULAR_EXPRESSION "${failRegex}")

add_subdirectory(lib)

set(CPACK_SOURCE_GENERATOR "ZIP")
set(CPACK_SOURCE_IGNORE_FILES
        \\.git/
        ${CMAKE_BINARY_DIR}
        Testing
        ".*~$"
)
set(CPACK_VERBATIM_VARIABLES YES)
set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY OFF)
include(CPack)