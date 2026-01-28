#include "storage/impl/ThreadSafeKVStore.h"
#include "system/EchoMessageHandler.h"
#include "system/Logger.h"
#include "system/TransactionMessageHandler.h"
#include "system/maelstrom.h"
#include "transactions/TransactionServer.h"
#include "transactions/impl/TwoPLScheduler.h"

auto
main() -> int
{
    using namespace tp_project;
    maelstrom::Node node;
    node.registerMessageHandler(std::make_unique<system::EchoMessageHandler>());
    logger::info("Startup");

    /*
     * Two-Phase Locking (2PL) Scheduler
     *
     * Uses per-key locking with Wait-Die deadlock prevention.
     * Multiple transactions can execute concurrently when accessing different keys.
     * Guarantees serializability through strict two-phase locking.
     */
    auto scheduler = transactions::TransactionManager::makeTransactionManager<
        transactions::impl::TwoPLScheduler>(
        storage::KVStore::makeKVStore<storage::impl::ThreadSafeKVStore>(),
        transactions::TransactionServer::makeTransactionServer());

    node.registerMessageHandler(
        std::make_unique<system::TransactionMessageHandler>(scheduler));
    node.registerInitHandler(
        [&scheduler](const std::string_view nodeId,
            const std::vector<std::string> &peers) -> bool {
            return scheduler->start(nodeId, peers);
        });
    node.run();
    return 0;
}
