#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>

namespace tp_project::transactions {

using TransactionId = uint64_t;
using KeyType = int64_t;

class LockManager {
public:
    enum class AcquireResult {
        ACQUIRED,
        ABORTED
    };

private:
    struct LockEntry {
        std::optional<TransactionId> owner;
        mutable std::mutex mtx;
    };

    std::unordered_map<KeyType, LockEntry> lock_table;
    mutable std::mutex table_mutex;

    std::unordered_map<TransactionId, std::unordered_set<KeyType>> held_locks;
    mutable std::mutex held_locks_mutex;

    AcquireResult acquire_with_wait_die(LockEntry& lock_entry,
                                       KeyType key,
                                       TransactionId tx_id,
                                       std::unique_lock<std::mutex>& unique_lock);

public:
    LockManager() = default;
    ~LockManager() = default;

    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(LockManager&&) = delete;

    auto acquire(KeyType key, TransactionId tx_id) -> AcquireResult;
    void release(KeyType key, TransactionId tx_id);
    void release_all(TransactionId tx_id);
    [[nodiscard]] auto holds_lock(KeyType key, TransactionId tx_id) const -> bool;
};

} // namespace tp_project::transactions
