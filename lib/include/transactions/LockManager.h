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

/**
 * Transaction identifier - used as a timestamp for wait-die ordering.
 * Smaller values = older transactions (higher priority).
 */
using TransactionId = uint64_t;

/**
 * Key type for lock operations.
 */
using KeyType = int64_t;

/**
 * Lock Manager implementing wait-based lock acquisition.
 * 
 * Lock Acquisition Strategy:
 * - Try to acquire the lock immediately if available
 * - If not available, wait with timeout and retry
 * - Abort after max attempts to allow progress
 * 
 * This approach provides:
 * - High concurrency when contention is low
 * - Bounded waiting to prevent starvation
 * - Progress guarantee through aborts
 */
class LockManager {
public:
    /**
     * Result of attempting to acquire a lock.
     */
    enum class AcquireResult {
        ACQUIRED,  ///< Lock was successfully acquired
        ABORTED    ///< Lock could not be acquired (abort recommended)
    };

private:
    /**
     * Per-key lock state.
     */
    struct LockEntry {
        std::optional<TransactionId> owner;      // Current lock holder (exclusive)
        mutable std::mutex mtx;                  // Protects this entry
    };

    /**
     * Global lock table mapping keys to their lock entries.
     * Access to this map is protected by a separate mutex.
     */
    std::unordered_map<KeyType, LockEntry> lock_table;
    mutable std::mutex table_mutex;

    /**
     * Track which locks each transaction holds (for efficient cleanup).
     * TransactionId -> set of keys held by that transaction.
     */
    std::unordered_map<TransactionId, std::unordered_set<KeyType>> held_locks;
    mutable std::mutex held_locks_mutex;

    /**
     * Internal method to acquire a lock with wait-die logic.
     * Assumes lock_table_mutex is already held.
     * 
     * @param lock_entry The lock entry for the requested key
     * @param key The key being locked
     * @param tx_id The transaction requesting the lock
     * @param unique_lock Unique lock on the lock entry's mutex
     * @return AcquireResult indicating if lock was acquired or transaction should abort
     */
    AcquireResult acquire_with_wait_die(LockEntry& lock_entry, 
                                       KeyType key, 
                                       TransactionId tx_id,
                                       std::unique_lock<std::mutex>& unique_lock);

public:
    LockManager() = default;
    ~LockManager() = default;

    // Non-copyable, non-movable
    LockManager(const LockManager&) = delete;
    LockManager& operator=(const LockManager&) = delete;
    LockManager(LockManager&&) = delete;
    LockManager& operator=(LockManager&&) = delete;

    /**
     * Attempt to acquire an exclusive lock on a key.
     * 
     * Implements wait-die: if this transaction is older than the current
     * lock holder, returns ABORTED. Otherwise, waits for the lock.
     * 
     * @param key The key to lock
     * @param tx_id The transaction requesting the lock (also its timestamp)
     * @return ACQUIRED if lock obtained, ABORTED if transaction should abort
     */
    auto acquire(KeyType key, TransactionId tx_id) -> AcquireResult;

    /**
     * Release a specific lock held by a transaction.
     * Wakes up the next waiting transaction (if any).
     * 
     * @param key The key to unlock
     * @param tx_id The transaction releasing the lock
     */
    void release(KeyType key, TransactionId tx_id);

    /**
     * Release all locks held by a transaction.
     * Called on transaction commit or abort.
     * 
     * @param tx_id The transaction whose locks should be released
     */
    void release_all(TransactionId tx_id);

    /**
     * Check if a transaction currently holds a specific lock.
     * 
     * @param key The key to check
     * @param tx_id The transaction to check
     * @return true if the transaction holds the lock
     */
    [[nodiscard]] auto holds_lock(KeyType key, TransactionId tx_id) const -> bool;
};

} // namespace tp_project::transactions
