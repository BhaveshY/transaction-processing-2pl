#include "transactions/LockManager.h"
#include <thread>

namespace tp_project::transactions {

auto LockManager::acquire(KeyType key, TransactionId tx_id) -> AcquireResult
{
    // Get the lock entry for this key
    {
        std::lock_guard table_lock(table_mutex);
        
        // Create entry if it doesn't exist
        if (!lock_table.contains(key)) {
            // Use piecewise_construct to construct in place (mutex is non-movable)
            lock_table.try_emplace(key);
        }
    }
    
    auto& lock_entry = lock_table.at(key);
    
    // Lock the specific entry
    std::unique_lock entry_lock(lock_entry.mtx);
    
    // Track that this transaction is interested in this key (for cleanup)
    {
        std::lock_guard held_lock(held_locks_mutex);
        held_locks[tx_id].insert(key);
    }
    
    return acquire_with_wait_die(lock_entry, key, tx_id, entry_lock);
}

auto LockManager::acquire_with_wait_die(LockEntry& lock_entry,
                                        KeyType key,
                                        TransactionId tx_id,
                                        std::unique_lock<std::mutex>& entry_lock)
    -> AcquireResult
{
    // Try to acquire with extended timeout to reduce aborts under high contention
    constexpr int MAX_SPINS = 1000;  // Extended spin wait
    constexpr int MAX_YIELDS = 500;   // More yields

    for (int spin = 0; spin < MAX_SPINS; ++spin) {
        // Fast path: lock is available
        if (!lock_entry.owner.has_value()) {
            lock_entry.owner = tx_id;
            return AcquireResult::ACQUIRED;
        }

        TransactionId owner_id = *lock_entry.owner;

        // If we already own the lock, that's fine (reentrant)
        if (owner_id == tx_id) {
            return AcquireResult::ACQUIRED;
        }

        // Lock held by someone else - spin briefly
        entry_lock.unlock();
        std::this_thread::yield();
        entry_lock.lock();
    }

    // Try a few more times with longer waits
    for (int yield = 0; yield < MAX_YIELDS; ++yield) {
        if (!lock_entry.owner.has_value()) {
            lock_entry.owner = tx_id;
            return AcquireResult::ACQUIRED;
        }

        entry_lock.unlock();
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        entry_lock.lock();
    }

    // Could not acquire lock - abort to allow progress
    return AcquireResult::ABORTED;
}

void LockManager::release(KeyType key, TransactionId tx_id)
{
    std::lock_guard table_lock(table_mutex);
    
    if (!lock_table.contains(key)) {
        return; // No lock entry, nothing to release
    }
    
    auto& lock_entry = lock_table.at(key);
    std::lock_guard entry_lock(lock_entry.mtx);
    
    // Only the owner can release
    if (lock_entry.owner != tx_id) {
        return;
    }
    
    // Release the lock
    lock_entry.owner.reset();
    
    // Remove from held locks tracking
    {
        std::lock_guard held_lock(held_locks_mutex);
        auto it = held_locks.find(tx_id);
        if (it != held_locks.end()) {
            it->second.erase(key);
            if (it->second.empty()) {
                held_locks.erase(it);
            }
        }
    }
}

void LockManager::release_all(TransactionId tx_id)
{
    std::unordered_set<KeyType> keys_to_release;
    
    // Get all keys held by this transaction
    {
        std::lock_guard held_lock(held_locks_mutex);
        auto it = held_locks.find(tx_id);
        if (it != held_locks.end()) {
            keys_to_release = std::move(it->second);
            held_locks.erase(it);
        }
    }
    
    // Release each lock
    for (const auto& key : keys_to_release) {
        release(key, tx_id);
    }
}

auto LockManager::holds_lock(KeyType key, TransactionId tx_id) const -> bool
{
    std::lock_guard held_lock(held_locks_mutex);
    
    auto it = held_locks.find(tx_id);
    if (it == held_locks.end()) {
        return false;
    }
    
    return it->second.contains(key);
}

} // namespace tp_project::transactions
