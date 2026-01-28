#pragma once

#include "storage/KVStore.h"
#include "storage/Types.h"

#include <array>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <unordered_map>

namespace tp_project::storage::impl {

/**
 * Thread-safe Key-Value Store using striped mutexes for high concurrency.
 * 
 * Design uses 256 independent mutex "stripes" - each key is hashed to a specific
 * stripe. This allows concurrent operations on non-conflicting keys, providing
 * much better parallelism than a single global mutex.
 * 
 * The striped approach is a classic lock striping pattern used in Java's
 * ConcurrentHashMap and similar high-performance concurrent data structures.
 */
class ThreadSafeKVStore final : public KVStore {
    static constexpr size_t NUM_STRIPES = 256;

    /**
     * Each stripe has its own mutex and map partition.
     * This allows true concurrent access to different stripes.
     * Mutex is mutable to allow locking from const methods.
     */
    struct Stripe {
        mutable std::mutex mtx;
        std::unordered_map<KeyType, ValueContainer> partition;
    };

    std::array<Stripe, NUM_STRIPES> stripes;

    /**
     * Hash the key to determine which stripe it belongs to.
     * Uses std::hash for consistent distribution across stripes.
     */
    [[nodiscard]] size_t get_stripe_index(KeyType key) const noexcept {
        return std::hash<KeyType>{}(key) % NUM_STRIPES;
    }

    /**
     * Get the stripe for a given key.
     */
    [[nodiscard]] Stripe& get_stripe(KeyType key) noexcept {
        return stripes[get_stripe_index(key)];
    }

    /**
     * Const version for read operations.
     */
    [[nodiscard]] const Stripe& get_stripe(KeyType key) const noexcept {
        return stripes[get_stripe_index(key)];
    }

public:
    ThreadSafeKVStore() noexcept = default;
    ~ThreadSafeKVStore() override = default;

    // Non-copyable, non-movable
    ThreadSafeKVStore(const ThreadSafeKVStore&) = delete;
    ThreadSafeKVStore& operator=(const ThreadSafeKVStore&) = delete;
    ThreadSafeKVStore(ThreadSafeKVStore&&) = delete;
    ThreadSafeKVStore& operator=(ThreadSafeKVStore&&) = delete;

    /**
     * Get the value associated with a key.
     * Thread-safe: locks only the specific stripe for this key.
     * 
     * @param key The key to look up
     * @return Optional containing the value container, or empty if key doesn't exist
     */
    [[nodiscard]] auto get(KeyType key) const noexcept
        -> std::optional<ValueContainer> override;

    /**
     * Append a value to the key's array (or create new array if key doesn't exist).
     * Thread-safe: locks only the specific stripe for this key.
     * 
     * @param key The key to append to
     * @param value The value to append
     */
    auto append(KeyType key, ValueType value) noexcept -> void override;
};

} // namespace tp_project::storage::impl
