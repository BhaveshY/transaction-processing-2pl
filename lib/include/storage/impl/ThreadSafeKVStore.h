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

class ThreadSafeKVStore final : public KVStore {
    static constexpr size_t NUM_STRIPES = 256;

    struct Stripe {
        mutable std::mutex mtx;
        std::unordered_map<KeyType, ValueContainer> partition;
    };

    std::array<Stripe, NUM_STRIPES> stripes;

    [[nodiscard]] size_t get_stripe_index(KeyType key) const noexcept {
        return std::hash<KeyType>{}(key) % NUM_STRIPES;
    }

    [[nodiscard]] Stripe& get_stripe(KeyType key) noexcept {
        return stripes[get_stripe_index(key)];
    }

    [[nodiscard]] const Stripe& get_stripe(KeyType key) const noexcept {
        return stripes[get_stripe_index(key)];
    }

public:
    ThreadSafeKVStore() noexcept = default;
    ~ThreadSafeKVStore() override = default;

    ThreadSafeKVStore(const ThreadSafeKVStore&) = delete;
    ThreadSafeKVStore& operator=(const ThreadSafeKVStore&) = delete;
    ThreadSafeKVStore(ThreadSafeKVStore&&) = delete;
    ThreadSafeKVStore& operator=(ThreadSafeKVStore&&) = delete;

    [[nodiscard]] auto get(KeyType key) const noexcept
        -> std::optional<ValueContainer> override;

    auto append(KeyType key, ValueType value) noexcept -> void override;
};

} // namespace tp_project::storage::impl
