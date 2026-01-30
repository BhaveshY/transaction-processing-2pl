#include "storage/impl/ThreadSafeKVStore.h"

namespace tp_project::storage::impl {

auto ThreadSafeKVStore::get(KeyType key) const noexcept
    -> std::optional<ValueContainer>
{
    auto& stripe = get_stripe(key);
    std::lock_guard lock(stripe.mtx);

    const auto& partition = stripe.partition;
    if (partition.contains(key)) {
        return partition.at(key);
    }
    return std::nullopt;
}

void ThreadSafeKVStore::append(KeyType key, ValueType value) noexcept
{
    auto& stripe = get_stripe(key);
    std::lock_guard lock(stripe.mtx);

    auto& partition = stripe.partition;
    if (partition.contains(key)) {
        partition[key].push_back(value);
    } else {
        partition.emplace(key, ValueContainer({ value }));
    }
}

} // namespace tp_project::storage::impl
