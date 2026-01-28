#pragma once

#include "storage/Types.h"

#include <memory>
#include <optional>

namespace tp_project::storage {
/**
 * This class acts as a Key-Value Store interface you will implement in your
 * custom storage.
 */
class KVStore {
public:
    using pointer = std::shared_ptr<KVStore>;
    virtual ~KVStore() = default;

    /**
     * This function should take care of querying the key-value store for a key.
     * Depending on your scheduler, you might want to implement some form of
     * concurrency control.
     *
     * @param key
     * @return either an optional with the current value to the key or if there
     * exists no key, it should return an empty optional.
     */
    [[nodiscard]] virtual auto get(KeyType key) const noexcept
        -> std::optional<ValueContainer> = 0;

    /**
     * This function will append a value to the key's array if it already
     * exists. Otherwise, it will create an array with the passed value and
     * insert the new key.
     * @param key
     * @param value
     */
    virtual auto append(KeyType key, ValueType value) noexcept -> void = 0;

    template <typename KVStoreType, typename... Args>
    static auto makeKVStore(Args &&...args) -> pointer
    {
        return std::make_shared<KVStoreType>(std::forward<Args>(args)...);
    }
};
}