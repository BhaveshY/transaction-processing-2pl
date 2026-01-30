#pragma once

#include "storage/Types.h"

#include <memory>
#include <optional>

namespace tp_project::storage {

class KVStore {
public:
    using pointer = std::shared_ptr<KVStore>;
    virtual ~KVStore() = default;

    [[nodiscard]] virtual auto get(KeyType key) const noexcept
        -> std::optional<ValueContainer> = 0;

    virtual auto append(KeyType key, ValueType value) noexcept -> void = 0;

    template <typename KVStoreType, typename... Args>
    static auto makeKVStore(Args &&...args) -> pointer
    {
        return std::make_shared<KVStoreType>(std::forward<Args>(args)...);
    }
};
}
