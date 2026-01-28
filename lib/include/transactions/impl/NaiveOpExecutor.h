#pragma once
#include "storage/KVStore.h"
#include "transactions/OperationExecutor.h"

#include <optional>
namespace tp_project::transactions::impl {
class NaiveOpExecutor final : public OperationExecutor {
    storage::KVStore::pointer store;
    std::optional<ValueContainer> opResult;

public:
    explicit NaiveOpExecutor(storage::KVStore::pointer storage) noexcept;
    ~NaiveOpExecutor() override = default;
    void visitReadOp(const ReadOp *readOp) override;
    void visitAppendOp(const AppendOp *appendOp) override;
    auto result() -> std::optional<ValueContainer>;
};
}