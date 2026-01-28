#pragma once
namespace tp_project::transactions {
class AppendOp;
class ReadOp;
/**
 * This class is an interface for an operation visitor to execute the operations
 */
class OperationExecutor {
public:
    virtual ~OperationExecutor() = default;
    virtual void visitReadOp(const ReadOp *readOp) = 0;
    virtual void visitAppendOp(const AppendOp *appendOp) = 0;
};
}