#pragma once
#include <boost/json/object.hpp>

#include "storage/Types.h"

#include <cstdint>
#include <memory>
namespace tp_project::transactions {
class OperationExecutor;

class Operation {
    KeyType m_key;
    ValueType m_value;

protected:
    Operation(const KeyType key, const ValueType value)
        : m_key { key }
        , m_value { value }
    {
    }

public:
    using pointer = std::unique_ptr<Operation>;
    enum class Type : uint8_t {
        READ,
        APPEND,
    };

    enum class OpFields : uint8_t { OP = 0, KEY = 1, VALUE = 2 };

    [[nodiscard]] virtual auto type() const -> Type = 0;

    [[nodiscard]] virtual auto key() const -> KeyType { return m_key; }
    [[nodiscard]] virtual auto value() const -> ValueType { return m_value; }
    virtual void Accept(OperationExecutor *visitor) const = 0;

    virtual ~Operation() = default;

    static Type parse_type(const boost::json::array &operation);

    static pointer makeOperation(
        const boost::json::array &operation);
};

class ReadOp final : public Operation {
public:
    [[nodiscard]] Type type() const override { return Type::READ; }

    explicit ReadOp(const KeyType key)
        : Operation(key, -1)
    {
    }
    void Accept(OperationExecutor *visitor) const override;
};

class AppendOp final : public Operation {
public:
    [[nodiscard]] Type type() const override { return Type::APPEND; }

    explicit AppendOp(const KeyType key, const ValueType value)
        : Operation(key, value)
    {
    }
    void Accept(OperationExecutor *visitor) const override;
};
}