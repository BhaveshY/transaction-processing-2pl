#pragma once

#include <boost/json.hpp>

#include "transactions/Operations.h"

#include <memory>
#include <vector>
namespace tp_project::transactions {
class Transaction {
    std::vector<Operation::pointer> ops;

public:
    Transaction(Transaction &) = delete;

    explicit Transaction(const boost::json::array &transaction);

    Operation::pointer &operator[](int64_t idx);

    auto begin() { return ops.begin(); }

    auto end() { return ops.end(); }
    
    // Const iterators for const reference access
    auto begin() const { return ops.begin(); }

    auto end() const { return ops.end(); }
};
}