#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/enums/expression_type.hpp"

namespace duckdb {
class DuckTableEntry;
class Index;

// This is created by the optimizer rule or deserialization
struct RMIIndexScanBindData final : public TableFunctionData {
    explicit RMIIndexScanBindData(DuckTableEntry &table, Index &index)
        : table(table), index(index) {
        // Initialize with safe defaults (NULL values and INVALID expressions)
        values[0] = Value();
        values[1] = Value();
        expressions[0] = ExpressionType::INVALID;
        expressions[1] = ExpressionType::INVALID;
    }

    //! The table to scan
    DuckTableEntry &table;

    //! The index to use
    Index &index;

    //! The Predicates to scan.
    //! Index 0: Low bound or Equality value
    //! Index 1: High bound (for range/between)
    Value values[2];

    //! The comparison types (e.g., EQUAL, GREATERTHAN, etc.)
    ExpressionType expressions[2];

public:
    bool Equals(const FunctionData &other_p) const override {
        auto &other = other_p.Cast<RMIIndexScanBindData>();
        // Two bind data objects are equal if they target the same table and index
        return &other.table == &table && &other.index == &index;
    }
};

struct RMIIndexScanFunction {
    static TableFunction GetFunction();
};

} // namespace duckdb