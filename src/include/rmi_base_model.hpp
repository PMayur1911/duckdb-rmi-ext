#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class BaseRMIModel {
public:
    virtual ~BaseRMIModel() = default;

    virtual void Train(const std::vector<std::pair<double, idx_t>> &data) = 0;
    virtual idx_t Predict(double key) const = 0;
    virtual std::pair<idx_t, idx_t> GetSearchBounds(double key, idx_t total_rows) const = 0;

    // Overflow handling
    virtual void InsertIntoOverflow(double key, row_t row_id) = 0;
    virtual void DeleteFromOverflow(double key, row_t row_id) = 0;

    // Persistence
    // virtual void Serialize(Serializer &serializer) const = 0;
    // virtual void Deserialize(Deserializer &deserializer) = 0;

    // Error bounds
    virtual int64_t GetMinError() const = 0;
    virtual int64_t GetMaxError() const = 0;

    // Overflow structure
    virtual const std::unordered_map<double, std::vector<row_t>>& GetOverflowMap() const = 0;

    // Predict position (alias for Predict)
    virtual idx_t PredictPosition(double key) const = 0;

};

} // namespace duckdb
