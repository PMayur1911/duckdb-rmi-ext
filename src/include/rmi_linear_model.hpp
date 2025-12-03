#pragma once

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "rmi_base_model.hpp"

namespace duckdb {

class RMILinearModel : public BaseRMIModel {
public:
    RMILinearModel();
    ~RMILinearModel() override;

    // --- Model Parameters ---
    double slope;
    double intercept;

    // Error bounds
    int64_t min_error;
    int64_t max_error;

    // Overflow structure: key → row_ids
    duckdb::unordered_map<double, std::vector<row_t>> overflow_index;

    // --- Interface Methods ---
    void Train(const std::vector<std::pair<double, idx_t>> &data) override;
    idx_t Predict(double key) const override;
    std::pair<idx_t, idx_t> GetSearchBounds(double key, idx_t total_rows) const override;

    void InsertIntoOverflow(double key, row_t row_id) override;
    void DeleteFromOverflow(double key, row_t row_id) override;

    // Return pointer to overflow row ids vector for `key`, or nullptr if not present
    const std::vector<row_t> *GetOverflowRowIDs(double key) const;

    int64_t GetMinError() const override { return min_error; }
    int64_t GetMaxError() const override { return max_error; }

    const std::unordered_map<double, std::vector<row_t>>& GetOverflowMap() const override { return overflow_index; }

    idx_t PredictPosition(double key) const override { return Predict(key); }

};

} // namespace duckdb
