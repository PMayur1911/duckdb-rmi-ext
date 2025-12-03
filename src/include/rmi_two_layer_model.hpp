#pragma once

#include "rmi_base_model.hpp"
#include <vector>

namespace duckdb {

class RMITwoLayerModel : public BaseRMIModel {
public:
    RMITwoLayerModel();
    ~RMITwoLayerModel() override = default;

    string model_name;

    // Stage-1 root linear model
    double root_slope = 0.0;
    double root_intercept = 0.0;

    // Stage-2: leaf linear models
    idx_t K = 0;
    std::vector<double> leaf_slopes;
    std::vector<double> leaf_intercepts;
    std::vector<idx_t> segment_bounds;

    // Error bounds
    int64_t min_error;
    int64_t max_error;

    // Overflow map
    unordered_map<double, std::vector<row_t>> overflow_index;

    // Core API
    void Train(const std::vector<std::pair<double, idx_t>> &data) override;

    idx_t Predict(double key) const override;
    std::pair<idx_t,idx_t> GetSearchBounds(double key, idx_t total_rows) const override;

    void InsertIntoOverflow(double key, row_t row_id) override;
    void DeleteFromOverflow(double key, row_t row_id) override;

    const unordered_map<double, std::vector<row_t>>& GetOverflowMap() const override {
        return overflow_index;
    }

    idx_t PredictPosition(double key) const override { return Predict(key); }

    int64_t GetMinError() const override { return min_error; }
    int64_t GetMaxError() const override { return max_error; }

private:
    void TrainRootModel(const std::vector<std::pair<double, idx_t>> &data);
    void BuildSegments(const std::vector<std::pair<double, idx_t>> &data);

    idx_t PredictSegment(double key) const;
    idx_t PredictLeaf(idx_t seg, double key) const;

    inline idx_t ClampSegment(idx_t s) const {
        return (s >= K ? K - 1 : s);
    }
};

} // namespace duckdb
