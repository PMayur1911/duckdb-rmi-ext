#pragma once

#include "rmi_base_model.hpp"
#include <cmath>
#include <algorithm>

namespace duckdb {

class RMITwoLayerModel : public BaseRMIModel {
public:
    // -------------------------
    // Stage 1: Root Linear Model
    // -------------------------
    double root_slope = 0.0;
    double root_intercept = 0.0;

    // -------------------------
    // Stage 2: Piecewise Linear Models
    // -------------------------
    vector<double> leaf_slopes;
    vector<double> leaf_intercepts;
    vector<idx_t> segment_bounds;   // size K+1, bounds of segments
    idx_t K = 0;                    // number of segments

    // Global error bounds
    int64_t min_error = 0;
    int64_t max_error = 0;

    // Overflow (key -> row_ids)
    duckdb::unordered_map<double, std::vector<row_t>> overflow_index;

    // Window radius for local scan
    idx_t window_radius = 64;

public:
    RMITwoLayerModel() = default;
    ~RMITwoLayerModel() override = default;

    // -------------------------
    // Main Interface
    // -------------------------
    void Train(const std::vector<std::pair<double, idx_t>> &data) override;
    idx_t Predict(double key) const override;
    pair<idx_t, idx_t> GetSearchBounds(double key, idx_t total_rows) const override;

    void InsertIntoOverflow(double key, row_t row_id) override;
    void DeleteFromOverflow(double key, row_t row_id) override;

    int64_t GetMinError() const override { return min_error; }
    int64_t GetMaxError() const override { return max_error; }

    const std::unordered_map<double, std::vector<row_t>>& GetOverflowMap() const override {
        return overflow_index;
    }

    idx_t PredictPosition(double key) const override { return Predict(key); }

private:

    // -------------------------
    // Stage 1: root-model fitting
    // -------------------------
    void TrainRootModel(const std::vector<std::pair<double, idx_t>> &data);

    // -------------------------
    // Stage 2: segment assignment + local linear fits
    // -------------------------
    void BuildSegments(const std::vector<std::pair<double, idx_t>> &data);

    idx_t PredictSegment(double key) const;
    idx_t ClampSegment(idx_t s) const;

    // Evaluate leaf model
    idx_t PredictLeaf(idx_t segment, double key) const;

};

} // namespace duckdb
