#pragma once

#include "rmi_base_model.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class RMIPolyModel : public BaseRMIModel {
public:
    RMIPolyModel();
    ~RMIPolyModel() override;

    // Polynomial coefficients a0, a1, ..., ad
    std::vector<double> coeffs;

    // Max polynomial degree to consider during training
    int max_degree;

    // Error bounds
    int64_t min_error;
    int64_t max_error;

    // Overflow structure
    duckdb::unordered_map<double, std::vector<row_t>> overflow_index;

    // --- Model API ---
    void Train(const std::vector<std::pair<double, idx_t>> &data) override;
    idx_t Predict(double key) const override;
    std::pair<idx_t, idx_t> GetSearchBounds(double key, idx_t total_rows) const override;

    void InsertIntoOverflow(double key, row_t row_id) override;
    void DeleteFromOverflow(double key, row_t row_id) override;

    int64_t GetMinError() const override { return min_error; }
    int64_t GetMaxError() const override { return max_error; }

    const std::unordered_map<double, std::vector<row_t>>& GetOverflowMap() const override {
        return overflow_index;
    }

    idx_t PredictPosition(double key) const override { return Predict(key); }

private:
    // --- Regression helpers (embedded utils) ---
    bool SolveLinearSystem(std::vector<std::vector<double>> &A,
                           std::vector<double> &b,
                           std::vector<double> &x_out) const;

    std::vector<double> FitBestPolynomial(const std::vector<double> &x,
                                          const std::vector<double> &y,
                                          int max_degree) const;

    double EvalPolynomial(const std::vector<double> &coeffs, double x) const;

};

} // namespace duckdb
