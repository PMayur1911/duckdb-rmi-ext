#include "rmi_poly_model.hpp"
#include <cmath>
#include <limits>
#include <algorithm>

namespace duckdb {

RMIPolyModel::RMIPolyModel()
    : max_degree(6),
      min_error(std::numeric_limits<int64_t>::max()),
      max_error(std::numeric_limits<int64_t>::min()) {
    model_name = "RMIPolyModel";
}

RMIPolyModel::~RMIPolyModel() {}


bool RMIPolyModel::SolveLinearSystem(std::vector<std::vector<double>> &A, std::vector<double> &b, std::vector<double> &x_out) const {
    size_t n = A.size();
    x_out.assign(n, 0.0);

    for (size_t i = 0; i < n; i++) {
        // Pivot selection
        size_t pivot = i;
        double max_abs = fabs(A[i][i]);
        for (size_t r = i + 1; r < n; r++) {
            if (fabs(A[r][i]) > max_abs) {
                pivot = r;
                max_abs = fabs(A[r][i]);
            }
        }
        if (max_abs < 1e-12) return false;

        if (pivot != i) {
            std::swap(A[i], A[pivot]);
            std::swap(b[i], b[pivot]);
        }

        double diag = A[i][i];
        for (size_t c = i; c < n; c++)
            A[i][c] /= diag;
        b[i] /= diag;

        for (size_t r = i + 1; r < n; r++) {
            double f = A[r][i];
            if (fabs(f) < 1e-12) continue;

            for (size_t c = i; c < n; c++)
                A[r][c] -= f * A[i][c];
            b[r] -= f * b[i];
        }
    }

    for (int i = (int)n - 1; i >= 0; i--) {
        double v = b[i];
        for (size_t c = i + 1; c < n; c++)
            v -= A[i][c] * x_out[c];
        x_out[i] = v;
    }
    return true;
}

// Fit best polynomial (degree 1..max_degree), choose smallest MSE
std::vector<double> RMIPolyModel::FitBestPolynomial(const std::vector<double> &x, const std::vector<double> &y, int max_degree) const {
    size_t n = x.size();
    std::vector<double> best = {0.0, 1.0};
    double best_mse = std::numeric_limits<double>::infinity();

    for (int d = 1; d <= max_degree; d++) {
        int m = d + 1;
        std::vector<std::vector<double>> ATA(m, std::vector<double>(m, 0.0));
        std::vector<double> ATy(m, 0.0);

        for (size_t i = 0; i < n; i++) {
            double xp[32];
            xp[0] = 1.0;
            for (int k = 1; k <= d; k++)
                xp[k] = xp[k-1] * x[i];

            for (int r = 0; r < m; r++) {
                ATy[r] += xp[r] * y[i];
                for (int c = 0; c < m; c++)
                    ATA[r][c] += xp[r] * xp[c];
            }
        }

        std::vector<double> coeffs;
        if (!SolveLinearSystem(ATA, ATy, coeffs))
            continue;

        double sse = 0.0;
        for (size_t i = 0; i < n; i++) {
            double pred = EvalPolynomial(coeffs, x[i]);
            double diff = y[i] - pred;
            sse += diff * diff;
        }
        double mse = sse / double(n);

        if (mse < best_mse) {
            best_mse = mse;
            best = coeffs;
        }
    }
    return best;
}

double RMIPolyModel::EvalPolynomial(const std::vector<double> &coeffs,
                                    double x) const {
    double r = 0.0;
    for (size_t i = coeffs.size(); i-- > 0; )
        r = r * x + coeffs[i];
    return r;
}

// Train Model
void RMIPolyModel::Train(const std::vector<std::pair<double, idx_t>> &data) {

    const idx_t n = data.size();
    if (n == 0) {
        coeffs = {0.0};
        min_error = max_error = 0;
        return;
    }

    std::vector<double> x(n), y(n);
    for (idx_t i = 0; i < n; i++) {
        x[i] = data[i].first;
        y[i] = double(data[i].second);
    }

    coeffs = FitBestPolynomial(x, y, max_degree);

    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    for (idx_t i = 0; i < n; i++) {
        double key = data[i].first;
        idx_t true_pos = data[i].second;
        idx_t pred = Predict(key);

        int64_t err = int64_t(true_pos) - int64_t(pred);
        if (err < min_error) min_error = err;
        if (err > max_error) max_error = err;
    }
}

idx_t RMIPolyModel::Predict(double key) const {
    double p = EvalPolynomial(coeffs, key);
    if (p < 0) return 0;
    return idx_t(p);
}

// Search Bounds
std::pair<idx_t, idx_t> RMIPolyModel::GetSearchBounds(double key,
                                                      idx_t total_rows) const {
    idx_t predicted = Predict(key);

    long long lo = static_cast<long long>(predicted) + min_error;
    long long hi = static_cast<long long>(predicted) + max_error;

    if (lo < 0) lo = 0;
    if (hi < 0) hi = 0;

    if (hi >= static_cast<long long>(total_rows))
        hi = total_rows - 1;
    if (lo >= static_cast<long long>(total_rows))
        lo = total_rows - 1;

    return {idx_t(lo), idx_t(hi)};
}

// Overflow Handling
void RMIPolyModel::InsertIntoOverflow(double key, row_t row_id) {
    overflow_index[key].push_back(row_id);
}

void RMIPolyModel::DeleteFromOverflow(double key, row_t row_id) {
    auto it = overflow_index.find(key);
    if (it == overflow_index.end()) return;

    auto &vec = it->second;
    vec.erase(std::remove(vec.begin(), vec.end(), row_id), vec.end());

    if (vec.empty())
        overflow_index.erase(it);
}

} // namespace duckdb
