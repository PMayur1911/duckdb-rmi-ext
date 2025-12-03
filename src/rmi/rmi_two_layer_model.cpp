#include "rmi_two_layer_model.hpp"
#include <limits>
#include <cmath>

namespace duckdb {

RMITwoLayerModel::RMITwoLayerModel()
    : min_error(std::numeric_limits<int64_t>::max()),
      max_error(std::numeric_limits<int64_t>::min()) {
    model_name = "RMITwoLayerModel";
}

// ------------------------------------------------------------------
// Utility clamp
// ------------------------------------------------------------------
static inline idx_t ClampIndex(long double x, idx_t total_rows) {
    if (x < 0) return 0;
    if ((idx_t)x >= total_rows) return total_rows - 1;
    return (idx_t)x;
}

// ------------------------------------------------------------------
// Stage 1: Train root linear regression
// ------------------------------------------------------------------
void RMITwoLayerModel::TrainRootModel(const std::vector<std::pair<double,idx_t>> &data) {
    const idx_t n = data.size();
    if (n == 0) {
        root_slope = 0;
        root_intercept = 0;
        return;
    }

    long double sum_x = 0, sum_y = 0;
    for (auto &p : data) {
        sum_x += p.first;
        sum_y += p.second;
    }

    long double mean_x = sum_x / n;
    long double mean_y = sum_y / n;

    long double Sxx = 0, Sxy = 0;
    for (auto &p : data) {
        long double xc = p.first - mean_x;
        long double yc = p.second - mean_y;
        Sxx += xc * xc;
        Sxy += xc * yc;
    }

    if (fabsl(Sxx) < 1e-18) {
        root_slope = 0.0;
        root_intercept = (double)mean_y;
    } else {
        root_slope = (double)(Sxy / Sxx);
        root_intercept = (double)(mean_y - root_slope * mean_x);
    }
}


// ------------------------------------------------------------------
// Predict segment id
// ------------------------------------------------------------------
idx_t RMITwoLayerModel::PredictSegment(double key) const {
    long double seg = root_slope * key + root_intercept;
    if (seg < 0) return 0;
    return (idx_t)std::min((long double)(K - 1), seg);
}

// ------------------------------------------------------------------
// Stage 2: Build leaf models on contiguous partitions
// ------------------------------------------------------------------
void RMITwoLayerModel::BuildSegments(const std::vector<std::pair<double, idx_t>> &data) {
    const idx_t n = data.size();
    if (n == 0) {
        K = 0;
        return;
    }

    // Number of segments ~ sqrt(N)
    K = (idx_t)floor(sqrt((double)n));
    if (K == 0) K = 1;

    leaf_slopes.resize(K);
    leaf_intercepts.resize(K);
    segment_bounds.resize(K + 1);

    idx_t seg_size = n / K;
    if (seg_size < 1) seg_size = 1;

    idx_t start = 0;

    for (idx_t seg = 0; seg < K; seg++) {
        idx_t end = (seg == K - 1 ? n : start + seg_size);
        segment_bounds[seg] = start;

        idx_t count = end - start;

        // --- Edge case: 1 point or 0 points in segment ---
        if (count < 2) {
            leaf_slopes[seg] = 0.0;
            leaf_intercepts[seg] = (double)start;
            start = end;
            continue;
        }

        // ---------------------------------------------
        // NUMERICALLY STABLE MEAN-CENTERED REGRESSION
        // ---------------------------------------------

        long double sum_x = 0.0, sum_y = 0.0;

        for (idx_t i = start; i < end; i++) {
            sum_x += data[i].first;
            sum_y += data[i].second;
        }

        long double mean_x = sum_x / (long double)count;
        long double mean_y = sum_y / (long double)count;

        long double Sxx = 0.0;
        long double Sxy = 0.0;

        for (idx_t i = start; i < end; i++) {
            long double xc = (long double)data[i].first - mean_x;
            long double yc = (long double)data[i].second - mean_y;
            Sxx += xc * xc;
            Sxy += xc * yc;
        }

        // If Sxx is almost zero → vertical collapse → constant model
        if (fabsl(Sxx) < 1e-18) {
            leaf_slopes[seg] = 0.0;
            leaf_intercepts[seg] = (double)mean_y;
        } else {
            long double slope = Sxy / Sxx;
            long double intercept = mean_y - slope * mean_x;

            leaf_slopes[seg] = (double)slope;
            leaf_intercepts[seg] = (double)intercept;
        }

        start = end;
    }

    segment_bounds[K] = n;
}


// ------------------------------------------------------------------
// Predict using leaf model
// ------------------------------------------------------------------
idx_t RMITwoLayerModel::PredictLeaf(idx_t seg, double key) const {
    long double pos = leaf_slopes[seg] * key + leaf_intercepts[seg];
    return (idx_t)pos;
}

// ------------------------------------------------------------------
// Train full RMI (root + leaves + global error bounds)
// ------------------------------------------------------------------
void RMITwoLayerModel::Train(const std::vector<std::pair<double,idx_t>> &data) {
    const idx_t n = data.size();
    if (n == 0) {
        root_slope = 0; root_intercept = 0; K = 0;
        min_error = max_error = 0;
        return;
    }

    TrainRootModel(data);
    BuildSegments(data);

    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    for (auto &p : data) {
        double key = p.first;
        idx_t truth = p.second;

        idx_t seg0 = PredictSegment(key);

        idx_t candidates[3] = {
            ClampSegment(seg0),
            ClampSegment(seg0 > 0 ? seg0 - 1 : 0),
            ClampSegment(seg0 + 1)
        };

        idx_t best_pred = PredictLeaf(candidates[0], key);
        for (int j = 1; j < 3; j++) {
            idx_t alt = PredictLeaf(candidates[j], key);
            if (llabs((long long)truth - (long long)alt) <
                llabs((long long)truth - (long long)best_pred)) {
                best_pred = alt;
            }
        }

        long long err = (long long)truth - (long long)best_pred;
        if (err < min_error) min_error = err;
        if (err > max_error) max_error = err;
    }
}

// ------------------------------------------------------------------
// Predict approximate position
// ------------------------------------------------------------------
idx_t RMITwoLayerModel::Predict(double key) const {
    if (K == 0) return 0;
    idx_t seg = PredictSegment(key);
    return PredictLeaf(seg, key);
}

// ------------------------------------------------------------------
// Return [low, high] search window
// ------------------------------------------------------------------
pair<idx_t,idx_t> RMITwoLayerModel::GetSearchBounds(double key,
                                                    idx_t total_rows) const {
    idx_t pred = Predict(key);

    long long lo = (long long)pred + min_error;
    long long hi = (long long)pred + max_error;

    if (lo < 0) lo = 0;
    if (hi >= (long long)total_rows) hi = total_rows - 1;

    return {(idx_t)lo, (idx_t)hi};
}

// ------------------------------------------------------------------
// Overflow
// ------------------------------------------------------------------
void RMITwoLayerModel::InsertIntoOverflow(double key, row_t row_id) {
    overflow_index[key].push_back(row_id);
}

void RMITwoLayerModel::DeleteFromOverflow(double key, row_t row_id) {
    auto it = overflow_index.find(key);
    if (it == overflow_index.end()) return;

    auto &vec = it->second;
    vec.erase(std::remove(vec.begin(), vec.end(), row_id), vec.end());
    if (vec.empty())
        overflow_index.erase(it);
}

} // namespace duckdb
