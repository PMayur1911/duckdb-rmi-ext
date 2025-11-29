#include "rmi_two_layer_model.hpp"
#include <limits>
#include <cmath>

namespace duckdb {

// =====================================================
// Utility: clamp predicted index into [0, total_rows-1]
// =====================================================
static inline idx_t ClampIndex(long double x, idx_t total_rows) {
    if (x < 0) return 0;
    if ((idx_t)x >= total_rows) return total_rows - 1;
    return (idx_t)x;
}

// =====================================================
// Stage-1: Train Root Linear Model
// =====================================================
void RMITwoLayerModel::TrainRootModel(const std::vector<std::pair<double, idx_t>> &data) {
    const idx_t n = data.size();
    if (n == 0) {
        root_slope = 0.0;
        root_intercept = 0.0;
        return;
    }

    long double sx = 0, sy = 0, sxy = 0, sx2 = 0;
    for (auto &p : data) {
        double x = p.first;
        long double y = p.second;
        sx += x;
        sy += y;
        sxy += (x * y);
        sx2 += (x * x);
    }

    long double denom = (n * sx2 - sx * sx);
    if (denom == 0) {
        root_slope = 0.0;
        root_intercept = (long double)data[0].second;
    } else {
        root_slope = (double)((n * sxy - sx * sy) / denom);
        root_intercept = (double)((sy - root_slope * sx) / n);
    }
}

// =====================================================
// Predict segment (float), then cast to idx_t
// =====================================================
idx_t RMITwoLayerModel::PredictSegment(double key) const {
    long double seg = root_slope * key + root_intercept;

    if (seg < 0) return 0;
    if ((idx_t)seg >= K) return K - 1;
    return (idx_t)seg;
}

idx_t RMITwoLayerModel::ClampSegment(idx_t s) const {
    if (s >= K) return K - 1;
    return s;
}

// =====================================================
// Stage-2: build segments + train local linear fits
// =====================================================
void RMITwoLayerModel::BuildSegments(const std::vector<std::pair<double, idx_t>> &data) {
    const idx_t n = data.size();
    if (n == 0) {
        K = 0;
        return;
    }

    // NUMBER OF SEGMENTS
    K = (idx_t)floor(sqrt((double)n));
    if (K == 0) K = 1;

    leaf_slopes.resize(K);
    leaf_intercepts.resize(K);
    segment_bounds.resize(K + 1);

    idx_t seg_size = n / K;
    if (seg_size < 1) seg_size = 1;

    idx_t start = 0;

    for (idx_t seg = 0; seg < K; seg++) {
        idx_t end = (seg == K - 1) ? n : start + seg_size;
        segment_bounds[seg] = start;

        if (end - start < 2) {
            leaf_slopes[seg] = 0;
            leaf_intercepts[seg] = start;
            start = end;
            continue;
        }

        long double sx = 0, sy = 0, sxy = 0, sx2 = 0;
        for (idx_t i = start; i < end; i++) {
            double x = data[i].first;
            long double y = data[i].second;
            sx += x;
            sy += y;
            sxy += x * y;
            sx2 += x * x;
        }

        long double denom = ((end - start) * sx2 - sx * sx);
        if (denom == 0) {
            leaf_slopes[seg] = 0.0;
            leaf_intercepts[seg] = (double)start;
        } else {
            leaf_slopes[seg] = (double)(((end - start) * sxy - sx * sy) / denom);
            leaf_intercepts[seg] = (double)((sy - leaf_slopes[seg] * sx) / (end - start));
        }

        start = end;
    }

    segment_bounds[K] = n;
}

// =====================================================
// Predict using stage-2 leaf model
// =====================================================
idx_t RMITwoLayerModel::PredictLeaf(idx_t seg, double key) const {
    long double pos = leaf_slopes[seg] * key + leaf_intercepts[seg];
    return (idx_t)pos;
}

// =====================================================
// Train entire RMI (Stage-1 + Stage-2)
// =====================================================
void RMITwoLayerModel::Train(const std::vector<std::pair<double, idx_t>> &data) {
    if (data.empty()) {
        root_slope = 0;
        root_intercept = 0;
        K = 0;
        return;
    }

    // Stage 1
    TrainRootModel(data);

    // Stage 2
    BuildSegments(data);

    // Compute global error bounds
    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    const idx_t n = data.size();

    for (auto &p : data) {
        double key = p.first;
        idx_t truth = p.second;

        idx_t seg_pred = PredictSegment(key);

        // candidate segments: seg-1, seg, seg+1
        idx_t segs[3] = {
            ClampSegment(seg_pred),
            ClampSegment(seg_pred > 0 ? seg_pred - 1 : 0),
            ClampSegment(seg_pred + 1)
        };

        // best local prediction
        idx_t local_pred = PredictLeaf(segs[0], key);
        for (int j = 1; j < 3; j++) {
            idx_t alt = PredictLeaf(segs[j], key);
            if (llabs((long long)truth - (long long)alt) < llabs((long long)truth - (long long)local_pred)) {
                local_pred = alt;
            }
        }

        long long err = (long long)truth - (long long)local_pred;

        if (err < min_error) min_error = err;
        if (err > max_error) max_error = err;
    }
}

// =====================================================
// Predict final row position (approx)
// =====================================================
idx_t RMITwoLayerModel::Predict(double key) const {
    if (K == 0) return 0;

    idx_t seg = PredictSegment(key);
    return PredictLeaf(seg, key);
}

// =====================================================
// Return tight search bounds for binary search
// =====================================================
pair<idx_t, idx_t> RMITwoLayerModel::GetSearchBounds(double key, idx_t total_rows) const {
    idx_t pred = Predict(key);
    long long lo = (long long)pred + min_error;
    long long hi = (long long)pred + max_error;

    if (lo < 0) lo = 0;
    if (hi >= (long long)total_rows) hi = total_rows - 1;

    return {(idx_t)lo, (idx_t)hi};
}

// =====================================================
// Overflow Handling
// =====================================================
void RMITwoLayerModel::InsertIntoOverflow(double key, row_t row_id) {
    overflow_index[key].push_back(row_id);
}

void RMITwoLayerModel::DeleteFromOverflow(double key, row_t row_id) {
    auto it = overflow_index.find(key);
    if (it == overflow_index.end()) return;

    auto &vec = it->second;
    vec.erase(remove(vec.begin(), vec.end(), row_id), vec.end());
    if (vec.empty()) overflow_index.erase(it);
}

} // namespace duckdb
