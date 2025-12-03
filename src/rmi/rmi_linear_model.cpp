#include "rmi_linear_model.hpp"

#include <fstream>
#include <sstream>

namespace duckdb {

RMILinearModel::RMILinearModel()
    : slope(0.0),
      intercept(0.0),
      min_error(std::numeric_limits<int64_t>::max()),
      max_error(std::numeric_limits<int64_t>::min()) {
        model_name = "RMILinearModel";
}

RMILinearModel::~RMILinearModel() {}

static void RMILog(const std::string &msg) {
    std::ofstream log("/tmp/rmi_model.log", std::ios::app);
    if (log.is_open()) {
        log << msg << std::endl;
        log.close();
    }
}

void RMILinearModel::Train(const std::vector<std::pair<double, idx_t>> &data) {
    const idx_t n = data.size();

    if (n == 0) {
        slope = 0;
        intercept = 0;
        min_error = 0;
        max_error = 0;
        return;
    }

    long double mean_x = 0, mean_y = 0;

    for (auto &p : data) {
        mean_x += p.first;
        mean_y += p.second;
    }

    mean_x /= n;
    mean_y /= n;

    long double num = 0, den = 0;

    for (auto &p : data) {
        long double dx = p.first - mean_x;
        long double dy = (long double)p.second - mean_y;
        num += dx * dy;
        den += dx * dx;
    }

    if (den == 0) {
        slope = 0;
        intercept = mean_y;
    } else {
        slope = (double)(num / den);
        intercept = (double)(mean_y - slope * mean_x);
    }

    // Error Bounds
    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    for (auto &p : data) {
        long double pred = slope * p.first + intercept;
        int64_t err = (int64_t)p.second - (int64_t)pred;

        min_error = std::min(min_error, err);
        max_error = std::max(max_error, err);
    }
}


idx_t RMILinearModel::Predict(double key) const {
    long double predicted = slope * key + intercept;

    if (predicted < 0.0)
        return 0;

    return static_cast<idx_t>(predicted);
}

// GetSearchBounds: Return [predicted + min_error, predicted + max_error]
std::pair<idx_t, idx_t> RMILinearModel::GetSearchBounds(double key,
                                                        idx_t total_rows) const {
    const idx_t predicted = Predict(key);

    long long lo = static_cast<long long>(predicted) + min_error;
    long long hi = static_cast<long long>(predicted) + max_error;

    if (lo < 0) lo = 0;
    if (hi < 0) hi = 0;

    if (hi >= static_cast<long long>(total_rows))
        hi = total_rows - 1;

    if (lo >= static_cast<long long>(total_rows))
        lo = total_rows - 1;

    return {static_cast<idx_t>(lo), static_cast<idx_t>(hi)};
}


void RMILinearModel::InsertIntoOverflow(double key, row_t row_id) {
    overflow_index[key].push_back(row_id);
}

void RMILinearModel::DeleteFromOverflow(double key, row_t row_id) {
    auto it = overflow_index.find(key);
    if (it == overflow_index.end()) {
        return;
    }

    auto &vec = it->second;

    vec.erase(std::remove(vec.begin(), vec.end(), row_id), vec.end());

    if (vec.empty()) {
        overflow_index.erase(it);
    }
}

const std::vector<row_t> *RMILinearModel::GetOverflowRowIDs(double key) const {
    auto it = overflow_index.find(key);
    if (it == overflow_index.end()) {
        return nullptr;
    }
    return &it->second;
}

} // namespace duckdb
