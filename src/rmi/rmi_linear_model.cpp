#include "rmi_linear_model.hpp"

namespace duckdb {

RMILinearModel::RMILinearModel()
    : slope(0.0),
      intercept(0.0),
      min_error(std::numeric_limits<int64_t>::max()),
      max_error(std::numeric_limits<int64_t>::min()) {
        // Constructor body (empty for now)
}

RMILinearModel::~RMILinearModel() {
    // Destructor body (empty for now)
}

void RMILinearModel::Train(const std::vector<std::pair<double, idx_t>> &data) {
    
    const idx_t n = data.size();
    
    if (n == 0) {
        slope = 0.0;
        intercept = 0.0;
        min_error = 0;
        max_error = 0;
        return;
    }

    long double sum_x = 0.0;
    long double sum_y = 0.0;
    long double sum_xy = 0.0;
    long double sum_x2 = 0.0;

    // Compute regression stats
    for (const auto &p : data) {
        const double x = p.first;
        const long double y = static_cast<long double>(p.second);

        sum_x += x;
        sum_y += y;
        sum_xy += (x * y);
        sum_x2 += (x * x);
    }

    long double denom = (n * sum_x2 - sum_x * sum_x);
    if (denom == 0) {
        // Base case: column has constant values
        slope = 0.0;
        intercept = static_cast<double>(data[0].second);
    } else {
        slope = static_cast<double>((n * sum_xy - sum_x * sum_y) / denom);
        intercept = static_cast<double>((sum_y - slope * sum_x) / n);
    }

    // Compute error bounds
    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    for (const auto &p : data) {
        const double key = p.first;
        const idx_t true_pos = p.second;

        const idx_t pred = Predict(key);
        const int64_t error = static_cast<int64_t>(true_pos) - static_cast<int64_t>(pred);

        if (error < min_error) min_error = error;
        if (error > max_error) max_error = error;
    }
}

idx_t RMILinearModel::Predict(double key) const {
    long double predicted = slope * key + intercept;

    if (predicted < 0.0)
        return 0;

    // No clamping to total_rows yet (unknown here)
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

// ---------------------------------------------------------------------------
// Serialize
// ---------------------------------------------------------------------------
// void RMILinearModel::Serialize(Serializer &serializer) const {

//     // Use Serializer::WriteProperty so serialization works for all serializer implementations
//     serializer.WriteProperty<double>(100, "slope", slope);
//     serializer.WriteProperty<double>(101, "intercept", intercept);

//     serializer.WriteProperty<int64_t>(102, "min_error", min_error);
//     serializer.WriteProperty<int64_t>(103, "max_error", max_error);

//     // Serialize overflow map directly (Serializer has support for duckdb::unordered_map)
//     serializer.WriteProperty<duckdb::unordered_map<double, std::vector<row_t>>>(104, "overflow_index", overflow_index);
// }

// ---------------------------------------------------------------------------
// Deserialize
// ---------------------------------------------------------------------------
// void RMILinearModel::Deserialize(Deserializer &deserializer) {

//     // Read properties with the same field ids used in Serialize
//     slope = deserializer.ReadProperty<double>(100, "slope");
//     intercept = deserializer.ReadProperty<double>(101, "intercept");

//     min_error = deserializer.ReadProperty<int64_t>(102, "min_error");
//     max_error = deserializer.ReadProperty<int64_t>(103, "max_error");

//     // Read overflow map
//     overflow_index = deserializer.ReadProperty<duckdb::unordered_map<double, std::vector<row_t>>>(104, "overflow_index");
// }

} // namespace duckdb
