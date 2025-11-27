#include "src/include/rmi/rmi_model.hpp"
#include <limits>
namespace duckdb {

RMIModel::RMIModel() : slope(0), intercept(0), min_error(0), max_error(0) {
    // Constructor initializes the model
}

RMIModel::~RMIModel() {
    // Destructor
}

// --- RMIModel::Train ---
// This function fits a simple linear regression model to the
// provided data and calculates the min/max error bounds.
void RMIModel::Train(const std::vector<std::pair<double, int64_t>> &data) {
    
    // --- 1. Fit the Linear Regression Model ---
    
    double n = data.size();
    if (n == 0) {
        slope = 0.0;
        intercept = 0.0;
        min_error = 0;
        max_error = 0;
        return;
    }

    double sum_x = 0.0;
    double sum_y = 0.0;
    double sum_xy = 0.0;
    double sum_xx = 0.0;

    for (const auto& pair : data) {
        double x = pair.first;         // key
        double y = (double)pair.second; // position (rank)

        sum_x += x;
        sum_y += y;
        sum_xy += x * y;
        sum_xx += x * x;
    }

    // Calculate slope (a) and intercept (b)
    // a = (N * sum(xy) - sum(x) * sum(y)) / (N * sum(xx) - sum(x)^2)
    // b = (sum(y) - a * sum(x)) / N
    
    double denominator = (n * sum_xx) - (sum_x * sum_x);

    // Check for "all keys are the same" (vertical line)
    // which would cause a divide-by-zero.
    if (std::abs(denominator) < 1e-9) {
        slope = 0.0;
        intercept = sum_y / n;
    } else {
        slope = ((n * sum_xy) - (sum_x * sum_y)) / denominator;
        intercept = (sum_y - (slope * sum_x)) / n;
    }
    
    // Handle invalid model parameters (NaN/Inf)
    if (std::isnan(slope) || std::isinf(slope)) {
        slope = 0.0;
        intercept = sum_y / n;
    }
    
    // --- 2. Calculate Min/Max Error Bounds ---
    min_error = std::numeric_limits<int64_t>::max();
    max_error = std::numeric_limits<int64_t>::min();

    for (const auto& pair : data) {
        double key = pair.first;
        int64_t actual_pos = pair.second;
        
        // Get the model's prediction for this key
        int64_t predicted_pos = PredictPosition(key);
        
        // Calculate the error
        // error = actual_position - predicted_position
        int64_t error = actual_pos - predicted_pos;

        // Update bounds
        if (error < min_error) {
            min_error = error;
        }
        if (error > max_error) {
            max_error = error;
        }
    }
}

// --- RMIModel::PredictPosition ---
// Predicts the position of a single key
int64_t RMIModel::PredictPosition(double key) const {
    return (key * slope) + intercept;
}

// --- RMIModel::GetSearchBounds ---
// Get the [start, end] search bounds for a key
pair<int64_t, int64_t> RMIModel::GetSearchBounds(double key, idx_t data_size) const {
    int64_t predicted_pos = PredictPosition(key);
    
    int64_t start_pos = std::max((int64_t)0, predicted_pos + min_error);
    int64_t end_pos   = std::min((int64_t)data_size, predicted_pos + max_error);
    
    return {start_pos, end_pos};
}

// --- RMIModel::InsertIntoOverflow ---
// Insert a new (key, row_id) pair into the overflow index
void RMIModel::InsertIntoOverflow(double key, row_t row_id) {
    overflow_index[key].insert(row_id);
}

// --- RMIModel::DeleteFromOverflow ---
// Delete a (key, row_id) pair from the overflow index
void RMIModel::DeleteFromOverflow(double key, row_t row_id) {
    auto it = overflow_index.find(key);
    
    if (it != overflow_index.end()) {
        
        it->second.erase(row_id);
        
        if (it->second.empty()) {
            overflow_index.erase(it);
        }
    }
}

} // namespace duckdb