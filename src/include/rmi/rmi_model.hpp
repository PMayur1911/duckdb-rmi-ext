#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"

namespace duckdb {

class RMIModel {
public:
    RMIModel();
    ~RMIModel();

    // --- Model Parameters ---
    // The linear regression model: pos = (key * slope) + intercept
    double slope;
    double intercept;

    // The learned error bounds
    int64_t min_error;
    int64_t max_error;

    // --- Overflow Index ---
    // A simple map to store new keys that are not in the trained model
    std::map<double, set<row_t>> overflow_index;
    // --- Model Logic ---
    
    //! Train the linear regression model on a set of (key, position) pairs
    void Train(const std::vector<std::pair<double, int64_t>> &data);
    
    //! Predict the position of a single key

    //! Predict the position of a single key
    int64_t PredictPosition(double key) const;

    //! Get the [start, end] search bounds for a key
    pair<int64_t, int64_t> GetSearchBounds(double key, idx_t data_size) const;

    // --- Overflow Logic ---

    //! Insert a new (key, row_id) pair into the overflow index
    void InsertIntoOverflow(double key, row_t row_id);

    //! Delete a (key, row_id) pair from the overflow index
    void DeleteFromOverflow(double key, row_t row_id);
};

} // namespace duckdb
