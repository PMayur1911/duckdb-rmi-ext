#pragma once
#include "duckdb.hpp"
#include "duckdb/storage/index.hpp"
#include <vector>

using namespace duckdb;
using std::vector;

struct KeyRowPair {
    double key;
    row_t rowid;
};

enum class LearnedIndexModelKind : uint8_t {
    LINEAR = 0,
    POLY = 1,
    PIECEWISE_LINEAR = 2
};

class LearnedRMIIndexBase : public Index {
public:
    vector<double> sorted_keys;
    vector<row_t> sorted_rowids;
    idx_t window_radius = 32;

    LearnedRMIIndexBase(const IndexStorageInfo &info);

protected:
    void ExtractKeys(const DataChunk &entries, vector<double> &keys_out) const;
    void BuildSortedIndex(vector<KeyRowPair> &pairs);
};
