#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include "rmi_base_model.hpp"

namespace duckdb {

class FunctionExpressionMatcher;
struct RMIIndexScanBindData;

struct RMIIndexScanState : public IndexScanState {
    Value values[2];
    ExpressionType expressions[2];
    bool checked = false;
    std::set<row_t> row_ids;
};

struct RMIEntry {
    double key;
    row_t row_id;

    // Sort primarily by key, secondarily by row_id
    bool operator<(const RMIEntry& other) const {
        if (key != other.key) {
            return key < other.key;
        }
        return row_id < other.row_id;
    }
};

struct RMIIndexStats {
    idx_t total_rows = 0;
    idx_t model_count = 1;
    idx_t training_data_size = 0;
    idx_t overflow_size = 0;
    idx_t lower_model_fanout = 0;
};

class RMIIndex : public BoundIndex {
public:
    static constexpr const char *TYPE_NAME = "RMI";

public:
    RMIIndex(const string &name,
             IndexConstraintType index_constraint_type,
             const vector<column_t> &column_ids,
             TableIOManager &table_io_manager,
             const vector<unique_ptr<Expression>> &unbound_expressions,
             AttachedDatabase &db,
             const case_insensitive_map_t<Value> &options,
             const IndexStorageInfo &info = IndexStorageInfo(),
             idx_t estimated_cardinality = 0);

	//! Create a index instance of this type
    static unique_ptr<BoundIndex> Create(CreateIndexInput &input) {
        auto rmi = make_uniq<RMIIndex>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                          input.unbound_expressions, input.db, input.options, input.storage_info);
		return std::move(rmi);
    }

    static PhysicalOperator &CreatePlan(PlanIndexInput &input);

    // --- RMI Model ---
    static const case_insensitive_set_t MODEL_MAP;
    
    std::unique_ptr<BaseRMIModel> model;
    std::vector<std::pair<double, row_t>> training_data;
    idx_t total_rows = 0;

    // --- Required for Option A (full scan functionality) ---
	std::vector<double> owned_keys;
	std::vector<row_t> owned_rowids;

    double *base_table_keys = nullptr;
    row_t *base_table_row_ids = nullptr;
    idx_t data_size = 0;

    // Internal mutex
    duckdb::mutex rmi_lock;

public:
    // Build
    void Build(Vector &sorted_keys, Vector &sorted_row_ids, idx_t row_count);

    // Construction
    // void Construct(DataChunk &input, Vector &row_ids, idx_t thread_idx);
    // void Compact();

    std::unique_ptr<RMIIndexStats> GetStats();

    // Expression matching
    bool TryMatchLookupExpression(const std::unique_ptr<Expression> &expr,
                                  std::vector<std::reference_wrapper<Expression>> &bindings) const;

    unique_ptr<ExpressionMatcher> MakeFunctionMatcher() const;

    // Scan API
	unique_ptr<IndexScanState> TryInitializeScan(const Expression &expr, const Expression &filter_expr);
    bool Scan(IndexScanState &state, idx_t max_count, std::set<row_t> &row_ids);

    // Index API
    ErrorData Append(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
    ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;
    void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;
    void CommitDrop(IndexLock &index_lock) override;

    void Vacuum(IndexLock &lock) override;
    string VerifyAndToString(IndexLock &state, bool only_verify) override;
    void VerifyAllocations(IndexLock &state) override;
    idx_t GetInMemorySize(IndexLock &state) override;
    bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;

    string GetConstraintViolationMessage(VerifyExistenceType, idx_t, DataChunk &) override {
        return "Constraint violation in RMI index";
    }

    IndexStorageInfo SerializeToDisk(QueryContext context, const case_insensitive_map_t<Value> &options) override;
    IndexStorageInfo SerializeToWAL(const case_insensitive_map_t<Value> &options) override;

    // Pointers to the base table's sorted data
    // (These are set during the Build() phase)
    std::vector<RMIEntry> index_data;
    // idx_t data_size;

private:
    bool is_dirty = false;

    // ---- Scan helpers (from old code) ----
    bool SearchEqual(double key, idx_t max_count, std::set<row_t> &row_ids);
    bool SearchGreater(double key, bool equal, idx_t max_count, std::set<row_t> &row_ids);
    bool SearchLess(double key, bool equal, idx_t max_count, std::set<row_t> &row_ids);
    bool SearchCloseRange(double key_low, double key_high, bool left_equal, bool right_equal,
                          idx_t max_count, std::set<row_t> &row_ids);
};

} // namespace duckdb
