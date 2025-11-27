//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/rmi/rmi.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/bound_index.hpp"
#include "src/include/rmi/rmi_model.hpp"


namespace duckdb {
struct RMIIndexScanState : public IndexScanState {
    //! The Predicates to scan.
    //! A single predicate for point lookups, and two predicates for range scans.
    Value values[2];

    //! THe expressions over the scan predicates.
    ExpressionType expressions[2];
    bool checked = false;

    //! All scanned row IDs.
    set<row_t> row_ids;
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

class RMI : public BoundIndex {

public:
    // Index type name for the RMI
    static constexpr const char *TYPE_NAME = "RMI";

    int rmi_value;

public:
    RMI(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
        TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
        AttachedDatabase &db, const case_insensitive_map_t<Value> &options,
        const IndexStorageInfo &info = IndexStorageInfo(), idx_t estimated_cardinality = 0);

    //! Create a index instance of this type
    static unique_ptr<BoundIndex> Create(CreateIndexInput &input) {
        auto rmi = make_uniq<RMI>(input.name, input.constraint_type, input.column_ids, input.table_io_manager,
		                          input.unbound_expressions, input.db, input.options, input.storage_info);
		return std::move(rmi);
    }
    
    //! Plan Index Construction
    static PhysicalOperator &CreatePlan(PlanIndexInput &input);


    // CSCI543 for later
    // unique_ptr<IndexScanState> InitializeScan() const;
    // idx_t Scan(IndexScanState &state, Vector &result) const;
        // Vector &result depends on how we want to scan to be

public:
	//! Try to initialize a scan on the ART with the given expression and filter.
	unique_ptr<IndexScanState> TryInitializeScan(const Expression &expr, const Expression &filter_expr);
	//! Perform a lookup on the ART, fetching up to max_count row IDs.
	//! If all row IDs were fetched, it return true, else false.
	bool Scan(IndexScanState &state, idx_t max_count, set<row_t> &row_ids);

    //! Insert a chunk of entries into the index
	ErrorData Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) override;

    //! Delete a chunk of entries from the index. The lock obtained from InitializeLock must be held
	void Delete(IndexLock &lock, DataChunk &entries, Vector &row_identifiers) override;

    //! Deletes all data from the index. The lock obtained from InitializeLock must be held
	void CommitDrop(IndexLock &index_lock) override;

    //! Build an RMI Index from a vector of sorted keys and their row IDs.
	void Build(Vector &sorted_keys, Vector &sorted_row_ids, const idx_t row_count);

    // Append is a wrapper for Insert
    ErrorData Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) override;
    
    // We don't support merging, vacuuming, or constraints
    bool MergeIndexes(IndexLock &state, BoundIndex &other_index) override;
    void Vacuum(IndexLock &l) override;
    string GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) override;

    // Simple in-memory size calculation
    idx_t GetInMemorySize(IndexLock &state) override;

    // Debugging/verification functions (stubs)
    string VerifyAndToString(IndexLock &l, const bool only_verify) override;
    void VerifyAllocations(IndexLock &l) override;

    // Pointers to the base table's sorted data
    // (These are set during the Build() phase)
    std::vector<RMIEntry> index_data;
    idx_t data_size;

private:
	bool SearchEqual(double key, idx_t max_count, set<row_t> &row_ids);
	bool SearchGreater(double key, bool equal, idx_t max_count, set<row_t> &row_ids);
	bool SearchLess(double key, bool equal, idx_t max_count, set<row_t> &row_ids);
	bool SearchCloseRange(double key_low, double key_high, bool left_equal, bool right_equal, idx_t max_count,
	                      set<row_t> &row_ids);
    
    // --- Data Members ---
    unique_ptr<RMIModel> model; // The data-holding object

};
} // namespace duckdb