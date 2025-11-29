#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/progress_data.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

class DuckTableEntry;

class PhysicalCreateRMIIndex : public PhysicalOperator {
public:
    static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
    PhysicalCreateRMIIndex(PhysicalPlan &physical_plan,
                           const vector<LogicalType> &types_p,
                           TableCatalogEntry &table,
                           const vector<column_t> &column_ids,
                           unique_ptr<CreateIndexInfo> info,
                           vector<unique_ptr<Expression>> unbound_expressions,
                           idx_t estimated_cardinality);

    // --- Index metadata ---
    //! The table for which we are creating the index
    DuckTableEntry &table;

    //! Column IDs for index storage
    vector<column_t> storage_ids;

    //! Copy of CreateIndexInfo (ownership moved here)
    unique_ptr<CreateIndexInfo> info;

    //! Unbound expressions (for optimizer)
    vector<unique_ptr<Expression>> unbound_expressions;

public:
    // --------- Source interface (NOOP: index creation is a sink operator) ---------
    bool IsSource() const override { return true; }

    SourceResultType GetData(ExecutionContext &context,
                             DataChunk &chunk,
                             OperatorSourceInput &input) const override {
        return SourceResultType::FINISHED;
    }

    string GetName() const override {
        return "RMI_INDEX_SCAN";
    }

public:
    // --------- Sink interface (actual index construction happens here) ---------
    bool IsSink() const override { return true; }

    bool ParallelSink() const override { return true; }

    unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
    unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

    SinkResultType Sink(ExecutionContext &context,
                        DataChunk &chunk,
                        OperatorSinkInput &input) const override;

    SinkCombineResultType Combine(ExecutionContext &context,
                                  OperatorSinkCombineInput &input) const override;

    SinkFinalizeType Finalize(Pipeline &pipeline,
                              Event &event,
                              ClientContext &context,
                              OperatorSinkFinalizeInput &input) const override;

    // --------- Progress ---------
    ProgressData GetSinkProgress(ClientContext &context,
                                 GlobalSinkState &gstate,
                                 ProgressData source_progress) const override;
};

} // namespace duckdb
