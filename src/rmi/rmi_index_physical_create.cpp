#include "rmi_index_physical_create.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"

#include "duckdb/execution/operator/filter/physical_filter.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/table_io_manager.hpp"

#include "rmi_index.hpp"

namespace duckdb {

//==============================================================================
// Helper: Extract numeric value from UnifiedVectorFormat and convert to double
//==============================================================================
static double ExtractDoubleValue(const UnifiedVectorFormat &fmt, idx_t sel_idx, PhysicalType phys_type) {
    if (!fmt.validity.RowIsValid(sel_idx)) {
        return 0.0;  // Null or invalid; caller checks validity separately
    }
    
    auto data_ptr = (uint8_t *)fmt.data;
    switch (phys_type) {
        case PhysicalType::INT8: {
            int8_t val = ((int8_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::INT16: {
            int16_t val = ((int16_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::INT32: {
            int32_t val = ((int32_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::INT64: {
            int64_t val = ((int64_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::UINT8: {
            uint8_t val = ((uint8_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::UINT16: {
            uint16_t val = ((uint16_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::UINT32: {
            uint32_t val = ((uint32_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::UINT64: {
            uint64_t val = ((uint64_t *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::FLOAT: {
            float val = ((float *)fmt.data)[sel_idx];
            return (double)val;
        }
        case PhysicalType::DOUBLE: {
            double val = ((double *)fmt.data)[sel_idx];
            return val;
        }
        default:
            throw InvalidTypeException(LogicalType::DOUBLE, "Unsupported type in RMI index");
    }
}

//================================================================================================
// PhysicalCreateRMIIndex Constructor
//================================================================================================

PhysicalCreateRMIIndex::PhysicalCreateRMIIndex(
    PhysicalPlan &plan,
    const vector<LogicalType> &types_p,
    TableCatalogEntry &table_p,
    const vector<column_t> &column_ids,
    unique_ptr<CreateIndexInfo> info_p,
    vector<unique_ptr<Expression>> unbound_exprs_p,
    idx_t estimated_cardinality)
    : PhysicalOperator(plan, PhysicalOperatorType::EXTENSION, types_p, estimated_cardinality),
      table(table_p.Cast<DuckTableEntry>()),
      info(std::move(info_p)),
      unbound_expressions(std::move(unbound_exprs_p)) {

    //! Convert logical → physical column ids
    for (auto &cid : column_ids) {
        auto phys = table.GetColumns().LogicalToPhysical(LogicalIndex(cid)).index;
        storage_ids.push_back(phys);
    }
}

//================================================================================================
// Global Sink State
//================================================================================================

class CreateRMIIndexGlobalState final : public GlobalSinkState {
public:
    explicit CreateRMIIndexGlobalState(const PhysicalCreateRMIIndex &op)
        : op(op) {
    }

    const PhysicalCreateRMIIndex &op;

    //! Key-RowID data from all threads
    unique_ptr<ColumnDataCollection> collection;

    //! Global index instance (unregistered)
    unique_ptr<RMIIndex> global_index;

    //! Client context
    shared_ptr<ClientContext> client_ctx;

    //! Parallel scan state
    ColumnDataParallelScanState scan_state;

    //! For merge operations
    mutex glock;

    //! Progress counters
    atomic<idx_t> rows_loaded = {0};
};

unique_ptr<GlobalSinkState> PhysicalCreateRMIIndex::GetGlobalSinkState(ClientContext &context) const {
    auto gstate = make_uniq<CreateRMIIndexGlobalState>(*this);

    //! Collection schema: [key, rowid]
    vector<LogicalType> types = {
        unbound_expressions[0]->return_type,
        LogicalType::ROW_TYPE
    };

    gstate->collection = make_uniq<ColumnDataCollection>(
        BufferManager::GetBufferManager(context),
        types
    );

    gstate->client_ctx = context.shared_from_this();

    //! Create the RMI index object (unbuilt)
    auto &storage = table.GetStorage();
    auto &iom = TableIOManager::Get(storage);
    auto &db = storage.db;

    gstate->global_index = make_uniq<RMIIndex>(
        info->index_name,
        info->constraint_type,
        storage_ids,
        iom,
        unbound_expressions,
        db,
        info->options,
        IndexStorageInfo(),
        estimated_cardinality
    );

    return std::move(gstate);
}

//================================================================================================
// Local Sink State
//================================================================================================

class CreateRMIIndexLocalState final : public LocalSinkState {
public:
    unique_ptr<ColumnDataCollection> collection;
    ColumnDataAppendState append_state;
};

unique_ptr<LocalSinkState> PhysicalCreateRMIIndex::GetLocalSinkState(ExecutionContext &context) const {
    auto state = make_uniq<CreateRMIIndexLocalState>();

    vector<LogicalType> types = {
        unbound_expressions[0]->return_type,
        LogicalType::ROW_TYPE
    };

    state->collection = make_uniq<ColumnDataCollection>(
        BufferManager::GetBufferManager(context.client),
        types
    );

    state->collection->InitializeAppend(state->append_state);
    return std::move(state);
}

//================================================================================================
// Sink
//================================================================================================

SinkResultType PhysicalCreateRMIIndex::Sink(ExecutionContext &context,
                                            DataChunk &chunk,
                                            OperatorSinkInput &input) const {
    auto &lstate = input.local_state.Cast<CreateRMIIndexLocalState>();
    auto &gstate = input.global_state.Cast<CreateRMIIndexGlobalState>();

    lstate.collection->Append(lstate.append_state, chunk);
    gstate.rows_loaded += chunk.size();
    return SinkResultType::NEED_MORE_INPUT;
}

//================================================================================================
// Combine
//================================================================================================

SinkCombineResultType PhysicalCreateRMIIndex::Combine(
    ExecutionContext &context,
    OperatorSinkCombineInput &input) const {

    auto &gstate = input.global_state.Cast<CreateRMIIndexGlobalState>();
    auto &lstate = input.local_state.Cast<CreateRMIIndexLocalState>();

    if (lstate.collection->Count() == 0) {
        return SinkCombineResultType::FINISHED;
    }

    lock_guard<mutex> lock(gstate.glock);

    if (!gstate.collection) {
        gstate.collection = std::move(lstate.collection);
    } else {
        gstate.collection->Combine(*lstate.collection);
    }

    return SinkCombineResultType::FINISHED;
}

//================================================================================================
// Finalize (build index + register in catalog)
//================================================================================================

SinkFinalizeType PhysicalCreateRMIIndex::Finalize(
    Pipeline &pipeline,
    Event &event,
    ClientContext &context,
    OperatorSinkFinalizeInput &input) const {

    auto &gstate = input.global_state.Cast<CreateRMIIndexGlobalState>();

    //! Prepare scanning
    gstate.collection->InitializeScan(gstate.scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);

    //! Local scan chunk
    DataChunk scan_chunk;
    gstate.collection->InitializeScanChunk(scan_chunk);

    vector<pair<double, row_t>> all_data;
    all_data.reserve(gstate.collection->Count());

    //! Scan all rows (single-threaded; RMI does not need vector parallelism)
    ColumnDataLocalScanState local;
    while (gstate.collection->Scan(gstate.scan_state, local, scan_chunk)) {
        UnifiedVectorFormat key_v, rowid_v;
        scan_chunk.data[0].ToUnifiedFormat(scan_chunk.size(), key_v);
        scan_chunk.data[1].ToUnifiedFormat(scan_chunk.size(), rowid_v);

        auto rid_ptr = UnifiedVectorFormat::GetData<row_t>(rowid_v);

        for (idx_t i = 0; i < scan_chunk.size(); i++) {
            idx_t key_idx = key_v.sel->get_index(i);
            idx_t rid_idx = rowid_v.sel->get_index(i);

            if (!key_v.validity.RowIsValid(key_idx)) continue;
            if (!rowid_v.validity.RowIsValid(rid_idx)) continue;

            // Extract the numeric value and convert to double
            double key = ExtractDoubleValue(key_v, key_idx, scan_chunk.data[0].GetType().InternalType());
            all_data.emplace_back(key, rid_ptr[rid_idx]);
        }
    }

    //! Sort + train
    std::sort(all_data.begin(), all_data.end(),
              [](auto &a, auto &b) { return a.first < b.first; });

    gstate.global_index->training_data = all_data;
    gstate.global_index->total_rows = all_data.size();

    //! Build underlying model
    {
        Vector dummy_keys(LogicalType::DOUBLE);
        Vector dummy_rowids(LogicalType::ROW_TYPE);

        idx_t n = all_data.size();
        dummy_keys.Initialize(n);
        dummy_rowids.Initialize(n);

        auto kptr = FlatVector::GetData<double>(dummy_keys);
        auto rptr = FlatVector::GetData<row_t>(dummy_rowids);

        for (idx_t i = 0; i < n; i++) {
            kptr[i] = all_data[i].first;
            rptr[i] = all_data[i].second;
        }

        gstate.global_index->Build(dummy_keys, dummy_rowids, n);
    }

    //! Register in catalog
    auto &schema = table.schema;
    info->column_ids = storage_ids;

    auto entry = schema.CreateIndex(
                     schema.GetCatalogTransaction(context),
                     *info,
                     table)
                     .get();

    D_ASSERT(entry);
    auto &duck_index = entry->Cast<DuckIndexEntry>();
    duck_index.initial_index_size =
        gstate.global_index->Cast<BoundIndex>().GetInMemorySize();

    //! Attach to table storage
    table.GetStorage().AddIndex(std::move(gstate.global_index));

    return SinkFinalizeType::READY;
}

//================================================================================================
// Progress
//================================================================================================

ProgressData PhysicalCreateRMIIndex::GetSinkProgress(
        ClientContext &context,
        GlobalSinkState &gstate_p,
        ProgressData source_progress) const {

    auto &gstate = gstate_p.Cast<CreateRMIIndexGlobalState>();
    ProgressData p;

    p.done = gstate.rows_loaded;
    p.total = estimated_cardinality;

    return p;
}

} // namespace duckdb
