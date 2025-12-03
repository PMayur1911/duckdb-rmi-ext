#include "rmi_module.hpp"
#include "rmi_index.hpp"
#include "rmi_index_scan.hpp"

#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

BindInfo RMIIndexScanBindInfo(const optional_ptr<FunctionData> bind_data_p) {
    auto &bind_data = bind_data_p->Cast<RMIIndexScanBindData>();
    return BindInfo(bind_data.table);
}

struct RMIIndexScanGlobalState final : public GlobalTableFunctionState {
    // The DataChunk containing all read columns.
    DataChunk all_columns;
    vector<idx_t> projection_ids;

    ColumnFetchState fetch_state;
    TableScanState local_storage_state;
    vector<StorageIndex> column_ids;

    // Index scan state
    unique_ptr<IndexScanState> index_state;
    Vector row_ids = Vector(LogicalType::ROW_TYPE);
};

static unique_ptr<GlobalTableFunctionState> RMIIndexScanInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<RMIIndexScanBindData>();
    auto result = make_uniq<RMIIndexScanGlobalState>();

    // Setup the scan state for the local storage
    auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
    result->column_ids.reserve(input.column_ids.size());

    // Figure out the storage column ids
    for (auto &id : input.column_ids) {
        storage_t col_id = id;
        if (id != DConstants::INVALID_INDEX) {
            col_id = bind_data.table.GetColumn(LogicalIndex(id)).StorageOid();
        }
        result->column_ids.emplace_back(col_id);
    }

    // Initialize the storage scan state
    result->local_storage_state.Initialize(result->column_ids, context, input.filters);
    local_storage.InitializeScan(bind_data.table.GetStorage(), result->local_storage_state.local_state, input.filters);

    // We recreate the state that RMI::Scan expects (values and expressions)
    auto rmi_state = make_uniq<RMIIndexScanState>();
    
    // Copy predicates from Bind Data to Execution State
    rmi_state->values[0] = bind_data.values[0];
    rmi_state->values[1] = bind_data.values[1];
    rmi_state->expressions[0] = bind_data.expressions[0];
    rmi_state->expressions[1] = bind_data.expressions[1];
    
    result->index_state = std::move(rmi_state);

    // Early out if there is nothing to project
    if (!input.CanRemoveFilterColumns()) {
        return std::move(result);
    }

    // We need this to project out what we scan from the underlying table.
    result->projection_ids = input.projection_ids;

    auto &duck_table = bind_data.table.Cast<DuckTableEntry>();
    const auto &columns = duck_table.GetColumns();
    vector<LogicalType> scanned_types;
    for (const auto &col_idx : input.column_indexes) {
        if (col_idx.IsRowIdColumn()) {
            scanned_types.emplace_back(LogicalType::ROW_TYPE);
        } else {
            scanned_types.push_back(columns.GetColumn(col_idx.ToLogical()).Type());
        }
    }
    result->all_columns.Initialize(context, scanned_types);

    return std::move(result);
}

static void RMIIndexScanExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

    auto &bind_data = data_p.bind_data->Cast<RMIIndexScanBindData>();
    auto &state = data_p.global_state->Cast<RMIIndexScanGlobalState>();
    auto &transaction = DuckTransaction::Get(context, bind_data.table.catalog);

    // Get the specific RMI state
    auto &rmi_state = state.index_state->Cast<RMIIndexScanState>();

    // Check if we have already scanned. 
    // Since RMI::Scan (as implemented) is not resumable/paging, we do it once.
    if (rmi_state.checked) {
        output.SetCardinality(0);
        return;
    }

    // Prepare a temporary set to hold the results from RMI
    std::set<row_t> result_set;

    // Call Scan
    // We pass STANDARD_VECTOR_SIZE to limit the number of rows (though our RMI currently effectively does one pass)
    bind_data.index.Cast<RMIIndex>().Scan(rmi_state, STANDARD_VECTOR_SIZE, result_set);
    
    // Mark as checked so we don't loop infinitely
    rmi_state.checked = true;

    idx_t row_count = result_set.size();

    if (row_count == 0) {
        // Short-circuit if the index had no more rows
        output.SetCardinality(0);
        return;
    }

    // Copy the results from std::set to the DuckDB Vector
    // We get a raw pointer to the vector's data
    auto row_ids_ptr = FlatVector::GetData<row_t>(state.row_ids);
    idx_t i = 0;
    for (const auto &row_id : result_set) {
        row_ids_ptr[i++] = row_id;
    }

    // Fetch the data from the local storage given the row ids
    if (state.projection_ids.empty()) {
        bind_data.table.GetStorage().Fetch(transaction, output, state.column_ids, state.row_ids, row_count,
                                           state.fetch_state);
        return;
    }

    // Otherwise, we need to first fetch into our scan chunk, and then project out the result
    state.all_columns.Reset();
    bind_data.table.GetStorage().Fetch(transaction, state.all_columns, state.column_ids, state.row_ids, row_count,
                                       state.fetch_state);
    output.ReferenceColumns(state.all_columns, state.projection_ids);
}

static unique_ptr<BaseStatistics> RMIIndexScanStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                                         column_t column_id) {
    auto &bind_data = bind_data_p->Cast<RMIIndexScanBindData>();
    auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
    if (local_storage.Find(bind_data.table.GetStorage())) {
        return nullptr;
    }
    return bind_data.table.GetStatistics(context, column_id);
}

void RMIIndexScanDependency(LogicalDependencyList &entries, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<RMIIndexScanBindData>();
    entries.AddDependency(bind_data.table);
}

unique_ptr<NodeStatistics> RMIIndexScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
    auto &bind_data = bind_data_p->Cast<RMIIndexScanBindData>();
    auto &local_storage = LocalStorage::Get(context, bind_data.table.catalog);
    const auto &storage = bind_data.table.GetStorage();
    idx_t table_rows = storage.GetTotalRows();
    idx_t estimated_cardinality = table_rows + local_storage.AddedRows(bind_data.table.GetStorage());
    return make_uniq<NodeStatistics>(table_rows, estimated_cardinality);
}

static InsertionOrderPreservingMap<string> RMIIndexScanToString(TableFunctionToStringInput &input) {
    D_ASSERT(input.bind_data);
    InsertionOrderPreservingMap<string> result;
    auto &bind_data = input.bind_data->Cast<RMIIndexScanBindData>();
    result["Table"] = bind_data.table.name;
    result["Index"] = bind_data.index.GetIndexName();
    return result;
}

static void RMIScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                             const TableFunction &function) {
    auto &bind_data = bind_data_p->Cast<RMIIndexScanBindData>();
    serializer.WriteProperty(100, "catalog", bind_data.table.schema.catalog.GetName());
    serializer.WriteProperty(101, "schema", bind_data.table.schema.name);
    serializer.WriteProperty(102, "table", bind_data.table.name);
    serializer.WriteProperty(103, "index_name", bind_data.index.GetIndexName());

    serializer.WriteObject(104, "predicates", [&](Serializer &ser) {
        ser.WriteProperty(0, "val0", bind_data.values[0]);
        ser.WriteProperty(1, "val1", bind_data.values[1]);
        ser.WriteProperty(2, "expr0", bind_data.expressions[0]);
        ser.WriteProperty(3, "expr1", bind_data.expressions[1]);
    });
}

static unique_ptr<FunctionData> RMIScanDeserialize(Deserializer &deserializer, TableFunction &function) {
    auto &context = deserializer.Get<ClientContext &>();

    const auto catalog = deserializer.ReadProperty<string>(100, "catalog");
    const auto schema = deserializer.ReadProperty<string>(101, "schema");
    const auto table = deserializer.ReadProperty<string>(102, "table");
    auto &catalog_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, table);
    
    if (catalog_entry.type != CatalogType::TABLE_ENTRY) {
        throw SerializationException("Cant find table for %s.%s", schema, table);
    }

    const auto index_name = deserializer.ReadProperty<string>(103, "index_name");

    // --- CHANGE: Deserialize Predicates ---
    Value val0, val1;
    ExpressionType expr0, expr1;

    deserializer.ReadObject(104, "predicates", [&](Deserializer &ser) {
        val0 = ser.ReadProperty<Value>(0, "val0");
        val1 = ser.ReadProperty<Value>(1, "val1");
        expr0 = ser.ReadProperty<ExpressionType>(2, "expr0");
        expr1 = ser.ReadProperty<ExpressionType>(3, "expr1");
    });

    auto &duck_table = catalog_entry.Cast<DuckTableEntry>();
    auto &table_info = *catalog_entry.GetStorage().GetDataTableInfo();

    unique_ptr<RMIIndexScanBindData> result = nullptr;

    table_info.BindIndexes(context, RMIIndex::TYPE_NAME);
    table_info.GetIndexes().Scan([&](Index &index) {
        if (!index.IsBound() || RMIIndex::TYPE_NAME != index.GetIndexType()) {
            return false;
        }
        auto &index_entry = index.Cast<RMIIndex>();
        if (index_entry.GetIndexName() == index_name) {
            result = make_uniq<RMIIndexScanBindData>(duck_table, index_entry);
            result->values[0] = val0;
            result->values[1] = val1;
            result->expressions[0] = expr0;
            result->expressions[1] = expr1;
            return true;
        }
        return false;
    });

    if (!result) {
        throw SerializationException("Could not find index %s on table %s.%s", index_name, schema, table);
    }
    return std::move(result);
}

TableFunction RMIIndexScanFunction::GetFunction() {
    TableFunction func("rmi_index_scan", {}, RMIIndexScanExecute);
    func.init_local = nullptr;
    func.init_global = RMIIndexScanInitGlobal;
    func.statistics = RMIIndexScanStatistics;
    func.dependency = RMIIndexScanDependency;
    func.cardinality = RMIIndexScanCardinality;
    func.pushdown_complex_filter = nullptr;
    func.to_string = RMIIndexScanToString;
    func.table_scan_progress = nullptr;
    func.projection_pushdown = true;
    func.filter_pushdown = false;
    func.get_bind_info = RMIIndexScanBindInfo;
    func.serialize = RMIScanSerialize;
    func.deserialize = RMIScanDeserialize;

    return func;
}

// Register
void RMIModule::RegisterIndexScan(ExtensionLoader &loader) {
    loader.RegisterFunction(RMIIndexScanFunction::GetFunction());
}

} // namespace duckdb
