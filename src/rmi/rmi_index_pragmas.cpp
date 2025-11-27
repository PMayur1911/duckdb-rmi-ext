#include "src/include/rmi/rmi_module.hpp"
#include "src/include/rmi/rmi_index.hpp"

#include "duckdb/catalog/catalog_entry/duck_index_entry.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/local_storage.hpp"

namespace duckdb {

//-------------------------------------------------------------------------
// 1. RMI Index INFO (Lists all RMI indexes)
//-------------------------------------------------------------------------

// BIND
static unique_ptr<FunctionData> RMIIndexInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("catalog_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("schema_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("index_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("table_name");
    return_types.emplace_back(LogicalType::VARCHAR);

    return nullptr;
}

// INIT GLOBAL
struct RMIIndexInfoState final : public GlobalTableFunctionState {
    idx_t offset = 0;
    vector<reference<IndexCatalogEntry>> entries;
};

static unique_ptr<GlobalTableFunctionState> RMIIndexInfoInit(ClientContext &context, TableFunctionInitInput &input) {
    auto result = make_uniq<RMIIndexInfoState>();

    // Scan all schemas to find indexes of type "RMI"
    auto schemas = Catalog::GetAllSchemas(context);
    for (auto &schema : schemas) {
        schema.get().Scan(context, CatalogType::INDEX_ENTRY, [&](CatalogEntry &entry) {
            auto &index_entry = entry.Cast<IndexCatalogEntry>();
            if (index_entry.index_type == RMI::TYPE_NAME) {
                result->entries.push_back(index_entry);
            }
        });
    };
    return std::move(result);
}

// EXECUTE
static void RMIIndexInfoExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &data = data_p.global_state->Cast<RMIIndexInfoState>();
    if (data.offset >= data.entries.size()) {
        return;
    }

    idx_t row = 0;
    while (data.offset < data.entries.size() && row < STANDARD_VECTOR_SIZE) {
        auto &index_entry = data.entries[data.offset++].get();
        auto &table_entry = index_entry.schema.catalog.GetEntry<TableCatalogEntry>(context, index_entry.GetSchemaName(),
                                                                                   index_entry.GetTableName());
        
        // --- ADDED: Physical Existence Check ---
        auto &storage = table_entry.GetStorage();
        RMI *rmi_index = nullptr;

        auto &table_info = *storage.GetDataTableInfo();
        
        // 1. Force the system to load/bind indexes for this table
        table_info.BindIndexes(context, RMI::TYPE_NAME);
        
        // 2. Scan memory to find the physical index object
        table_info.GetIndexes().Scan([&](Index &index) {
            // Skip unbound or non-RMI indexes
            if (!index.IsBound() || RMI::TYPE_NAME != index.GetIndexType()) {
                return false;
            }
            auto &rmi = index.Cast<RMI>();
            // Check if names match
            if (rmi.GetIndexName() == index_entry.name) {
                rmi_index = &rmi;
                return true;
            }
            return false;
        });

        // 3. Validation: If Catalog has it, but Storage doesn't -> CRASH/ERROR
        if (!rmi_index) {
            throw BinderException("Index %s present in catalog but not found in physical storage", index_entry.name);
        }
        // ---------------------------------------

        // Output metadata
        idx_t col = 0;
        output.data[col++].SetValue(row, Value(index_entry.catalog.GetName()));
        output.data[col++].SetValue(row, Value(index_entry.schema.name));
        output.data[col++].SetValue(row, Value(index_entry.name));
        output.data[col++].SetValue(row, Value(table_entry.name));

        row++;
    }
    output.SetCardinality(row);
}

//-------------------------------------------------------------------------
// Helper: Get RMI Index Instance
//-------------------------------------------------------------------------
static optional_ptr<RMI> TryGetIndex(ClientContext &context, const string &index_name) {
    auto qname = QualifiedName::Parse(index_name);

    // Look up the index name in the catalog
    Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
    auto &index_entry = Catalog::GetEntry(context, CatalogType::INDEX_ENTRY, qname.catalog, qname.schema, qname.name)
                            .Cast<IndexCatalogEntry>();
    
    auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qname.catalog, index_entry.GetSchemaName(),
                                          index_entry.GetTableName())
                            .Cast<TableCatalogEntry>();

    auto &storage = table_entry.GetStorage();
    RMI *rmi_index = nullptr;

    auto &table_info = *storage.GetDataTableInfo();
    table_info.BindIndexes(context, RMI::TYPE_NAME);
    
    // Find the specific pointer to the RMI class
    table_info.GetIndexes().Scan([&](Index &index) {
        if (!index.IsBound() || RMI::TYPE_NAME != index.GetIndexType()) {
            return false;
        }
        auto &rmi = index.Cast<RMI>();
        if (index_entry.name == index_name) {
            rmi_index = &rmi;
            return true;
        }
        return false;
    });

    return rmi_index;
}

//-------------------------------------------------------------------------
// 2. RMI Index DUMP (Dumps the internal keys/row_ids)
//-------------------------------------------------------------------------

// BIND
struct RMIIndexDumpBindData final : public TableFunctionData {
    string index_name;
};

static unique_ptr<FunctionData> RMIIndexDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<RMIIndexDumpBindData>();

    result->index_name = input.inputs[0].GetValue<string>();

    // Return types: We return the Key and the RowID
    names.emplace_back("key");
    return_types.emplace_back(LogicalType::DOUBLE);

    names.emplace_back("row_id");
    return_types.emplace_back(LogicalType::ROW_TYPE);

    return std::move(result);
}

// INIT
struct RMIIndexDumpState final : public GlobalTableFunctionState {
    const RMI &index;
    idx_t current_offset = 0;

public:
    explicit RMIIndexDumpState(const RMI &index) : index(index) {
    }
};

static unique_ptr<GlobalTableFunctionState> RMIIndexDumpInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<RMIIndexDumpBindData>();

    auto rmi_index = TryGetIndex(context, bind_data.index_name);
    if (!rmi_index) {
        throw BinderException("Index %s not found", bind_data.index_name);
    }

    return make_uniq<RMIIndexDumpState>(*rmi_index);
}

// EXECUTE
static void RMIIndexDumpExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<RMIIndexDumpState>();

    // Pointers to the output columns
    auto key_data = FlatVector::GetData<double>(output.data[0]);
    auto row_id_data = FlatVector::GetData<row_t>(output.data[1]);

    idx_t output_count = 0;
    
    // Access the sorted data vector from the RMI class
    const auto &data = state.index.index_data;
    idx_t total_size = data.size();

    while (state.current_offset < total_size && output_count < STANDARD_VECTOR_SIZE) {
        
        // Get data from our struct
        const auto &entry = data[state.current_offset];

        key_data[output_count] = entry.key;
        row_id_data[output_count] = entry.row_id;

        state.current_offset++;
        output_count++;
    }

    output.SetCardinality(output_count);
}

//-------------------------------------------------------------------------
// Register
//-------------------------------------------------------------------------
void RMIModule::RegisterIndexPragmas(ExtensionLoader &loader) {

    // Register: pragma_rmi_index_info()
    TableFunction info_function("pragma_rmi_index_info", {}, RMIIndexInfoExecute, RMIIndexInfoBind,
                                RMIIndexInfoInit);
    loader.RegisterFunction(info_function);

    // Register: rmi_index_dump('index_name')
    TableFunction dump_function("rmi_index_dump", {LogicalType::VARCHAR}, RMIIndexDumpExecute, RMIIndexDumpBind,
                                RMIIndexDumpInit);
    loader.RegisterFunction(dump_function);
}

} // namespace duckdb