#include "rmi_module.hpp"
#include "rmi_index.hpp"

#include "rmi_base_model.hpp"
#include "rmi_linear_model.hpp"
#include "rmi_poly_model.hpp"
#include "rmi_two_layer_model.hpp"

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

#include <fstream>
#include <sstream>

namespace duckdb {

// RMI Index INFO (Lists all RMI indexes)
static void RMILog(const std::string &msg) {
    std::ofstream log("/tmp/rmi_model.log", std::ios::app);
    if (log.is_open()) {
        log << msg << std::endl;
        log.close();
    }
}

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
            if (index_entry.index_type == RMIIndex::TYPE_NAME) {
                result->entries.push_back(index_entry);
            }
        });
    }
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
        
        auto &storage = table_entry.GetStorage();
        RMIIndex *rmi_index = nullptr;

        auto &table_info = *storage.GetDataTableInfo();
        
        // Force the system to load/bind indexes for this table
        table_info.BindIndexes(context, RMIIndex::TYPE_NAME);
        
        // Scan memory to find the physical index object
        table_info.GetIndexes().Scan([&](Index &index) {
            // Skip unbound or non-RMI indexes
            if (!index.IsBound() || RMIIndex::TYPE_NAME != index.GetIndexType()) {
                return false;
            }
            auto &rmi = index.Cast<RMIIndex>();
            // Check if names match
            if (rmi.GetIndexName() == index_entry.name) {
                rmi_index = &rmi;
                return true;
            }
            return false;
        });

        // Validation: If Catalog has it, but Storage doesn't -> CRASH/ERROR
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

static optional_ptr<RMIIndex> TryGetIndex(ClientContext &context, const string &index_name) {
    auto qname = QualifiedName::Parse(index_name);

    // Look up the index name in the catalog
    Binder::BindSchemaOrCatalog(context, qname.catalog, qname.schema);
    auto &index_entry = Catalog::GetEntry(context, CatalogType::INDEX_ENTRY, qname.catalog, qname.schema, qname.name)
                            .Cast<IndexCatalogEntry>();
    
    auto &table_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, qname.catalog, index_entry.GetSchemaName(),
                                          index_entry.GetTableName())
                            .Cast<TableCatalogEntry>();

    auto &storage = table_entry.GetStorage();
    RMIIndex *rmi_index = nullptr;

    auto &table_info = *storage.GetDataTableInfo();
    table_info.BindIndexes(context, RMIIndex::TYPE_NAME);
    
    // Find the specific pointer to the RMIIndex class
    table_info.GetIndexes().Scan([&](Index &index) {
        if (!index.IsBound() || RMIIndex::TYPE_NAME != index.GetIndexType()) {
            return false;
        }
        auto &rmi = index.Cast<RMIIndex>();
        if (index_entry.name == index_name) {
            rmi_index = &rmi;
            return true;
        }
        return false;
    });

    return rmi_index;
}

// BIND
struct RMIIndexDumpBindData final : public TableFunctionData {
    string index_name;
};

static unique_ptr<FunctionData> RMIIndexDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<RMIIndexDumpBindData>();

    result->index_name = input.inputs[0].GetValue<string>();

    names.emplace_back("key");
    return_types.emplace_back(LogicalType::DOUBLE);

    names.emplace_back("row_id");
    return_types.emplace_back(LogicalType::ROW_TYPE);

    return std::move(result);
}

// INIT
struct RMIIndexDumpState final : public GlobalTableFunctionState {
    const RMIIndex &index;
    idx_t current_offset = 0;

public:
    explicit RMIIndexDumpState(const RMIIndex &index) : index(index) {
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
    
    // Access the sorted data vector from the RMIIndex class
    const auto &data = state.index.index_data;
    idx_t total_size = data.size();

    while (state.current_offset < total_size && output_count < STANDARD_VECTOR_SIZE) {
        
        // Get data from our RMIEntry struct
        const auto &entry = data[state.current_offset];

        key_data[output_count] = entry.key;
        row_id_data[output_count] = entry.row_id;

        state.current_offset++;
        output_count++;
    }

    output.SetCardinality(output_count);
}

// BIND
struct RMIIndexModelStatsBindData final : public TableFunctionData {
    string index_name;
};

static unique_ptr<FunctionData> RMIIndexModelStatsBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<RMIIndexModelStatsBindData>();

    result->index_name = input.inputs[0].GetValue<string>();

    names.emplace_back("key");
    return_types.emplace_back(LogicalType::DOUBLE);

    names.emplace_back("row_id");
    return_types.emplace_back(LogicalType::ROW_TYPE);

    names.emplace_back("predicted_position");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("min_error");
    return_types.emplace_back(LogicalType::BIGINT);

    names.emplace_back("max_error");
    return_types.emplace_back(LogicalType::BIGINT);

    return std::move(result);
}

// INIT
struct RMIIndexModelStatsState final : public GlobalTableFunctionState {
    const RMIIndex &index;
    idx_t current_offset = 0;

public:
    explicit RMIIndexModelStatsState(const RMIIndex &index) : index(index) {
    }
};

static unique_ptr<GlobalTableFunctionState> RMIIndexModelStatsInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<RMIIndexModelStatsBindData>();

    auto rmi_index = TryGetIndex(context, bind_data.index_name);
    if (!rmi_index) {
        throw BinderException("Index %s not found", bind_data.index_name);
    }

    return make_uniq<RMIIndexModelStatsState>(*rmi_index);
}

// EXECUTE
static void RMIIndexModelStatsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<RMIIndexModelStatsState>();

    auto key_data = FlatVector::GetData<double>(output.data[0]);
    auto row_id_data = FlatVector::GetData<row_t>(output.data[1]);
    auto pred_pos_data = FlatVector::GetData<int64_t>(output.data[2]);
    auto min_error_data = FlatVector::GetData<int64_t>(output.data[3]);
    auto max_error_data = FlatVector::GetData<int64_t>(output.data[4]);

    idx_t output_count = 0;

    const auto &data = state.index.index_data;
    idx_t total_size = data.size();

    while (state.current_offset < total_size && output_count < STANDARD_VECTOR_SIZE) {
        const auto &entry = data[state.current_offset];

        double key = entry.key;
        row_t row_id = entry.row_id;

        // Get predictions from the model
        int64_t predicted_pos = (int64_t)state.index.model->PredictPosition(key);
        int64_t min_err = (int64_t)state.index.model->GetMinError();
        int64_t max_err = (int64_t)state.index.model->GetMaxError();

        key_data[output_count] = key;
        row_id_data[output_count] = row_id;
        pred_pos_data[output_count] = predicted_pos;
        min_error_data[output_count] = min_err;
        max_error_data[output_count] = max_err;

        state.current_offset++;
        output_count++;
    }

    output.SetCardinality(output_count);
}

// BIND
struct RMIIndexOverflowBindData final : public TableFunctionData {
    string index_name;
};

static unique_ptr<FunctionData> RMIIndexOverflowBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<RMIIndexOverflowBindData>();

    result->index_name = input.inputs[0].GetValue<string>();

    names.emplace_back("key");
    return_types.emplace_back(LogicalType::DOUBLE);

    names.emplace_back("row_id");
    return_types.emplace_back(LogicalType::ROW_TYPE);

    names.emplace_back("source");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(result);
}

// INIT
struct RMIIndexOverflowState final : public GlobalTableFunctionState {
    const RMIIndex &index;
    
    // Iterators for overflow map
    std::unordered_map<double, std::vector<row_t>>::const_iterator overflow_iter;
    std::unordered_map<double, std::vector<row_t>>::const_iterator overflow_end;
    size_t current_row_in_bucket = 0;

public:
    explicit RMIIndexOverflowState(const RMIIndex &index) : index(index) {
        overflow_iter = index.model->GetOverflowMap().begin();
        overflow_end = index.model->GetOverflowMap().end();
    }
};

static unique_ptr<GlobalTableFunctionState> RMIIndexOverflowInit(ClientContext &context, TableFunctionInitInput &input) {
    auto &bind_data = input.bind_data->Cast<RMIIndexOverflowBindData>();

    auto rmi_index = TryGetIndex(context, bind_data.index_name);
    if (!rmi_index) {
        throw BinderException("Index %s not found", bind_data.index_name);
    }

    return make_uniq<RMIIndexOverflowState>(*rmi_index);
}

// EXECUTE
static void RMIIndexOverflowExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
    auto &state = data_p.global_state->Cast<RMIIndexOverflowState>();

    auto key_data = FlatVector::GetData<double>(output.data[0]);
    auto row_id_data = FlatVector::GetData<row_t>(output.data[1]);
    auto source_data = FlatVector::GetData<string_t>(output.data[2]);

    idx_t output_count = 0;

    // Iterate through overflow map
    while (state.overflow_iter != state.overflow_end && output_count < STANDARD_VECTOR_SIZE) {
        const auto &key = state.overflow_iter->first;
        const auto &row_ids = state.overflow_iter->second;

        // Output each row_id in this bucket
        while (state.current_row_in_bucket < row_ids.size() && output_count < STANDARD_VECTOR_SIZE) {
            key_data[output_count] = key;
            row_id_data[output_count] = row_ids[state.current_row_in_bucket];
            
            // Mark as overflow
            string source_str = "overflow";
            source_data[output_count] = StringVector::AddString(output.data[2], source_str);

            state.current_row_in_bucket++;
            output_count++;
        }

        // Move to next bucket if we finished this one
        if (state.current_row_in_bucket >= row_ids.size()) {
            state.overflow_iter++;
            state.current_row_in_bucket = 0;
        }
    }

    output.SetCardinality(output_count);
}

struct RMIIndexModelInfoBindData final : public TableFunctionData {
    string index_name;
};

static unique_ptr<FunctionData> RMIIndexModelInfoBind(
    ClientContext &context, TableFunctionBindInput &input,
    vector<LogicalType> &return_types, vector<string> &names) {

    auto result = make_uniq<RMIIndexModelInfoBindData>();
    result->index_name = input.inputs[0].GetValue<string>();

    names.emplace_back("field");
    return_types.emplace_back(LogicalType::VARCHAR);

    names.emplace_back("value");
    return_types.emplace_back(LogicalType::VARCHAR);

    return std::move(result);
}

struct RMIIndexModelInfoState final : public GlobalTableFunctionState {
    const RMIIndex &index;
    bool emitted = false;

    explicit RMIIndexModelInfoState(const RMIIndex &idx) : index(idx) {
    }
};

static unique_ptr<GlobalTableFunctionState> RMIIndexModelInfoInit(
    ClientContext &context, TableFunctionInitInput &input) {

    auto &bind = input.bind_data->Cast<RMIIndexModelInfoBindData>();

    auto rmi_index = TryGetIndex(context, bind.index_name);
    if (!rmi_index) {
        throw BinderException("Index %s not found", bind.index_name);
    }

    return make_uniq<RMIIndexModelInfoState>(*rmi_index);
}

static void EmitKV(DataChunk &chunk, idx_t row,
                   const string &field, const string &value) {

    chunk.SetCardinality(row + 1);

    auto &col0 = chunk.data[0];
    auto &col1 = chunk.data[1];

    FlatVector::GetData<string_t>(col0)[row] =
        StringVector::AddString(col0, field);

    FlatVector::GetData<string_t>(col1)[row] =
        StringVector::AddString(col1, value);
}

static void RMIIndexModelInfoExecute(
    ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {

    auto &state = data_p.global_state->Cast<RMIIndexModelInfoState>();
    if (state.emitted) {
        output.SetCardinality(0);
        return;
    }

    auto &model = *state.index.model;

    idx_t row = 0;

    // Model type
    EmitKV(output, row++, "model_type", model.GetModelTypeName());

    // General fields
    EmitKV(output, row++, "min_error", to_string(model.GetMinError()));
    EmitKV(output, row++, "max_error", to_string(model.GetMaxError()));
    EmitKV(output, row++, "overflow_key_count",
           to_string(model.GetOverflowMap().size()));

    // Now detect model kind
    if (auto *lin = dynamic_cast<RMILinearModel*>(&model)) {
        EmitKV(output, row++, "slope", to_string(lin->slope));
        EmitKV(output, row++, "intercept", to_string(lin->intercept));
    }
    else if (auto *poly = dynamic_cast<RMIPolyModel*>(&model)) {
        EmitKV(output, row++, "degree", to_string(poly->coeffs.size() - 1));
        for (idx_t i = 0; i < poly->coeffs.size(); i++) {
            EmitKV(output, row++, "coeff[" + to_string(i) + "]",
                   to_string(poly->coeffs[i]));
        }
    }
    else if (auto *two = dynamic_cast<RMITwoLayerModel*>(&model)) {
        EmitKV(output, row++, "root_slope", to_string(two->root_slope));
        EmitKV(output, row++, "root_intercept", to_string(two->root_intercept));
        EmitKV(output, row++, "segments(K)", to_string(two->K));

        for (idx_t i = 0; i < two->K; i++) {
            EmitKV(output, row++, "leaf_slope[" + to_string(i) + "]",
                   to_string(two->leaf_slopes[i]));
            EmitKV(output, row++, "leaf_intercept[" + to_string(i) + "]",
                   to_string(two->leaf_intercepts[i]));
        }
    }

    state.emitted = true;
}


void RMIModule::RegisterIndexPragmas(ExtensionLoader &loader) {

    // Register: pragma_rmi_index_info()
    TableFunction info_function("pragma_rmi_index_info", {}, RMIIndexInfoExecute, RMIIndexInfoBind,
                                RMIIndexInfoInit);
    loader.RegisterFunction(info_function);

    // Register: rmi_index_dump('index_name')
    TableFunction dump_function("rmi_index_dump", {LogicalType::VARCHAR}, RMIIndexDumpExecute, RMIIndexDumpBind,
                                RMIIndexDumpInit);
    loader.RegisterFunction(dump_function);

    // Register: rmi_index_model_stats('index_name')
    TableFunction model_stats_function("rmi_index_model_stats", {LogicalType::VARCHAR}, RMIIndexModelStatsExecute,
                                       RMIIndexModelStatsBind, RMIIndexModelStatsInit);
    loader.RegisterFunction(model_stats_function);

    // Register: rmi_index_overflow('index_name')
    TableFunction overflow_function("rmi_index_overflow", {LogicalType::VARCHAR}, RMIIndexOverflowExecute,
                                    RMIIndexOverflowBind, RMIIndexOverflowInit);
    loader.RegisterFunction(overflow_function);

    // Resgister: rmi_index_model_info('index_name')
    TableFunction model_info_fn("rmi_index_model_info", {LogicalType::VARCHAR}, RMIIndexModelInfoExecute, RMIIndexModelInfoBind, RMIIndexModelInfoInit);
    loader.RegisterFunction(model_info_fn);

}

} // namespace duckdb
