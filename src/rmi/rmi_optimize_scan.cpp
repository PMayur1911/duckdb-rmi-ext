#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <fstream>
#include <sstream>

#include "rmi_index.hpp"
#include "rmi_module.hpp"
#include "rmi_index_scan.hpp"

namespace duckdb {

// Logger
static void RMILog(const std::string &msg) {
    std::ofstream log("/tmp/rmi_optimizer.log", std::ios::app);
    if (log.is_open()) {
        log << msg << std::endl;
        log.close();
    }
}

class RMIIndexScanOptimizer : public OptimizerExtension {
public:
    RMIIndexScanOptimizer() {
        optimize_function = Optimize;
    }

    // Helper to map a single TableFilter to our Bind Data slots
    static void MapFilterToBindData(TableFilter &filter, RMIIndexScanBindData &bind_data) {
        if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
            auto &constant_filter = filter.Cast<ConstantFilter>();
            
            idx_t slot = bind_data.values[0].IsNull() ? 0 : 1;
            
            if (slot < 2) {
                bind_data.expressions[slot] = constant_filter.comparison_type;
                bind_data.values[slot] = constant_filter.constant;
                
                RMILog("MapFilterToBindData: Mapped slot " + std::to_string(slot) + 
                       " with Value: " + constant_filter.constant.ToString());
            } else {
                RMILog("MapFilterToBindData: Skipped filter (No slots left).");
            }
        } else {
             RMILog("MapFilterToBindData: Skipped filter (Not CONSTANT_COMPARISON). Type: " + 
                    std::to_string((int)filter.filter_type));
        }
    }

    static bool TryOptimize(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        auto &op = *plan;

        if (op.type != LogicalOperatorType::LOGICAL_GET) {
            return false;
        }

        RMILog("TryOptimize: Found LOGICAL_GET. Checking details...");
        auto &get = op.Cast<LogicalGet>();

        // Check if this is a standard table scan
        if (get.function.name != "seq_scan") {
            RMILog("TryOptimize: Not seq_scan, function is: " + get.function.name);
            return false;
        }

        // Check if the table is a DuckDB table
        auto &table = *get.GetTable();
        if (!table.IsDuckTable()) {
            RMILog("TryOptimize: Not a DuckTable.");
            return false;
        }

        auto &duck_table = table.Cast<DuckTableEntry>();
        auto &table_info = *table.GetStorage().GetDataTableInfo();

        // Check if we have filters pushed down into this scan
        if (get.table_filters.filters.empty()) {
            RMILog("TryOptimize: No table_filters pushed down to scan. RMI requires filters.");
            return false; 
        }

        unique_ptr<RMIIndexScanBindData> bind_data = nullptr;

        RMILog("TryOptimize: Scanning indexes on table...");
        
        // Look for an RMI Index on the table
        table_info.BindIndexes(context, RMIIndex::TYPE_NAME);
        
        table_info.GetIndexes().Scan([&](Index &index) {
            if (!index.IsBound()) {
                return false;
            }
            if (RMIIndex::TYPE_NAME != index.GetIndexType()) {
                return false;
            }

            RMILog("TryOptimize: Found an RMI Index!");

            auto &rmi_index = index.Cast<RMIIndex>();
            auto column_ids = rmi_index.GetColumnIds();
            
            if (column_ids.size() != 1) {
                RMILog("TryOptimize: RMI Index on multiple columns not supported yet.");
                return false;
            }
            
            idx_t indexed_col_idx = column_ids[0];
            RMILog("TryOptimize: RMI Index is on column ID: " + std::to_string(indexed_col_idx));

            // Check if the pushed-down filters apply to our indexed column
            auto entry = get.table_filters.filters.find(indexed_col_idx);
            if (entry == get.table_filters.filters.end()) {
                RMILog("TryOptimize: Filters exist, but NOT on the indexed column.");
                return false; 
            }

            RMILog("TryOptimize: Found matching filters for indexed column. creating bind_data.");

            // Found a match! Create bind data
            bind_data = make_uniq<RMIIndexScanBindData>(duck_table, rmi_index);
            
            auto &filter = *entry->second;

            // Extract filter logic into bind_data
            if (filter.filter_type == TableFilterType::CONJUNCTION_AND) {
                RMILog("TryOptimize: Filter is CONJUNCTION_AND (Range).");
                auto &and_filter = filter.Cast<ConjunctionAndFilter>();
                for (auto &child_filter : and_filter.child_filters) {
                    MapFilterToBindData(*child_filter, *bind_data);
                }
            } else {
                RMILog("TryOptimize: Filter is Single Predicate.");
                MapFilterToBindData(filter, *bind_data);
            }
            
            // Validate to ensure we actually extracted something
            if (bind_data->values[0].IsNull()) {
                RMILog("TryOptimize: Failed to extract valid constants from filter.");
                bind_data = nullptr; 
                return false;
            }

            return true; // Stop scanning indexes
        });

        if (!bind_data) {
            RMILog("TryOptimize: No valid RMI index match found after scanning.");
            return false;
        }

        // Sort the bind data
        if (!bind_data->values[1].IsNull()) {
             if (bind_data->values[0] > bind_data->values[1]) {
                 RMILog("TryOptimize: Swapping values to ensure Slot 0 is Lower Bound.");
                 std::swap(bind_data->values[0], bind_data->values[1]);
                 std::swap(bind_data->expressions[0], bind_data->expressions[1]);
             }
        }

        // Replace the Scan Function
        get.function = RMIIndexScanFunction::GetFunction(); 
        get.bind_data = std::move(bind_data);

        return true;
    }

    static bool OptimizeChildren(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
        auto ok = TryOptimize(context, plan);
        for (auto &child : plan->children) {
            ok |= OptimizeChildren(context, child);
        }
        return ok;
    }

    static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
        OptimizeChildren(input.context, plan);
    }
};

void RMIModule::RegisterScanOptimizer(DatabaseInstance &db) {
    db.config.optimizer_extensions.push_back(RMIIndexScanOptimizer());
}

} // namespace duckdb