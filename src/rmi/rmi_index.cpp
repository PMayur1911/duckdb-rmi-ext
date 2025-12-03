#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/exception.hpp"

#include "rmi_index.hpp"
#include "rmi_linear_model.hpp"
#include "rmi_poly_model.hpp"
#include "rmi_two_layer_model.hpp"
#include "rmi_module.hpp"

#include <fstream>
#include <sstream>
namespace duckdb {

static void RMILog(const std::string &msg) {
    std::ofstream log("/tmp/rmi_indexxx.log", std::ios::app);
    if (log.is_open()) {
        log << msg << std::endl;
        log.close();
    }
}
    
// Helper: Extract numeric value from UnifiedVectorFormat and convert to double
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

RMIIndex::RMIIndex(const string &name,
                   IndexConstraintType constraint_type,
                   const vector<column_t> &column_ids,
                   TableIOManager &iom,
                   const vector<unique_ptr<Expression>> &unbound_expressions,
                   AttachedDatabase &db,
                   const case_insensitive_map_t<Value> &options,
                   const IndexStorageInfo &info,
                   idx_t estimated_cardinality)
    : BoundIndex(name, RMIIndex::TYPE_NAME, constraint_type, column_ids, iom, unbound_expressions, db) {

    // Validate key types
    for (idx_t i = 0; i < types.size(); i++) {
        switch (types[i]) {
            case PhysicalType::DOUBLE:
            case PhysicalType::FLOAT:
            case PhysicalType::INT8:
            case PhysicalType::INT16:
            case PhysicalType::INT32:
            case PhysicalType::INT64:
            case PhysicalType::UINT8:
            case PhysicalType::UINT16:
            case PhysicalType::UINT32:
            case PhysicalType::UINT64:
                break;
            default:
                throw InvalidTypeException(logical_types[i], "RMI index only supports numeric columns");
        }
    }

    if (constraint_type != IndexConstraintType::NONE) {
        throw NotImplementedException("RMI index does not support UNIQUE/PRIMARY KEY constraints");
    }

    // Choose model implementation from options (default: linear)
    string model_name = "linear";
    auto it = options.find("model");
    if (it != options.end()) {
        model_name = StringUtil::Lower(it->second.ToString());
    }

    if (model_name == "linear") {
        model = make_uniq<RMILinearModel>();
    } else if (model_name == "poly") {
        model = make_uniq<RMIPolyModel>();
    } else if (model_name == "two_layer" || model_name == "two-layer" || model_name == "two layer") {
        model = make_uniq<RMITwoLayerModel>();
    } else {
        throw InvalidInputException("Unsupported RMI model '%s'. Supported models: linear, poly, two_layer", model_name.c_str());
    }

    total_rows = 0;
}

void RMIModule::RegisterIndex(DatabaseInstance &db) {
    IndexType type;

    type.name = RMIIndex::TYPE_NAME;
    type.create_instance = RMIIndex::Create;
    type.create_plan = RMIIndex::CreatePlan;

    db.config.GetIndexTypes().RegisterIndexType(type);
}

const case_insensitive_set_t RMIIndex::MODEL_MAP = { "linear", "poly", "two_layer" };

// PhysicalOperator &RMIIndex::CreatePlan(PlanIndexInput &input) {
// 	throw NotImplementedException("RMIIndex::CreatePlan will be implemented in rmi_index_plan.cpp");
// }

//==============================================================================
// Construct (initial build / training)
//==============================================================================

// void RMIIndex::Construct(DataChunk &input, Vector &row_ids, idx_t thread_idx) {
//     lock_guard<mutex> guard(rmi_lock);

//     idx_t n = input.size();
//     training_data.reserve(training_data.size() + n);

//     UnifiedVectorFormat key_data;
//     input.data[0].ToUnifiedFormat(n, key_data);

//     auto rowid_ptr = (row_t *)row_ids.GetData();

//     for (idx_t i = 0; i < n; i++) {
//         idx_t sel = key_data.sel->get_index(i);
//         if (!key_data.validity.RowIsValid(sel))
//             continue;

//         // Extract the numeric value and convert to double
//         double key = ExtractDoubleValue(key_data, sel, types[0]);
//         row_t rid = rowid_ptr[i];

//         training_data.emplace_back(key, rid);
//     }
//     total_rows += n;
// }


// Compact() – retrain the model + rebuild error bounds
// Uncommet if needed later

// void RMIIndex::Compact() {
//     lock_guard<mutex> guard(rmi_lock);

//     if (training_data.empty())
//         return;

//     // Sort training data by key
//     std::sort(training_data.begin(), training_data.end(),
//               [](auto &a, auto &b) { return a.first < b.first; });

//     total_rows = training_data.size();

//     model->Train(training_data);
//     is_dirty = true;
// }

std::unique_ptr<RMIIndexStats> RMIIndex::GetStats() {
    auto stats = std::make_unique<RMIIndexStats>();

    stats->total_rows = total_rows;
    stats->model_count = 1;
    stats->training_data_size = training_data.size();
    stats->overflow_size = model->GetOverflowMap().size();
    stats->lower_model_fanout = 0;

    return stats;
}

// Expression Matching (optional)
bool RMIIndex::TryMatchLookupExpression(
    const std::unique_ptr<Expression> &expr,
    std::vector<std::reference_wrapper<Expression>> &bindings) const {
    return false; // not implemented yet
}

unique_ptr<ExpressionMatcher> RMIIndex::MakeFunctionMatcher() const {
    return nullptr;
}

// Insert / Delete (overflow only)
ErrorData RMIIndex::Insert(IndexLock &, DataChunk &data, Vector &row_ids) {
    lock_guard<mutex> guard(rmi_lock);

    DataChunk expr;
    expr.Initialize(Allocator::DefaultAllocator(), logical_types);
    ExecuteExpressions(data, expr);

    UnifiedVectorFormat key_data;
    expr.data[0].ToUnifiedFormat(expr.size(), key_data);

    auto rowid_ptr = (row_t *)row_ids.GetData();

    for (idx_t i = 0; i < expr.size(); i++) {
        idx_t sel = key_data.sel->get_index(i);
        if (!key_data.validity.RowIsValid(sel))
            continue;

        // Extract the numeric value and convert to double
        double key = ExtractDoubleValue(key_data, sel, types[0]);
        row_t rid = rowid_ptr[i];
        model->InsertIntoOverflow(key, rid);
    }

    return ErrorData();
}

ErrorData RMIIndex::Append(IndexLock &l, DataChunk &entries, Vector &row_ids) {
    return Insert(l, entries, row_ids);
}

void RMIIndex::Delete(IndexLock &, DataChunk &data, Vector &row_ids) {
    lock_guard<mutex> guard(rmi_lock);

    DataChunk expr;
    expr.Initialize(Allocator::DefaultAllocator(), logical_types);
    ExecuteExpressions(data, expr);

    UnifiedVectorFormat key_data;
    expr.data[0].ToUnifiedFormat(expr.size(), key_data);

    auto rowid_ptr = (row_t *)row_ids.GetData();

    for (idx_t i = 0; i < expr.size(); i++) {
        idx_t sel = key_data.sel->get_index(i);
        if (!key_data.validity.RowIsValid(sel))
            continue;

        // Extract the numeric value and convert to double
        double key = ExtractDoubleValue(key_data, sel, types[0]);
        row_t rid = rowid_ptr[i];
        model->DeleteFromOverflow(key, rid);
    }
}

void RMIIndex::CommitDrop(IndexLock &) {
    lock_guard<mutex> guard(rmi_lock);
    model.reset();
}

void RMIIndex::Build(const std::vector<std::pair<double, row_t>> &sorted_data) {
    // 1. Prepare the Sorted Struct Array
    index_data.clear();
    index_data.reserve(sorted_data.size());
    total_rows = sorted_data.size(); // Track size of the static part

    RMILog("Building RMI Index with " + std::to_string(total_rows) + " entries.\n");

    // Copy data into our struct vector (input is already sorted by key)
    for (auto &kv : sorted_data) {
        RMIEntry entry;
        entry.key = kv.first;
        entry.row_id = kv.second;
        index_data.push_back(entry);
    }

    int i = 0;
    RMILog("Printing the index_data before sort:\n");
    for (auto it = index_data.begin(); it != index_data.end(); it++) {
        RMILog("\t{ " + std::to_string(it->key) + ", " + std::to_string(it->row_id) + "}");
        i++;
        if (i > 100) {
            break;
        }
    }
    RMILog("End of index_data before sort\n");


    // Ensure strict sorting (vital for binary search or range scans)
    std::sort(index_data.begin(), index_data.end());

    // 2. Train the Model on the Sorted Array
    std::vector<std::pair<double, idx_t>> training_data;
    training_data.reserve(index_data.size());

    for (idx_t i = 0; i < index_data.size(); ++i) {
        // Training X = key, Y = actual position in the vector
        training_data.emplace_back(index_data[i].key, (idx_t)i);
    }

    i = 0;
    RMILog("Printing the Training_data:\n");
    for (auto it = training_data.begin(); it != training_data.end(); it++) {
        RMILog("\t{ " + std::to_string(it->first) + ", " + std::to_string(it->second) + "}");
        i++;
        if (i > 100) {
            break;
        }
    }
    RMILog("End of Training_data\n");

    model->Train(training_data);

    // RMILog("Printing the index_data after sort:\n");
    // for (auto it = index_data.begin(); it != index_data.end(); it++) {
    //     RMILog("{ " + std::to_string(it->key) + ", " + std::to_string(it->row_id) + "}\n");
    // }
    // RMILog("End of index_data after sort\n");


}

void RMIIndex::Vacuum(IndexLock &) {}
idx_t RMIIndex::GetInMemorySize(IndexLock &) { return 0; }
string RMIIndex::VerifyAndToString(IndexLock &, bool) { return "RMIIndex"; }
void RMIIndex::VerifyAllocations(IndexLock &) {}
bool RMIIndex::MergeIndexes(IndexLock &, BoundIndex &) { return false; }

static unique_ptr<IndexScanState> InitializeScanSinglePredicate(const Value &value,
                                                                const ExpressionType expression_type) {
	auto result = make_uniq<RMIIndexScanState>();
	result->values[0] = value;
	result->expressions[0] = expression_type;
	return std::move(result);
}

static unique_ptr<IndexScanState> InitializeScanTwoPredicates(const Value &low_value,
                                                              const ExpressionType low_expression_type,
                                                              const Value &high_value,
                                                              const ExpressionType high_expression_type) {
	auto result = make_uniq<RMIIndexScanState>();
	result->values[0] = low_value;
	result->expressions[0] = low_expression_type;
	result->values[1] = high_value;
	result->expressions[1] = high_expression_type;
	return std::move(result);
}

unique_ptr<IndexScanState> RMIIndex::TryInitializeScan(const Expression &expr, const Expression &filter_expr) {
	
    // --- Column Match Check ---
    // Only scan when the filter references the indexed column
    if (!expr.Equals(*unbound_expressions[0])) {
        return nullptr;
    }
    
    Value low_value, high_value, equal_value;
	ExpressionType low_comparison_type = ExpressionType::INVALID, high_comparison_type = ExpressionType::INVALID;

	// Try to find a matching index for any of the filter expressions.
	ComparisonExpressionMatcher matcher;

	// Match on a comparison type.
	matcher.expr_type = make_uniq<ComparisonExpressionTypeMatcher>();

	// Match on a constant comparison with the indexed expression.
	matcher.matchers.push_back(make_uniq<ExpressionEqualityMatcher>(expr));
	matcher.matchers.push_back(make_uniq<ConstantExpressionMatcher>());
	matcher.policy = SetMatcher::Policy::UNORDERED;

	vector<reference<Expression>> bindings;
	auto filter_match =
	    matcher.Match(const_cast<Expression &>(filter_expr), bindings); // NOLINT: Match does not alter the expr.
	if (filter_match) {
		// This is a range or equality comparison with a constant value, so we can use the index.
		// 		bindings[0] = the expression
		// 		bindings[1] = the index expression
		// 		bindings[2] = the constant
		auto &comparison = bindings[0].get().Cast<BoundComparisonExpression>();
		auto constant_value = bindings[2].get().Cast<BoundConstantExpression>().value;
		auto comparison_type = comparison.GetExpressionType();

		if (comparison.left->GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
			// The expression is on the right side, we flip the comparison expression.
			comparison_type = FlipComparisonExpression(comparison_type);
		}

		if (comparison_type == ExpressionType::COMPARE_EQUAL) {
			// An equality value overrides any other bounds.
			equal_value = constant_value;
		} else if (comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO ||
		           comparison_type == ExpressionType::COMPARE_GREATERTHAN) {
			// This is a lower bound.
			low_value = constant_value;
			low_comparison_type = comparison_type;
		} else {
			// This is an upper bound.
			high_value = constant_value;
			high_comparison_type = comparison_type;
		}

	} else if (filter_expr.GetExpressionType() == ExpressionType::COMPARE_BETWEEN) {
		auto &between = filter_expr.Cast<BoundBetweenExpression>();
		if (!between.input->Equals(expr)) {
			// The expression does not match the index expression.
			return nullptr;
		}

		if (between.lower->GetExpressionType() != ExpressionType::VALUE_CONSTANT ||
		    between.upper->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			// Not a constant expression.
			return nullptr;
		}

		low_value = between.lower->Cast<BoundConstantExpression>().value;
		low_comparison_type = between.lower_inclusive ? ExpressionType::COMPARE_GREATERTHANOREQUALTO
		                                              : ExpressionType::COMPARE_GREATERTHAN;
		high_value = (between.upper->Cast<BoundConstantExpression>()).value;
		high_comparison_type =
		    between.upper_inclusive ? ExpressionType::COMPARE_LESSTHANOREQUALTO : ExpressionType::COMPARE_LESSTHAN;
	}
	// FIXME: add another if...else... to match rewritten BETWEEN,
	// i.e., WHERE i BETWEEN 50 AND 1502 is rewritten to CONJUNCTION_AND.

	// We cannot use an index scan.
	if (equal_value.IsNull() && low_value.IsNull() && high_value.IsNull()) {
		return nullptr;
	}

	// Initialize the index scan state and return it.
	if (!equal_value.IsNull()) {
		// Equality predicate.
		return InitializeScanSinglePredicate(equal_value, ExpressionType::COMPARE_EQUAL);
	}
	if (!low_value.IsNull() && !high_value.IsNull()) {
		// Two-sided predicate.
		return InitializeScanTwoPredicates(low_value, low_comparison_type, high_value, high_comparison_type);
	}
	if (!low_value.IsNull()) {
		// Less-than predicate.
		return InitializeScanSinglePredicate(low_value, low_comparison_type);
	}
	// Greater-than predicate.
	return InitializeScanSinglePredicate(high_value, high_comparison_type);
}

// Scan dispatcher
bool RMIIndex::Scan(IndexScanState &state,
                    idx_t max_count,
                    std::set<row_t> &result_ids) {
    auto &s = state.Cast<RMIIndexScanState>();

    double key_low = s.values[0].GetValue<double>();

    // Single predicate?
    if (s.values[1].IsNull()) {
        lock_guard<mutex> guard(rmi_lock);

        switch (s.expressions[0]) {
            case ExpressionType::COMPARE_EQUAL:
                return SearchEqual(key_low, max_count, result_ids);
            case ExpressionType::COMPARE_GREATERTHAN:
                return SearchGreater(key_low, false, max_count, result_ids);
            case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
                return SearchGreater(key_low, true, max_count, result_ids);
            case ExpressionType::COMPARE_LESSTHAN:
                return SearchLess(key_low, false, max_count, result_ids);
            case ExpressionType::COMPARE_LESSTHANOREQUALTO:
                return SearchLess(key_low, true, max_count, result_ids);
            default:
                throw InternalException("RMI index scan type not implemented");
        }
    }

    // Two-sided (BETWEEN)
    lock_guard<mutex> guard(rmi_lock);

    double key_high = s.values[1].GetValue<double>();

    bool left_eq = (s.expressions[0] == ExpressionType::COMPARE_GREATERTHANOREQUALTO);
    bool right_eq = (s.expressions[1] == ExpressionType::COMPARE_LESSTHANOREQUALTO);

    return SearchCloseRange(key_low, key_high, left_eq, right_eq, max_count, result_ids);
}

//==============================================================================
// Core Search Routines (adapted to BaseRMIModel)
//==============================================================================
bool RMIIndex::SearchEqual(double key, idx_t max_count, std::set<row_t> &out) {
    // 1. Search Main Index (Binary Search / RMI Model)
    // We still use Epsilon here for the main sorted data
    const double epsilon = 1e-9;
    
    auto bounds = model->GetSearchBounds(key, (idx_t)index_data.size());
    idx_t start = bounds.first;
    idx_t end = bounds.second;

    if (start > index_data.size()) start = index_data.size();
    if (end > index_data.size()) end = index_data.size();

    for (idx_t i = start; i < end + 10; i++) {
        // Epsilon check for main data
        if (i >= index_data.size()) 
            break;

        if (std::abs(index_data[i].key - key) < epsilon) {
            if (out.size() + 1 > max_count) return false;
            out.insert(index_data[i].row_id);
        }
    }

    // 2. Search Overflow Map (Linear Scan as requested)
    for (auto &kv : model->GetOverflowMap()) {
        double k = kv.first;

        // "k == key" replacement: Check if k is within epsilon of key
        if (std::abs(k - key) < epsilon) {
            for (auto rid : kv.second) {
                if (out.size() + 1 > max_count) return false;
                out.insert(rid);
            }
        }
    }

    return true;
}


bool RMIIndex::SearchGreater(double key, bool equal, idx_t max_count, std::set<row_t> &out) {
    idx_t start = model->PredictPosition(key) + model->GetMinError();

    for (idx_t i = start; i < index_data.size(); i++) {
        double k = index_data[i].key;
        bool ok = equal ? (k >= key) : (k > key);
        if (ok) {
            if (out.size() + 1 > max_count) return false;
            out.insert(index_data[i].row_id);
        }
    }

    for (auto &kv : model->GetOverflowMap()) {
        double k = kv.first;
        if (!equal && k == key) continue;
        if (k >= key) {
            for (auto rid : kv.second) {
                if (out.size() + 1 > max_count) return false;
                out.insert(rid);
            }
        }
    }

    // RMILog("Printing the SearchGreater Results:\n");
    // for (std::set<row_t>::iterator it = out.begin(); it != out.end(); it++) {
    //     RMILog(std::to_string(*it) + "\n");
    // }
    // RMILog("End of SearchGreater Result\n");

    return true;
}

bool RMIIndex::SearchLess(double key, bool equal, idx_t max_count, std::set<row_t> &out) {
    idx_t end = std::min<int64_t>(index_data.size(), (idx_t)(model->PredictPosition(key) + model->GetMaxError()));

    for (idx_t i = 0; i < end; i++) {
        double k = index_data[i].key;
        bool ok = equal ? (k <= key) : (k < key);
        if (ok) {
            if (out.size() + 1 > max_count) return false;
            out.insert(index_data[i].row_id);
        }
    }

    for (auto &kv : model->GetOverflowMap()) {
        double k = kv.first;
        if (k > key) continue;
        if (!equal && k == key) continue;
        for (auto rid : kv.second) {
            if (out.size() + 1 > max_count) return false;
            out.insert(rid);
        }
    }
    return true;
}

bool RMIIndex::SearchCloseRange(double low,
                                double high,
                                bool left_eq,
                                bool right_eq,
                                idx_t max_count,
                                std::set<row_t> &out) {

    idx_t start = model->PredictPosition(low) + model->GetMinError();
    idx_t end = std::min<int64_t>(index_data.size(), (idx_t)(model->PredictPosition(high) + model->GetMaxError()));

    for (idx_t i = start; i < end; i++) {
        double k = index_data[i].key;

        bool ok_left = left_eq ? (k >= low) : (k > low);
        bool ok_right = right_eq ? (k <= high) : (k < high);

        if (ok_left && ok_right) {
            if (out.size() + 1 > max_count) return false;
            out.insert(index_data[i].row_id);
        }
    }

    for (auto &kv : model->GetOverflowMap()) {
        double k = kv.first;

        if (!left_eq && k == low) continue;
        if (right_eq ? (k > high) : (k >= high)) break;

        if (k >= low && k <= high) {
            for (auto rid : kv.second) {
                if (out.size() + 1 > max_count) return false;
                out.insert(rid);
            }
        }
    }
    return true;
}

//==============================================================================
// Persistence (optional)
//==============================================================================

IndexStorageInfo RMIIndex::SerializeToDisk(QueryContext ctx,
                                           const case_insensitive_map_t<Value> &opts) {
    throw NotImplementedException("RMI persistence not implemented yet");
}

IndexStorageInfo RMIIndex::SerializeToWAL(const case_insensitive_map_t<Value> &) {
    throw NotImplementedException("RMI WAL persistence not implemented yet");
}

} // namespace duckdb
