
#include "src/include/rmi/rmi_index.hpp"

#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/optimizer/matcher/expression_matcher.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "src/include/rmi/rmi_model.hpp"
#include "duckdb/main/database.hpp"

#include "src/include/rmi/rmi_module.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {


//------------------------------------------------------------------------------
// RMI Index Methods
//------------------------------------------------------------------------------
RMI::RMI(const string &name, const IndexConstraintType index_constraint_type, const vector<column_t> &column_ids,
        TableIOManager &table_io_manager, const vector<unique_ptr<Expression>> &unbound_expressions,
        AttachedDatabase &db, const case_insensitive_map_t<Value> &options,
        const IndexStorageInfo &info, idx_t estimated_cardinality) 
    : BoundIndex(name, RMI::TYPE_NAME, index_constraint_type, column_ids, table_io_manager, unbound_expressions, db) {
    
        for (idx_t i = 0; i < types.size(); i++) {
            switch(types[i]) {
                case PhysicalType::INT8:
                case PhysicalType::INT16:
                case PhysicalType::INT32:
                case PhysicalType::INT64:
                case PhysicalType::INT128:
                case PhysicalType::UINT8:
                case PhysicalType::UINT16:
                case PhysicalType::UINT32:
                case PhysicalType::UINT64:
                case PhysicalType::UINT128:
                case PhysicalType::FLOAT:
                case PhysicalType::DOUBLE:
                    break;
                default:
                    throw InvalidTypeException(logical_types[i], "Unsupported type for RMI index key.");
            }
        }

        if (index_constraint_type != IndexConstraintType::NONE) {
            throw NotImplementedException("RMI Indexes do not support UNIQUE or PRIMARY KEY constraints.");
        }

        
        //! Check for NULLs, NaNs, Inf etc maybe?
        
        //! Initialize the RMI model or something here?
        rmi_value = 0;
        
        
        // if (!info.IsValid()) {
        //     // We create a new RMI Index.
        //     return;
        // }
}


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


unique_ptr<IndexScanState> RMI::TryInitializeScan(const Expression &expr, const Expression &filter_expr) {
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

bool RMI::Scan(IndexScanState &state, const idx_t max_count, set<row_t> &row_ids) {
    auto &scan_state = state.Cast<RMIIndexScanState>();
    
    // --- Key Preparation (RMI Version) ---
    // Convert the query Value (e.g., Value(10)) into a double for our model
    // We assume the indexed column is a numeric type that can be cast to double.
    auto key_low = scan_state.values[0].GetValue<double>();
    
    // --- Dispatcher ---
    // This logic is identical to the ART's.

    if (scan_state.values[1].IsNull()) {
        // --- Single Predicate (e.g., =, >, <) ---
        lock_guard<mutex> l(lock);
        switch (scan_state.expressions[0]) {
        case ExpressionType::COMPARE_EQUAL:
            return SearchEqual(key_low, max_count, row_ids);
        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
            return SearchGreater(key_low, true, max_count, row_ids);
        case ExpressionType::COMPARE_GREATERTHAN:
            return SearchGreater(key_low, false, max_count, row_ids);
        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
            return SearchLess(key_low, true, max_count, row_ids);
        case ExpressionType::COMPARE_LESSTHAN:
            return SearchLess(key_low, false, max_count, row_ids);
        default:
            throw InternalException("RMI scan type not implemented");
        }
    }

    // --- Two Predicates (e.g., BETWEEN) ---
    lock_guard<mutex> l(lock);
    
    // Prepare the second key
    auto key_high = scan_state.values[1].GetValue<double>();

    bool left_equal = scan_state.expressions[0] == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
    bool right_equal = scan_state.expressions[1] == ExpressionType::COMPARE_LESSTHANOREQUALTO;
    
    // We call a new function for the most efficient case
    return SearchCloseRange(key_low, key_high, left_equal, right_equal, max_count, row_ids);
}
//------------------------------------------------------------------------------
// RMI Index Override Methods
//------------------------------------------------------------------------------
ErrorData RMI::Insert(IndexLock &lock, DataChunk &data, Vector &row_ids) {
    
    DataChunk expr_chunk;
    expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
    ExecuteExpressions(data, expr_chunk);

    UnifiedVectorFormat key_data;
    expr_chunk.data[0].ToUnifiedFormat(expr_chunk.size(), key_data);
    auto key_values = (double*)key_data.data; 
    
    auto row_id_values = (row_t*)row_ids.GetData();

    for (idx_t i = 0; i < expr_chunk.size(); i++) {
        
        auto key_idx = key_data.sel->get_index(i);

        if (!key_data.validity.RowIsValid(key_idx)) {
            continue; 
        }
        
        double key = key_values[key_idx];
        
        row_t row_id = row_id_values[i];

        model->InsertIntoOverflow(key, row_id);
    }

    return ErrorData {};
}


void RMI::Delete(IndexLock &lock, DataChunk &entries, Vector &row_ids) {
    
    DataChunk expr_chunk;
    expr_chunk.Initialize(Allocator::DefaultAllocator(), logical_types);
    ExecuteExpressions(entries, expr_chunk);

    UnifiedVectorFormat key_data;
    expr_chunk.data[0].ToUnifiedFormat(expr_chunk.size(), key_data);
    auto key_values = (double*)key_data.data; 

    auto row_id_values = (row_t*)row_ids.GetData();

    for (idx_t i = 0; i < expr_chunk.size(); i++) {
        
        auto key_idx = key_data.sel->get_index(i);

        if (!key_data.validity.RowIsValid(key_idx)) {
            continue;
        }

        double key = key_values[key_idx];
        
        row_t row_id = row_id_values[i];

        model->DeleteFromOverflow(key, row_id);
    }
}

void RMI::CommitDrop(IndexLock &index_lock) {
    model.reset();
}

void RMI::Build(Vector &sorted_keys, Vector &sorted_row_ids, const idx_t row_count) {
    // 1. Prepare the Sorted Struct Array
    index_data.clear();
    index_data.reserve(row_count);
    data_size = row_count; // Track size of the static part

    UnifiedVectorFormat key_data;
    sorted_keys.ToUnifiedFormat(row_count, key_data);
    auto raw_keys = (double*)key_data.data;
    auto raw_row_ids = (row_t*)sorted_row_ids.GetData();

    // Copy data into our struct vector
    for (idx_t i = 0; i < row_count; i++) {
        auto key_idx = key_data.sel->get_index(i);
        if (!key_data.validity.RowIsValid(key_idx)) {
            continue;
        }

        RMIEntry entry;
        entry.key = raw_keys[key_idx];
        entry.row_id = raw_row_ids[i];
        index_data.push_back(entry);
    }

    // Ensure strict sorting (vital for binary search or range scans)
    std::sort(index_data.begin(), index_data.end());

    // 2. Train the Model on the Sorted Array
    std::vector<std::pair<double, int64_t>> training_data;
    training_data.reserve(index_data.size());

    for (size_t i = 0; i < index_data.size(); ++i) {
        // Training X = key, Y = actual position in the vector
        training_data.emplace_back(index_data[i].key, (int64_t)i);
    }
    
    model->Train(training_data);
}

ErrorData RMI::Append(IndexLock &l, DataChunk &chunk, Vector &row_ids) {
	return ErrorData();
}
bool RMI::MergeIndexes(IndexLock &state, BoundIndex &other_index) {
	return false;
}
void RMI::Vacuum(IndexLock &l) {
}
string RMI::GetConstraintViolationMessage(VerifyExistenceType verify_type, idx_t failed_index, DataChunk &input) {
	return string();
}
idx_t RMI::GetInMemorySize(IndexLock &state) {
	return idx_t();
}
string RMI::VerifyAndToString(IndexLock &l, const bool only_verify) {
	return string();
}
void RMI::VerifyAllocations(IndexLock &l) {
}
//===--------------------------------------------------------------------===//
// Point and range lookups
//===--------------------------------------------------------------------===//

bool RMI::SearchEqual(double key, idx_t max_count, set<row_t> &row_ids) {
    
    // --- 1. Predict (using our 'model' object) ---
    // 
    auto [start_pos, end_pos] = model->GetSearchBounds(key, data_size);

    // Clamp bounds to vector size
    if (start_pos < 0) start_pos = 0;
    if (end_pos > (int64_t)index_data.size()) end_pos = (int64_t)index_data.size();

    // --- 2. Local Search (using our base table pointers) ---
    for (int64_t i = start_pos; i < end_pos; i++) {
        // Accessing member variables 'base_table_keys' and 'base_table_row_ids' implicitly
        if (index_data[i].key == key) {
            if (row_ids.size() + 1 > max_count) {
                return false; 
            }
            row_ids.insert(index_data[i].row_id);
        }
    }

    // --- 3. Check Overflow Index (using our 'model' object) ---
    auto it = model->overflow_index.find(key);
    if (it != model->overflow_index.end()) {
        for (auto row_id : it->second) {
            if (row_ids.size() + 1 > max_count) {
                return false;
            }
            row_ids.insert(row_id);
        }
    }
    return true; // "Scan complete"
}


bool RMI::SearchGreater(double key, bool equal, idx_t max_count, set<row_t> &row_ids) {
    
    // --- 1. Predict Start Position ---
    // Accessing member 'model' implicitly
    int64_t search_start = std::max((int64_t)0, model->PredictPosition(key) + model->min_error);

    // --- 2. Main Scan ---
    // Accessing member 'data_size' implicitly
    for (int64_t i = search_start; i < data_size; i++) {
        double current_key = index_data[i].key;
        bool matches = (equal) ? (current_key >= key) : (current_key > key);

        if (matches) {
            if (row_ids.size() + 1 > max_count) {
                return false; 
            }
            row_ids.insert(index_data[i].row_id);
        }
    }

    // --- 3. Check Overflow Index ---
    for (auto it = model->overflow_index.lower_bound(key); it != model->overflow_index.end(); ++it) {
        if (!equal && it->first == key) {
            continue; 
        }
        for (auto row_id : it->second) {
            if (row_ids.size() + 1 > max_count) {
                return false;
            }
            row_ids.insert(row_id);
        }
    }
    return true; // "Scan complete"
}


bool RMI::SearchLess(double key, bool equal, idx_t max_count, set<row_t> &row_ids) {
    
    // --- 1. Predict End Position ---
    int64_t search_end = std::min((int64_t)data_size, model->PredictPosition(key) + model->max_error);

    // --- 2. Main Scan ---
    for (int64_t i = 0; i < search_end; i++) {
        double current_key = index_data[i].key;
        bool matches = (equal) ? (current_key <= key) : (current_key < key);

        if (matches) {
            if (row_ids.size() + 1 > max_count) {
                return false;
            }
            row_ids.insert(index_data[i].row_id);
        }
    }

    // --- 3. Check Overflow Index ---
    for (auto const& [ov_key, ov_row_ids] : model->overflow_index) {
        if (ov_key > key) {
            continue; 
        }
        if (!equal && ov_key == key) {
            continue;
        }
        for (auto row_id : ov_row_ids) {
            if (row_ids.size() + 1 > max_count) {
                return false;
            }
            row_ids.insert(row_id);
        }
    }
    return true; // "Scan complete"
}

bool RMI::SearchCloseRange(double key_low, double key_high, bool left_equal, bool right_equal, idx_t max_count, set<row_t> &row_ids) {
    
    // --- 1. Predict Start and End Positions ---
    // 
    // Predict the start of the search
    int64_t search_start = std::max((int64_t)0, model->PredictPosition(key_low) + model->min_error);
    // Predict the end of the search
    int64_t search_end = std::min((int64_t)data_size, model->PredictPosition(key_high) + model->max_error);

    // --- 2. Main Scan (on base table data) ---
    // We scan *only* the small slice of the table between our predicted bounds.
    for (int64_t i = search_start; i < search_end; i++) {
        double current_key = index_data[i].key;

        // Check left bound
        bool gte = (left_equal) ? (current_key >= key_low) : (current_key > key_low);
        // Check right bound
        bool lte = (right_equal) ? (current_key <= key_high) : (current_key < key_high);

        if (gte && lte) {
            // Key is within range. Check if we've hit the max_count limit.
            if (row_ids.size() + 1 > max_count) {
                return false; // "Stop early, we are full"
            }
            row_ids.insert(index_data[i].row_id);
        }
    }

    // --- 3. Check Overflow Index ---
    // We must check all keys in the overflow index >= key_low
    for (auto it = model->overflow_index.lower_bound(key_low); it != model->overflow_index.end(); ++it) {
        double ov_key = it->first;
        
        // Check left bound
        if (!left_equal && ov_key == key_low) {
            continue; // Skip if it's (key > key_low) and we found key_low
        }

        // Check right bound
        if (right_equal) {
            if (ov_key > key_high) break; // We've gone past the range
        } else {
            if (ov_key >= key_high) break; // We've gone past the range
        }

        // Key is in range. Add all its row IDs.
        for (auto row_id : it->second) {
            if (row_ids.size() + 1 > max_count) {
                return false; // "Stop early, we are full"
            }
            row_ids.insert(row_id);
        }
    }

    return true; // "Scan complete"
}

void RMIModule::RegisterIndex(ExtensionLoader &loader) {

	IndexType index_type;

	index_type.name = RMI::TYPE_NAME;
	index_type.create_instance = RMI::Create;
	index_type.create_plan = RMI::CreatePlan;

	// Register the index type
	auto &db = loader.GetDatabaseInstance();
	db.config.GetIndexTypes().RegisterIndexType(index_type);
}


}   // namespace duckdb