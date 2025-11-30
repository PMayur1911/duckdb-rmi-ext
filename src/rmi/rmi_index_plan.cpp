#include "duckdb/planner/operator/logical_create_index.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/operator/filter/physical_filter.hpp"

#include "rmi_index.hpp"
#include "rmi_index_physical_create.hpp"

namespace duckdb {

PhysicalOperator &RMIIndex::CreatePlan(PlanIndexInput &input) {
    auto &create_index = input.op;
    auto &planner = input.planner;

    // ------------------------------------------------------------
    // 1. Validate we have only ONE expression
    // ------------------------------------------------------------
    if (create_index.expressions.size() != 1) {
        throw BinderException("RMI indexes can only be created over a single numeric column.");
    }

    // Validate single column is numeric
    auto &key_type = create_index.expressions[0]->return_type;
    switch (key_type.id()) {
        case LogicalTypeId::DOUBLE:
        case LogicalTypeId::FLOAT:
        case LogicalTypeId::INTEGER:
        case LogicalTypeId::BIGINT:
        case LogicalTypeId::SMALLINT:
        case LogicalTypeId::TINYINT:
        case LogicalTypeId::UTINYINT:
        case LogicalTypeId::USMALLINT:
        case LogicalTypeId::UINTEGER:
        case LogicalTypeId::UBIGINT:
            break;
        default:
            throw BinderException("RMI index key must be a numeric type.");
    }

    for (auto &option: create_index.info->options) {
        auto &k = option.first;
        auto &v = option.second;
    
        if (StringUtil::CIEquals(k, "model")) {
            if (v.type() != LogicalType::VARCHAR) {
                throw BinderException("RMI index 'model' must be a string");
            }
            auto model = v.GetValue<string>();
            if (RMIIndex::MODEL_MAP.find(model) == RMIIndex::MODEL_MAP.end()) {
                vector<string> allowed_models;
                for (auto &entry : RMIIndex::MODEL_MAP) {
                    allowed_models.push_back(StringUtil::Format("'%s'", entry));
                }
                throw BinderException("RMI index 'model' must be one of: %s", StringUtil::Join(allowed_models, ", "));
            }
        }
    }

    // ------------------------------------------------------------
    // 2. Build projection operator to compute the index key
    // ------------------------------------------------------------
    vector<LogicalType> proj_types;
    vector<unique_ptr<Expression>> select_list;

    // SELECT <key_expression>
    proj_types.push_back(key_type);
    select_list.push_back(std::move(create_index.expressions[0]));

    // SELECT rowid
    proj_types.push_back(LogicalType::ROW_TYPE);
    select_list.push_back(
        make_uniq<BoundReferenceExpression>(LogicalType::ROW_TYPE,
                                            create_index.info->scan_types.size() - 1));

    auto &projection = planner.Make<PhysicalProjection>(
        proj_types, std::move(select_list), create_index.estimated_cardinality);

    projection.children.push_back(input.table_scan);

    // ------------------------------------------------------------
    // 3. Add a NOT-NULL filter on the key column
    // ------------------------------------------------------------
    vector<LogicalType> filter_types;
    vector<unique_ptr<Expression>> filter_exprs;

    filter_types.push_back(key_type);

    auto is_not_null = make_uniq<BoundOperatorExpression>(
        ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);

    is_not_null->children.push_back(make_uniq<BoundReferenceExpression>(key_type, 0));
    filter_exprs.push_back(std::move(is_not_null));

    auto &null_filter = planner.Make<PhysicalFilter>(
        std::move(filter_types), std::move(filter_exprs), create_index.estimated_cardinality);

    null_filter.types.emplace_back(LogicalType::ROW_TYPE);
    null_filter.children.push_back(projection);

    // ------------------------------------------------------------
    // 4. Create PhysicalCreateRMIIndex operator
    // ------------------------------------------------------------
    auto &physical_create_index = planner.Make<PhysicalCreateRMIIndex>(
        create_index.types,
        create_index.table,
        create_index.info->column_ids,
        std::move(create_index.info),
        std::move(create_index.unbound_expressions),
        create_index.estimated_cardinality
    );

    physical_create_index.children.push_back(null_filter);
    return physical_create_index;
}

} // namespace duckdb
