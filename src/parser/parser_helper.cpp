#include "parser_helper.hpp"
#include "server_debug.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include <stack>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

//! Collect the set of column names that are restricted (SENSITIVE or FACT)
static unordered_set<string> CollectRestrictedColumns(const unordered_map<string, SIDRAConstraints> &constraints) {
	unordered_set<string> restricted;
	for (auto &[col_name, constraint] : constraints) {
		if (constraint.sensitive || constraint.fact) {
			restricted.insert(StringUtil::Lower(col_name));
		}
	}
	return restricted;
}

//! Find all LogicalGet operators in the plan and build a mapping of table_index → column names
static void CollectTableColumns(LogicalOperator &op, unordered_map<idx_t, vector<string>> &table_columns) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		table_columns[get.table_index] = get.names;
	}
	for (auto &child : op.children) {
		CollectTableColumns(*child, table_columns);
	}
}

//! Check if a plan contains an aggregation operator
static bool HasAggregation(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasAggregation(*child)) {
			return true;
		}
	}
	return false;
}

//! Resolve a column binding to a column name using the table_columns map
static string ResolveColumnName(ColumnBinding binding, const unordered_map<idx_t, vector<string>> &table_columns) {
	auto it = table_columns.find(binding.table_index);
	if (it != table_columns.end() && binding.column_index < it->second.size()) {
		return StringUtil::Lower(it->second[binding.column_index]);
	}
	return "";
}

//! Check if an expression references any restricted column (non-recursively for the top level)
static bool ExpressionReferencesRestricted(Expression &expr, const unordered_set<string> &restricted,
                                           const unordered_map<idx_t, vector<string>> &table_columns,
                                           string &found_column) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		string col_name = ResolveColumnName(col_ref.binding, table_columns);
		if (!col_name.empty() && restricted.count(col_name)) {
			found_column = col_name;
			return true;
		}
	}
	// Recurse into child expressions
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (!found) {
			found = ExpressionReferencesRestricted(child, restricted, table_columns, found_column);
		}
	});
	return found;
}

//! Check that GROUP BY expressions don't contain restricted columns
static void CheckGroupByNotRestricted(LogicalAggregate &agg, const unordered_set<string> &restricted,
                                      const unordered_map<idx_t, vector<string>> &table_columns) {
	for (auto &group_expr : agg.groups) {
		string found_column;
		if (ExpressionReferencesRestricted(*group_expr, restricted, table_columns, found_column)) {
			throw ParserException("Column '" + found_column +
			                      "' is marked as SENSITIVE or FACT and cannot be used in GROUP BY. "
			                      "Only DIMENSION columns can appear in GROUP BY.");
		}
	}
}

//! Check that projection expressions don't expose restricted columns raw (outside aggregates)
static void CheckProjectionNotRestricted(LogicalOperator &op, const unordered_set<string> &restricted,
                                         const unordered_map<idx_t, vector<string>> &table_columns) {
	if (op.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &expr : op.expressions) {
			// Aggregate expressions are allowed to reference restricted columns
			if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			// BoundColumnRef referencing the aggregate output table is OK (it's an aggregate result)
			// We need to check if this column ref points to a source table (LogicalGet)
			string found_column;
			if (ExpressionReferencesRestricted(*expr, restricted, table_columns, found_column)) {
				throw ParserException("Column '" + found_column +
				                      "' is marked as SENSITIVE or FACT and cannot be projected directly. "
				                      "It must be used inside an aggregate function (SUM, COUNT, AVG, MIN, MAX).");
			}
		}
	}
	for (auto &child : op.children) {
		CheckProjectionNotRestricted(*child, restricted, table_columns);
	}
}

//! Check that filters above aggregation don't reference restricted columns
static void CheckFiltersAboveAggregation(LogicalOperator &op, const unordered_set<string> &restricted,
                                         const unordered_map<idx_t, vector<string>> &table_columns,
                                         bool below_aggregation) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		below_aggregation = true;
	}

	if (op.type == LogicalOperatorType::LOGICAL_FILTER && !below_aggregation) {
		// This is a filter ABOVE the aggregation (HAVING-like)
		for (auto &expr : op.expressions) {
			string found_column;
			if (ExpressionReferencesRestricted(*expr, restricted, table_columns, found_column)) {
				throw ParserException("Column '" + found_column +
				                      "' is marked as SENSITIVE or FACT and cannot be used in HAVING filters. "
				                      "Only DIMENSION columns can appear in post-aggregation filters.");
			}
		}
	}

	for (auto &child : op.children) {
		CheckFiltersAboveAggregation(*child, restricted, table_columns, below_aggregation);
	}
}

//===--------------------------------------------------------------------===//
// Main entry point
//===--------------------------------------------------------------------===//

void CheckConstraints(LogicalOperator &plan, unordered_map<string, SIDRAConstraints> &constraints) {
	if (constraints.empty()) {
		throw ParserException("Decentralized tables must have privacy-preserving constraints!");
	}

	// Collect restricted columns (SENSITIVE + FACT)
	auto restricted = CollectRestrictedColumns(constraints);
	if (restricted.empty()) {
		SERVER_DEBUG_PRINT("No restricted columns found, skipping validation");
		return;
	}

	// Build table_index → column names mapping
	unordered_map<idx_t, vector<string>> table_columns;
	CollectTableColumns(plan, table_columns);

	// Rule 1: DMVs must have at least one aggregation
	if (!HasAggregation(plan)) {
		throw ParserException(
		    "Decentralized materialized views must contain at least one aggregation (SUM, COUNT, AVG, MIN, MAX). "
		    "Raw data cannot leave the personal data store.");
	}

	// Rule 2: GROUP BY must not contain SENSITIVE or FACT columns
	// Walk the plan to find aggregation operators and check their groups
	std::stack<LogicalOperator *> stack;
	stack.push(&plan);
	while (!stack.empty()) {
		auto *op = stack.top();
		stack.pop();
		if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			auto &agg = op->Cast<LogicalAggregate>();
			CheckGroupByNotRestricted(agg, restricted, table_columns);
		}
		for (auto &child : op->children) {
			stack.push(child.get());
		}
	}

	// Rule 3: Projections must not expose restricted columns raw
	CheckProjectionNotRestricted(plan, restricted, table_columns);

	// Rule 4: Filters above aggregation (HAVING) must not reference restricted columns
	CheckFiltersAboveAggregation(plan, restricted, table_columns, false);

	SERVER_DEBUG_PRINT("Constraint validation passed");
}

//===--------------------------------------------------------------------===//
// View query validation
//===--------------------------------------------------------------------===//

void CheckViewQueryConstraints(Connection &con, const string &view_query,
                               const unordered_map<string, SIDRAConstraints> &constraints) {
	if (constraints.empty()) {
		return;
	}

	// Parse and plan the SELECT query
	con.BeginTransaction();
	try {
		Parser parser;
		parser.ParseQuery(view_query);
		if (parser.statements.empty()) {
			con.Rollback();
			throw ParserException("Empty view query");
		}

		Planner planner(*con.context);
		planner.CreatePlan(parser.statements[0]->Copy());

		// Cast away const for the mutable constraints map (CheckConstraints signature)
		auto mutable_constraints = constraints;
		CheckConstraints(*planner.plan, mutable_constraints);

		con.Rollback();
	} catch (...) {
		con.Rollback();
		throw;
	}
}

} // namespace duckdb
