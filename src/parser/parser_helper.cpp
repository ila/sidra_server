#include "parser_helper.hpp"
#include "server_debug.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Plan traversal helpers (following PAC pattern)
//===--------------------------------------------------------------------===//

//! Find the operator that produces a given table_index in the plan tree
static LogicalOperator *FindOperatorByTableIndex(LogicalOperator *op, idx_t table_index) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.table_index == table_index) {
			return op;
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggr = op->Cast<LogicalAggregate>();
		if (aggr.group_index == table_index || aggr.aggregate_index == table_index) {
			return op;
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		if (proj.table_index == table_index) {
			return op;
		}
	}
	for (auto &child : op->children) {
		auto *result = FindOperatorByTableIndex(child.get(), table_index);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

//! Resolve a column binding to {table_name, column_name} by finding the source LogicalGet.
//! Uses GetColumnIds() to correctly map binding.column_index to the actual column name.
static pair<string, string> ResolveBinding(LogicalOperator &root, const ColumnBinding &binding) {
	auto *source = FindOperatorByTableIndex(&root, binding.table_index);
	if (!source || source->type != LogicalOperatorType::LOGICAL_GET) {
		return {"", ""};
	}

	auto &get = source->Cast<LogicalGet>();
	auto table_entry = get.GetTable();
	string table_name = table_entry ? StringUtil::Lower(table_entry->name) : "";

	const auto &column_ids = get.GetColumnIds();
	string col_name;
	if (binding.column_index < column_ids.size()) {
		col_name = StringUtil::Lower(get.GetColumnName(column_ids[binding.column_index]));
	}

	return {table_name, col_name};
}

//! Check if an expression references any restricted column. Traces bindings back to source tables.
static bool ExpressionReferencesRestricted(Expression &expr, const unordered_set<string> &restricted,
                                           LogicalOperator &root, string &found_column) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col_ref = expr.Cast<BoundColumnRefExpression>();
		auto [table_name, col_name] = ResolveBinding(root, col_ref.binding);
		if (!col_name.empty() && restricted.count(col_name)) {
			found_column = col_name;
			return true;
		}
	}
	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (!found) {
			found = ExpressionReferencesRestricted(child, restricted, root, found_column);
		}
	});
	return found;
}

//===--------------------------------------------------------------------===//
// Constraint checks
//===--------------------------------------------------------------------===//

//! Collect restricted column names (SENSITIVE + FACT)
static unordered_set<string> CollectRestrictedColumns(const unordered_map<string, SIDRAConstraints> &constraints) {
	unordered_set<string> restricted;
	for (auto &[col_name, constraint] : constraints) {
		if (constraint.sensitive || constraint.fact) {
			restricted.insert(StringUtil::Lower(col_name));
		}
	}
	return restricted;
}

//! Check plan has at least one aggregation
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

//! Check GROUP BY doesn't contain restricted columns
static void CheckGroupByNotRestricted(LogicalOperator &plan, const unordered_set<string> &restricted,
                                      LogicalOperator &root) {
	if (plan.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = plan.Cast<LogicalAggregate>();
		for (auto &group_expr : agg.groups) {
			string found_column;
			if (ExpressionReferencesRestricted(*group_expr, restricted, root, found_column)) {
				throw ParserException("Column '" + found_column +
				                      "' is marked as SENSITIVE or FACT and cannot be used in GROUP BY. "
				                      "Only DIMENSION columns can appear in GROUP BY.");
			}
		}
	}
	for (auto &child : plan.children) {
		CheckGroupByNotRestricted(*child, restricted, root);
	}
}

//! Check projections don't expose restricted columns raw (outside aggregates)
static void CheckProjectionNotRestricted(LogicalOperator &plan, const unordered_set<string> &restricted,
                                         LogicalOperator &root) {
	if (plan.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		for (auto &expr : plan.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			string found_column;
			if (ExpressionReferencesRestricted(*expr, restricted, root, found_column)) {
				throw ParserException("Column '" + found_column +
				                      "' is marked as SENSITIVE or FACT and cannot be projected directly. "
				                      "It must be used inside an aggregate function (SUM, COUNT, AVG, MIN, MAX).");
			}
		}
	}
	for (auto &child : plan.children) {
		CheckProjectionNotRestricted(*child, restricted, root);
	}
}

//! Check filters above aggregation don't reference restricted columns
static void CheckFiltersAboveAggregation(LogicalOperator &plan, const unordered_set<string> &restricted,
                                         LogicalOperator &root, bool below_aggregation) {
	if (plan.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		below_aggregation = true;
	}
	if (plan.type == LogicalOperatorType::LOGICAL_FILTER && !below_aggregation) {
		for (auto &expr : plan.expressions) {
			string found_column;
			if (ExpressionReferencesRestricted(*expr, restricted, root, found_column)) {
				throw ParserException("Column '" + found_column +
				                      "' is marked as SENSITIVE or FACT and cannot be used in HAVING filters. "
				                      "Only DIMENSION columns can appear in post-aggregation filters.");
			}
		}
	}
	for (auto &child : plan.children) {
		CheckFiltersAboveAggregation(*child, restricted, root, below_aggregation);
	}
}

//===--------------------------------------------------------------------===//
// Main entry points
//===--------------------------------------------------------------------===//

void CheckConstraints(LogicalOperator &plan, unordered_map<string, SIDRAConstraints> &constraints) {
	if (constraints.empty()) {
		throw ParserException("Decentralized tables must have privacy-preserving constraints!");
	}

	auto restricted = CollectRestrictedColumns(constraints);
	if (restricted.empty()) {
		SERVER_DEBUG_PRINT("No restricted columns found, skipping validation");
		return;
	}

#if SERVER_DEBUG
	plan.Print();
#endif
	SERVER_DEBUG_PRINT("CheckConstraints: " + to_string(restricted.size()) + " restricted columns");

	if (!HasAggregation(plan)) {
		throw ParserException(
		    "Decentralized materialized views must contain at least one aggregation (SUM, COUNT, AVG, MIN, MAX). "
		    "Raw data cannot leave the personal data store.");
	}

	CheckGroupByNotRestricted(plan, restricted, plan);
	CheckProjectionNotRestricted(plan, restricted, plan);
	CheckFiltersAboveAggregation(plan, restricted, plan, false);

	SERVER_DEBUG_PRINT("Constraint validation passed");
}

void CheckViewQueryConstraints(Connection &con, const string &view_query,
                               const unordered_map<string, SIDRAConstraints> &constraints) {
	if (constraints.empty()) {
		return;
	}

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

		auto mutable_constraints = constraints;
		CheckConstraints(*planner.plan, mutable_constraints);

		con.Rollback();
	} catch (...) {
		con.Rollback();
		throw;
	}
}

} // namespace duckdb
