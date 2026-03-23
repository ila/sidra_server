#include "parser_helper.hpp"
#include "server_debug.hpp"

#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/planner.hpp>
#include <stack>

namespace duckdb {

void CheckConstraints(LogicalOperator &plan, unordered_map<string, SIDRAConstraints> &constraints) {
	if (constraints.empty()) {
		throw ParserException("Decentralized tables must have privacy-preserving constraints!");
	}

	// TODO: implement constraint validation
	// checks:
	// 1. a sensitive column cannot be in a projection or a filter
	// 2. if a sensitive column is in a group by clause, there must be a minimum aggregation
	// 3. minimum aggregation columns must be used in a query with a group by

	SERVER_DEBUG_PRINT("Constraint validation not yet implemented");
}

} // namespace duckdb
