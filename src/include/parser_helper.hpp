#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

void CheckConstraints(LogicalOperator &plan, unordered_map<string, SIDRAConstraints> &constraints);

} // namespace duckdb
