#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

SIDRASelectOption ParseSelectQuery(string &query);

} // namespace duckdb
