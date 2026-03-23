#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

SIDRAViewConstraint ParseCreateView(string &query, TableScope scope);
string ParseViewTables(string &query);

} // namespace duckdb
