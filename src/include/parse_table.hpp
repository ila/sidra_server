#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

TableScope ParseScope(std::string &query);
unordered_map<string, SIDRAConstraints> ParseCreateTable(string &query);

} // namespace duckdb
