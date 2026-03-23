#pragma once

#include "duckdb.hpp"

namespace duckdb {

string UpdateResponsiveness(string &view_name);
string UpdateCompleteness(string &view_name);
string UpdateBufferSize(string &view_name);
string CleanupExpiredClients(std::unordered_map<string, string> &config);

} // namespace duckdb
