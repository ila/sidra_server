#pragma once

#include "duckdb.hpp"
#include "lpts_helpers.hpp"

namespace duckdb {

void WriteFile(const string &filename, bool append, const string &compiled_query);
string ReadFile(const string &file_path);
string ExtractTableName(const string &sql);
string ExtractViewName(const string &sql);
string ExtractViewQuery(string &query);
void ReplaceMaterializedView(string &query);

} // namespace duckdb
