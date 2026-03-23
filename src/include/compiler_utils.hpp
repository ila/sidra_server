#pragma once

#include "duckdb.hpp"

namespace duckdb {

void WriteFile(const string &filename, bool append, const string &compiled_query);
string ReadFile(const string &file_path);
string ExtractTableName(const string &sql);
string ExtractViewName(const string &sql);
string ExtractViewQuery(string &query);
string EscapeSingleQuotes(const string &input);
void ReplaceMaterializedView(string &query);
void RemoveRedundantWhitespaces(string &query);

} // namespace duckdb
