#pragma once

#include "duckdb.hpp"

namespace duckdb {

void GenerateServerRefreshScript(ClientContext &context, const FunctionParameters &parameters);
void GenerateClientRefreshScript(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb
