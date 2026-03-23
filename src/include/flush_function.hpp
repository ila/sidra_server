#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

void FlushFunction(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb
