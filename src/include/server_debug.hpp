#pragma once

#include "duckdb/common/printer.hpp"

// Set to 1 to enable verbose server debug output, 0 to disable
// This is separate from DuckDB's DEBUG macro to avoid cluttering test output
#define SERVER_DEBUG 0

#if SERVER_DEBUG
#define SERVER_DEBUG_PRINT(x) Printer::Print(x)
#else
#define SERVER_DEBUG_PRINT(x) ((void)0)
#endif
