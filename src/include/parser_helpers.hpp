#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Query preprocessing
//===--------------------------------------------------------------------===//

//! Clean and normalize a SQL query: strip comments, collapse whitespace, lowercase (preserving string literals)
string CleanQuery(const string &query);

//! Lowercase a SQL string while preserving single-quoted string literals
string SQLToLowerPreservingStrings(const string &sql);

//===--------------------------------------------------------------------===//
// Scope extraction
//===--------------------------------------------------------------------===//

//! Extract the SIDRA scope (centralized/decentralized/replicated) from a CREATE statement.
//! Returns the scope and a cleaned query with the scope keyword removed.
TableScope ExtractScope(string &query);

//===--------------------------------------------------------------------===//
// Table constraint extraction
//===--------------------------------------------------------------------===//

//! Extract SENSITIVE, FACT, DIMENSION column constraints from a CREATE TABLE statement.
//! Removes the constraint keywords from the query. Returns a map of column_name → constraints.
unordered_map<string, SIDRAConstraints> ExtractTableConstraints(string &query);

//===--------------------------------------------------------------------===//
// View constraint extraction
//===--------------------------------------------------------------------===//

//! Extract WINDOW, TTL, REFRESH, MINIMUM AGGREGATION from a CREATE MATERIALIZED VIEW statement.
//! Validates constraints based on the scope. Removes constraint clauses from the query.
SIDRAViewConstraint ExtractViewConstraints(string &query, TableScope scope);

//===--------------------------------------------------------------------===//
// Select option extraction
//===--------------------------------------------------------------------===//

//! Extract OPTION(RESPONSE RATIO ..., MINIMUM RESPONSE ...) from a SELECT statement.
//! Removes the OPTION clause from the query.
SIDRASelectOption ExtractSelectOptions(string &query);

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

//! Safely parse an integer from a regex match, throwing ParserException on failure
int SafeStoi(const string &str, const string &field_name);

//! Safely parse a double from a string, throwing ParserException on failure
double SafeStod(const string &str, const string &field_name);

} // namespace duckdb
