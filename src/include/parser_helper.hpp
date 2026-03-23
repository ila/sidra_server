#pragma once

#include "duckdb.hpp"
#include "helpers.hpp"

namespace duckdb {

//! Validate that a decentralized table has valid privacy constraints.
//! Called during CREATE DECENTRALIZED TABLE to verify constraints are present.
void CheckConstraints(LogicalOperator &plan, unordered_map<string, SIDRAConstraints> &constraints);

//! Validate that a decentralized materialized view query respects privacy constraints.
//! Checks: (1) has aggregation, (2) SENSITIVE/FACT not in GROUP BY, (3) SENSITIVE/FACT not projected raw,
//! (4) SENSITIVE/FACT not in HAVING filters.
//! Called during CREATE DECENTRALIZED MATERIALIZED VIEW.
void CheckViewQueryConstraints(Connection &con, const string &view_query,
                               const unordered_map<string, SIDRAConstraints> &constraints);

} // namespace duckdb
