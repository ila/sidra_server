#pragma once

#include "duckdb.hpp"

namespace duckdb {

// All metrics read from staging_view, write to centralized_view, grouped by sidra_window
string UpdateResponsiveness(const string &staging_view, const string &centralized_view);
string UpdateCompleteness(const string &staging_view, const string &centralized_view);
string UpdateBufferSize(const string &staging_view, const string &centralized_view);
string CleanupExpiredClients(std::unordered_map<string, string> &config);

} // namespace duckdb
