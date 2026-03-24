#pragma once

#include "duckdb.hpp"

#include <string>

namespace duckdb {

enum client_messages {
	CLOSE_CONNECTION = 0,
	NEW_CLIENT = 1,
	NEW_RESULT = 2,
	NEW_STATISTICS = 3,
	NEW_FILE = 4,
	ERROR_NON_EXISTING_CLIENT = 5,
	OK = 6,
	UPDATE_TIMESTAMP_CLIENT = 7,
	FLUSH = 8,
	UPDATE_WINDOW = 9,
};

inline string ToString(client_messages msg) {
	switch (msg) {
	case CLOSE_CONNECTION:
		return "Close connection";
	case NEW_CLIENT:
		return "New client";
	case NEW_RESULT:
		return "New result";
	case NEW_STATISTICS:
		return "New statistics";
	case NEW_FILE:
		return "New file";
	case ERROR_NON_EXISTING_CLIENT:
		return "Error non existing client";
	case OK:
		return "Ok";
	case UPDATE_TIMESTAMP_CLIENT:
		return "Update timestamp client";
	case FLUSH:
		return "Flush";
	case UPDATE_WINDOW:
		return "Update window";
	default:
		return "Unknown message";
	}
}

unordered_map<string, string> ParseConfig(string &path, string &config_name);
void CreateSystemTables(string &path, Connection &con);
void SendFile(std::unordered_map<string, string> &config, int32_t sock);

//! Ensure SIDRA metadata system tables exist in the given connection's database.
//! Uses CREATE TABLE IF NOT EXISTS — safe to call multiple times.
void EnsureMetadataTables(Connection &con);

//! Get the shadow DB filename for schema validation, derived from the current database name.
string GetShadowDBName(ClientContext &context);

} // namespace duckdb
