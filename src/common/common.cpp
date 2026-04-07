#include "common.hpp"
#include "server_debug.hpp"

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/connection.hpp"

#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <string>
#include <sys/mman.h>
#include <sys/socket.h>
#include <unistd.h>

namespace duckdb {

unordered_map<string, string> ParseConfig(string &path, string &config_name) {
	unordered_map<string, string> config;
	std::ifstream config_file(path + config_name);
	if (!config_file) {
		config_file.open(config_name);
		if (!config_file) {
			throw InvalidConfigurationException("Config file not found: " + config_name);
		}
	}
	string line;

	while (getline(config_file, line)) {
		// skip empty lines and comments
		if (line.empty() || line[0] == '#') {
			continue;
		}
		std::istringstream config_line(line);
		string key;
		if (std::getline(config_line, key, '=')) {
			string value;
			if (std::getline(config_line, value)) {
				StringUtil::Trim(key);
				StringUtil::Trim(value);
				config.insert(make_pair(key, value));
			}
		}
	}
	SERVER_DEBUG_PRINT("Loaded " + to_string(config.size()) + " config entries");
	return config;
}

void CreateSystemTables(string &path, Connection &con) {
	std::ifstream input_file(path + "tables.sql");
	if (!input_file) {
		input_file.open("tables.sql");
		if (!input_file) {
			throw InvalidConfigurationException("System tables file not found: tables.sql");
		}
	}
	string query;

	while (getline(input_file, query)) {
		if (query.empty()) {
			continue;
		}
		auto result = con.Query(query);
		if (result->HasError()) {
			throw ParserException("Error while creating system tables: " + result->GetError());
		}
	}
	SERVER_DEBUG_PRINT("System tables created successfully");
}

void EnsureMetadataTables(Connection &con) {
	// All DDL uses IF NOT EXISTS — safe to call repeatedly
	static const char *SYSTEM_TABLE_DDL[] = {
	    "CREATE TABLE IF NOT EXISTS sidra_clients(id UBIGINT PRIMARY KEY, creation TIMESTAMP, last_update TIMESTAMP)",
	    "CREATE TABLE IF NOT EXISTS sidra_settings(setting VARCHAR PRIMARY KEY, value VARCHAR)",
	    "CREATE TABLE IF NOT EXISTS sidra_tables(name VARCHAR PRIMARY KEY, type TINYINT, query VARCHAR, is_view "
	    "BOOLEAN)",
	    "CREATE TABLE IF NOT EXISTS sidra_clients_refreshes(id UBIGINT, table_name VARCHAR, last_result TIMESTAMP, "
	    "last_refresh TIMESTAMP)",
	    "CREATE TABLE IF NOT EXISTS sidra_table_constraints(table_name VARCHAR, column_name VARCHAR, sidra_sensitive "
	    "BOOL, sidra_fact BOOL, sidra_dimension BOOL, PRIMARY KEY(table_name, column_name))",
	    "CREATE TABLE IF NOT EXISTS sidra_view_constraints(view_name VARCHAR, sidra_window INT, sidra_ttl TINYINT, "
	    "sidra_refresh TINYINT, sidra_min_agg TINYINT, last_refresh TIMESTAMP, PRIMARY KEY(view_name))",
	    "CREATE TABLE IF NOT EXISTS sidra_current_window(view_name VARCHAR, sidra_window INT, last_update TIMESTAMP, "
	    "PRIMARY KEY(view_name))",
	    "CREATE TABLE IF NOT EXISTS sidra_cmv_queries(view_name VARCHAR PRIMARY KEY, source_view VARCHAR, "
	    "delta_sql VARCHAR, merge_template VARCHAR, data_table_name VARCHAR, ivm_type VARCHAR, last_flush TIMESTAMP "
	    "DEFAULT now())",
	};

	for (auto &ddl : SYSTEM_TABLE_DDL) {
		auto result = con.Query(ddl);
		if (result->HasError()) {
			throw ParserException("Error creating SIDRA system table: " + result->GetError());
		}
	}
	SERVER_DEBUG_PRINT("SIDRA metadata tables ensured");
}

string GetShadowDBName(ClientContext &context) {
	string db_name = "memory";
	try {
		auto &db_instance = DatabaseInstance::GetDatabase(context);
		Connection con(db_instance);
		auto result = con.Query("SELECT current_database()");
		if (!result->HasError() && result->RowCount() > 0) {
			auto name = result->GetValue(0, 0).ToString();
			if (!name.empty() && name != ":memory:") {
				db_name = name;
			}
		}
	} catch (...) {
		// fallback to "memory"
	}
	return "sidra_shadow_" + db_name + ".db";
}

void SendFile(std::unordered_map<string, string> &config, int32_t sock) {
	string file_path = config["db_path"] + "profile_output.json";
	int32_t fd = open(file_path.c_str(), O_RDONLY);
	if (fd < 0) {
		throw IOException("Failed to open file: " + file_path);
	}

	int32_t len = lseek(fd, 0, SEEK_END);
	if (len < 0) {
		close(fd);
		throw IOException("Failed to get file size: " + file_path);
	}

	void *buffer = mmap(0, len, PROT_READ, MAP_SHARED, fd, 0);
	if (buffer == MAP_FAILED) {
		close(fd);
		throw IOException("Failed to mmap file: " + file_path);
	}

	send(sock, &len, sizeof(len), 0);
	send(sock, buffer, len, 0);

	munmap(buffer, len);
	close(fd);
}

} // namespace duckdb
