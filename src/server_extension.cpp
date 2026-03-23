#define DUCKDB_EXTENSION_MAIN

#include "server_extension.hpp"
#include "compiler_utils.hpp"
#include "common.hpp"
#include "flush_function.hpp"
#include "generate_python_script.hpp"
#include "optimizer.hpp"
#include "parser.hpp"
#include "run_server.hpp"
#include "server_debug.hpp"

#include "duckdb/common/serializer/buffered_file_reader.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include <fstream>
#include <string.h>
#include <unistd.h>

namespace duckdb {

static void InitializeServer(Connection &con, string &config_path, unordered_map<string, string> &config) {
	con.BeginTransaction();
	CreateSystemTables(config_path, con);

	for (auto &config_item : config) {
		string string_insert = "insert into sidra_settings values ('" + EscapeSingleQuotes(config_item.first) + "', '" +
		                       EscapeSingleQuotes(config_item.second) + "');";
		auto r = con.Query(string_insert);
		if (r->HasError()) {
			throw ParserException("Error while inserting settings: " + r->GetError());
		}
	}
	con.Commit();
	SERVER_DEBUG_PRINT("Server initialized successfully");
}

void ParseJSON(Connection &con, std::unordered_map<string, string> &config, int32_t connfd, hugeint_t client) {
	int32_t size_json;
	ssize_t bytes_read = read(connfd, &size_json, sizeof(int32_t));
	if (bytes_read <= 0) {
		throw IOException("Failed to read JSON size from client");
	}

	std::vector<char> buffer(size_json);
	bytes_read = read(connfd, buffer.data(), size_json);
	if (bytes_read <= 0) {
		throw IOException("Failed to read JSON data from client");
	}
	SERVER_DEBUG_PRINT("Read " + to_string(bytes_read) + " bytes of JSON");

	// remove newlines using erase-remove idiom
	buffer.erase(std::remove(buffer.begin(), buffer.end(), '\n'), buffer.end());

	// TODO: add schema name
	Appender appender(con, "statistics");
	appender.BeginRow();
	appender.Append<string_t>(string(buffer.begin(), buffer.end()));
	appender.Append<hugeint_t>(client);
	appender.Append<timestamp_t>(Timestamp::GetCurrentTimestamp());
	appender.EndRow();
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();

	// Attempt to load config and initialize server tables (non-fatal if config missing)
	string config_path = "";
	string config_file = "server.config";
	try {
		auto config = ParseConfig(config_path, config_file);
		SERVER_DEBUG_PRINT("Loaded server configuration");

		DuckDB db(instance);
		Connection con(db);

		auto client_info = con.TableInfo("sidra_clients");
		auto db_name = con.Query("select current_database();");
		if (!db_name->HasError()) {
			auto db_name_str = db_name->GetValue(0, 0).ToString();
			if (!client_info && db_name_str == "sidra_parser") {
				Printer::Print("Initializing server!");
				InitializeServer(con, config_path, config);
			}
		}
	} catch (const InvalidConfigurationException &) {
		SERVER_DEBUG_PRINT("No server.config found, skipping server initialization");
	}

	// Register parser extension
	auto &db_config = DBConfig::GetConfig(instance);
	ParserExtension::Register(db_config, SIDRAParserExtension());
	SERVER_DEBUG_PRINT("Registered SIDRA parser extension");

	// Register optimizer rule for DROP TABLE metadata cleanup
	OptimizerExtension::Register(db_config, SIDRADropTableRule());
	SERVER_DEBUG_PRINT("Registered SIDRA DROP TABLE optimizer rule");

	// Register pragma functions
	auto flush = PragmaFunction::PragmaCall("flush", FlushFunction, {LogicalType::VARCHAR}, {LogicalType::VARCHAR});
	loader.RegisterFunction(flush);

	auto run_server = PragmaFunction::PragmaCall("run_server", RunServer, {});
	loader.RegisterFunction(run_server);

	auto gen_server_script = PragmaFunction::PragmaCall("generate_server_refresh_script", GenerateServerRefreshScript,
	                                                    {LogicalType::VARCHAR});
	loader.RegisterFunction(gen_server_script);

	auto gen_client_script = PragmaFunction::PragmaCall("generate_client_refresh_script", GenerateClientRefreshScript,
	                                                    {LogicalType::VARCHAR});
	loader.RegisterFunction(gen_client_script);

	SERVER_DEBUG_PRINT("Registered all pragma functions");
}

void ServerExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

string ServerExtension::Name() {
	return "server";
}

string ServerExtension::Version() const {
#ifdef EXT_VERSION_SERVER
	return EXT_VERSION_SERVER;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(server, loader) {
	duckdb::LoadInternal(loader);
}
}
