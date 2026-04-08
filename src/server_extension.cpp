#define DUCKDB_EXTENSION_MAIN

#include "server_extension.hpp"
#include "compiler_utils.hpp"
#include "common.hpp"
#include "flush_function.hpp"
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

	string schema_name = config.count("schema_name") ? config["schema_name"] : "main";
	Appender appender(con, schema_name, "statistics");
	appender.BeginRow();
	appender.Append<string_t>(string(buffer.begin(), buffer.end()));
	appender.Append<hugeint_t>(client);
	appender.Append<timestamp_t>(Timestamp::GetCurrentTimestamp());
	appender.EndRow();
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();

	// Ensure metadata tables exist in the main DB
	try {
		DuckDB db(instance);
		Connection con(db);
		EnsureMetadataTables(con);
		SERVER_DEBUG_PRINT("SIDRA metadata tables initialized in main DB");
	} catch (const std::exception &e) {
		Printer::Print("Warning: SIDRA metadata tables could not be initialized: " + string(e.what()));
	}

	// Register extension option for config path
	auto &db_config = DBConfig::GetConfig(instance);
	db_config.AddExtensionOption("sidra_config_path", "Path to SIDRA server config directory", LogicalType::VARCHAR,
	                             Value(""));

	// Register parser extension
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

	// Load OpenIVM in the main DB for refresh daemon + hooks
	try {
		DuckDB db(instance);
		Connection con(db);
		auto load_r = con.Query("LOAD './openivm.duckdb_extension'");
		if (load_r->HasError()) {
			Printer::Print("Warning: could not load OpenIVM: " + load_r->GetError());
		} else {
			SERVER_DEBUG_PRINT("Loaded OpenIVM in main DB (refresh daemon active)");
		}
	} catch (const std::exception &e) {
		Printer::Print("Warning: OpenIVM loading failed: " + string(e.what()));
	}

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
