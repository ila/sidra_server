#include "parser.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parser_helper.hpp"
#include "parser_helpers.hpp"
#include "server_debug.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/planner.hpp"

#include <chrono>
#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Thread-local storage for passing compiled queries from plan → bind
//===--------------------------------------------------------------------===//

// NOLINTBEGIN(cppcoreguidelines-avoid-non-const-global-variables)
static thread_local vector<string> g_sidra_main_queries;
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

static void ExecuteQueries(Connection &con, const vector<string> &queries, const string &error_prefix) {
	for (auto &query : queries) {
		auto result = con.Query(query);
		if (result->HasError()) {
			throw CatalogException(error_prefix + result->GetError());
		}
	}
}

//===--------------------------------------------------------------------===//
// DDL executor — bind-phase execution
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> SIDRADDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto main_queries = std::move(g_sidra_main_queries);
	g_sidra_main_queries.clear();

	// Execute metadata inserts in the main DB
	if (!main_queries.empty()) {
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection conn(db);
		EnsureMetadataTables(conn);
		ExecuteQueries(conn, main_queries, "Failed to execute SIDRA DDL: ");
		SERVER_DEBUG_PRINT("Executed " + to_string(main_queries.size()) + " queries in main DB");
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");
	return make_uniq<SIDRADDLBindData>();
}

void SIDRADDLExecuteFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	output.SetCardinality(0);
}

//===--------------------------------------------------------------------===//
// Query execution helpers (used by flush and other pragmas)
//===--------------------------------------------------------------------===//

void ExecuteAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append) {
	if (!queries.empty()) {
		WriteFile(file_path, append, queries);
		con.BeginTransaction();
		auto r = con.Query(queries);
		if (r->HasError()) {
			con.Rollback();
			throw ParserException("Error while executing compiled queries: " + r->GetError());
		}
		con.Commit();
	}
}

string HashQuery(const string &query) {
	std::hash<std::string> hasher;
	size_t hash_value = hasher(query);
	std::stringstream ss;
	ss << std::hex << hash_value;
	return ss.str().substr(0, 8);
}

void ExecuteCommitLogAndWriteQueries(Connection &con, const string &queries, const string &file_path,
                                     const string &view_name, bool append, int run, bool write) {
	if (queries.empty()) {
		return;
	}

	string csv_path = "results_" + view_name + ".csv";
	string log_path = "log_" + view_name + ".log";

	if (write) {
		WriteFile(file_path, false, queries);
	}
	std::ofstream csv_file(csv_path, run != 0 ? std::ios::app : std::ios::trunc);
	std::ofstream query_log(log_path, std::ios::trunc);

	if (!csv_file.is_open() || !query_log.is_open()) {
		throw IOException("Failed to open output file(s)");
	}

	if (run == 0) {
		csv_file << "run,query_hash,time_ms\n";
	}

	auto query_list = StringUtil::Split(queries, ';');
	for (auto &query : query_list) {
		StringUtil::Trim(query);
		if (query.empty()) {
			continue;
		}

		auto hash = HashQuery(query);
		auto start = std::chrono::high_resolution_clock::now();

		SERVER_DEBUG_PRINT("[EXEC] " + query.substr(0, 80) + (query.size() > 80 ? "..." : ""));
		con.BeginTransaction();
		auto r = con.Query(query + ";");
		if (r->HasError()) {
			con.Rollback();
			SERVER_DEBUG_PRINT("[EXEC] ERROR: " + r->GetError());
			throw ParserException("Error executing query [" + query + "]: " + r->GetError());
		}
		con.Commit();
		SERVER_DEBUG_PRINT("[EXEC] OK (" + to_string(r->Collection().Count()) + " result rows)");

		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> elapsed = end - start;
		int elapsed_ms = static_cast<int>(elapsed.count());

		csv_file << run << "," << hash << "," << elapsed_ms << "\n";
		query_log << "[" << hash << "]\n" << query << "\n\n";
	}
	Printer::Print("Finished run " + std::to_string(run) + "...");
}

void ExecuteCommitAndWriteQueries(Connection &con, const string &queries, const string &file_path, bool append,
                                  bool write) {
	if (!queries.empty() && write) {
		WriteFile(file_path, append, queries);
	}
	auto query_list = StringUtil::Split(queries, ';');
	for (auto &query : query_list) {
		StringUtil::Trim(query);
		if (query.empty()) {
			continue;
		}
		con.BeginTransaction();
		auto r = con.Query(query + ";");
		if (r->HasError()) {
			con.Rollback();
			throw ParserException("Error while executing compiled queries: " + r->GetError());
		}
		con.Commit();
	}
}

//===--------------------------------------------------------------------===//
// Internal helpers for compilation
//===--------------------------------------------------------------------===//

static string ConstructTable(Connection &con, const string &view_name_in, bool is_staging) {
	auto table_info = con.TableInfo(view_name_in);
	if (!table_info) {
		throw ParserException("Table not found: " + view_name_in);
	}

	string target_name = is_staging ? "sidra_staging_view_" + view_name_in : "sidra_centralized_view_" + view_name_in;

	string definition = "create table " + target_name + " (";
	for (auto &column : table_info->columns) {
		definition += column.GetName() + " " + StringUtil::Lower(column.GetType().ToString()) + ", ";
	}
	if (is_staging) {
		definition +=
		    "generation timestamptz, arrival timestamptz, sidra_window int, client_id ubigint, action tinyint);\n";
	} else {
		definition += "sidra_window int, client_id ubigint, responsiveness numeric(5, 2), "
		              "completeness numeric(5, 2), buffer_size numeric(5, 2));\n";
	}
	return definition;
}

static vector<string> CompileTableCreation(Connection &shadow_con, SIDRAParseData &data) {
	vector<string> metadata_queries;
	auto &table_name = data.table_name;

	// Ensure metadata tables exist in shadow DB for constraint lookups
	EnsureMetadataTables(shadow_con);

	// Validate the table creation in the shadow DB (all scopes)
	auto r = shadow_con.Query(data.stripped_sql);
	if (r->HasError()) {
		throw ParserException("Error validating table: " + r->GetError());
	}

	// Metadata for main DB
	metadata_queries.push_back("INSERT OR IGNORE INTO sidra_tables VALUES('" + EscapeSingleQuotes(table_name) + "', " +
	                           to_string(static_cast<int32_t>(data.scope)) + ", NULL, 0)");

	if (data.scope == TableScope::decentralized) {
		// Store constraints in shadow DB for future view validation
		for (auto &[col_name, constraint] : data.table_constraints) {
			shadow_con.Query("INSERT OR REPLACE INTO sidra_table_constraints VALUES ('" +
			                 EscapeSingleQuotes(table_name) + "', '" + EscapeSingleQuotes(col_name) + "', " +
			                 to_string(constraint.sensitive) + ", " + to_string(constraint.fact) + ", " +
			                 to_string(constraint.dimension) + ")");
		}

		// Constraint metadata for main DB
		for (auto &[col_name, constraint] : data.table_constraints) {
			metadata_queries.push_back("INSERT OR IGNORE INTO sidra_table_constraints VALUES ('" +
			                           EscapeSingleQuotes(table_name) + "', '" + EscapeSingleQuotes(col_name) + "', " +
			                           to_string(constraint.sensitive) + ", " + to_string(constraint.fact) + ", " +
			                           to_string(constraint.dimension) + ")");
		}
	}

	return metadata_queries;
}

static vector<string> CompileViewCreation(Connection &shadow_con, SIDRAParseData &data) {
	vector<string> metadata_queries;
	auto &view_name = data.view_name;
	auto &view_query = data.view_query;
	auto &vc = data.view_constraint;

	if (view_name.find("sidra_staging_view_") == 0) {
		throw ParserException("Views cannot start with sidra_staging_view_");
	}

	metadata_queries.push_back("INSERT OR IGNORE INTO sidra_tables VALUES('" + EscapeSingleQuotes(view_name) + "', " +
	                           to_string(static_cast<int32_t>(data.scope)) + ", '" + EscapeSingleQuotes(view_query) +
	                           "', true)");

	if (data.scope == TableScope::centralized) {
		// Compile CMV via OpenIVM in the shadow DB
		auto load_r = shadow_con.Query("LOAD './openivm.duckdb_extension'");
		if (load_r->HasError()) {
			throw ParserException("Failed to load OpenIVM extension: " + load_r->GetError());
		}
		SERVER_DEBUG_PRINT("[CMV] OpenIVM loaded in shadow DB");

		// OpenIVM compiles the MV: creates _ivm_data_<name>, delta tables, index, metadata
		string create_mv = "CREATE MATERIALIZED VIEW " + view_name + " AS " + view_query;
		auto mv_r = shadow_con.Query(create_mv);
		if (mv_r->HasError()) {
			throw ParserException("Error compiling centralized MV: " + mv_r->GetError());
		}
		SERVER_DEBUG_PRINT("[CMV] OpenIVM compiled MV in shadow DB");

		// Trigger a refresh to capture OpenIVM's compiled SQL
		// With cascade_mode != "off" (default "downstream"), this runs even with empty deltas.
		// Set ivm_files_path so OpenIVM writes the complete SQL to a file.
		string ivm_files_dir = "/tmp/sidra_ivm_compile";
		shadow_con.Query("SET ivm_files_path = '" + ivm_files_dir + "'");
		auto refresh_r = shadow_con.Query("PRAGMA ivm('" + EscapeSingleQuotes(view_name) + "')");
		if (refresh_r->HasError()) {
			throw ParserException("Error compiling IVM refresh queries: " + refresh_r->GetError());
		}

		// Read the compiled SQL from the file OpenIVM wrote
		string ivm_file = ivm_files_dir + "/ivm_upsert_queries_" + view_name + ".sql";
		string compiled_sql;
		{
			std::ifstream f(ivm_file);
			if (f.is_open()) {
				std::stringstream buf;
				buf << f.rdbuf();
				compiled_sql = buf.str();
			}
		}
		if (compiled_sql.empty()) {
			throw ParserException("Failed to read compiled IVM SQL from: " + ivm_file);
		}
		SERVER_DEBUG_PRINT("[CMV] OpenIVM compiled SQL:\n" + compiled_sql);

		// Extract IVM type from OpenIVM metadata
		// Column is 'type' (int8): 0=AGGREGATE_GROUP, 1=SIMPLE_AGGREGATE, 2=SIMPLE_PROJECTION, 3=FULL_REFRESH, 4=AGGREGATE_HAVING
		static const char *IVM_TYPE_NAMES[] = {"AGGREGATE_GROUP", "SIMPLE_AGGREGATE", "SIMPLE_PROJECTION",
		                                       "FULL_REFRESH", "AGGREGATE_HAVING"};
		auto type_r = shadow_con.Query("SELECT type FROM _duckdb_ivm_views WHERE view_name = '" +
		                               EscapeSingleQuotes(view_name) + "'");
		string ivm_type = "UNKNOWN";
		if (!type_r->HasError() && type_r->RowCount() > 0) {
			auto type_val = type_r->GetValue(0, 0).GetValue<int8_t>();
			if (type_val >= 0 && type_val <= 4) {
				ivm_type = IVM_TYPE_NAMES[type_val];
			}
		}

		// Get data table schema for creating CMV table on main DB
		string ivm_data_table = "_ivm_data_" + view_name;
		auto data_info = shadow_con.TableInfo(ivm_data_table);
		if (!data_info) {
			throw ParserException("OpenIVM data table not found: " + ivm_data_table);
		}

		// Find source view (DMV that this CMV references)
		shadow_con.BeginTransaction();
		auto source_tables = shadow_con.GetTableNames(view_query);
		shadow_con.Rollback();
		string source_view;
		for (auto &t : source_tables) {
			source_view = t;
			break;
		}
		if (source_view.empty()) {
			throw ParserException("Could not determine source view for CMV: " + view_name);
		}

		// Create CMV data table on main DB (same schema as OpenIVM's _ivm_data_*, minus internals)
		string cmv_data_table = "sidra_cmv_data_" + view_name;
		string cmv_ddl = "CREATE TABLE IF NOT EXISTS " + cmv_data_table + " (";
		for (auto &col : data_info->columns) {
			auto col_name = col.GetName();
			if (col_name.find("_ivm_") == 0) {
				continue;
			}
			cmv_ddl += col_name + " " + StringUtil::Lower(col.GetType().ToString()) + ", ";
		}
		cmv_ddl = cmv_ddl.substr(0, cmv_ddl.size() - 2) + ")";
		metadata_queries.push_back(cmv_ddl);

		// Store metadata
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_view_constraints VALUES('" +
		                           EscapeSingleQuotes(view_name) + "', " + to_string(vc.refresh) + ", " +
		                           to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " + to_string(vc.min_agg) +
		                           ", now())");

		// Store compiled IVM SQL for use during flush
		metadata_queries.push_back(
		    "INSERT OR IGNORE INTO sidra_cmv_queries VALUES('" + EscapeSingleQuotes(view_name) + "', '" +
		    EscapeSingleQuotes(source_view) + "', '" + EscapeSingleQuotes(compiled_sql) + "', '" +
		    EscapeSingleQuotes(cmv_data_table) + "', '" + EscapeSingleQuotes(ivm_type) + "', now())");

		SERVER_DEBUG_PRINT("[CMV] Compiled and stored: " + view_name + " (type=" + ivm_type +
		                   " source=" + source_view + " data_table=" + cmv_data_table + ")");

	} else if (data.scope == TableScope::decentralized) {
		// Validate the view query in the shadow DB (which has table schemas)
		auto create_table_query = "CREATE TABLE " + view_name + " AS " + view_query;
		auto r = shadow_con.Query(create_table_query);
		if (r->HasError()) {
			throw ParserException("Error validating view: " + r->GetError());
		}

		// Validate the view query against table constraints from shadow DB
		unordered_map<string, SIDRAConstraints> all_constraints;
		auto constraints_result = shadow_con.Query(
		    "SELECT column_name, sidra_sensitive, sidra_fact, sidra_dimension FROM sidra_table_constraints");
		if (!constraints_result->HasError()) {
			for (idx_t i = 0; i < constraints_result->RowCount(); i++) {
				string col_name = StringUtil::Lower(constraints_result->GetValue(0, i).ToString());
				SIDRAConstraints c;
				c.sensitive = constraints_result->GetValue(1, i).GetValue<bool>();
				c.fact = constraints_result->GetValue(2, i).GetValue<bool>();
				c.dimension = constraints_result->GetValue(3, i).GetValue<bool>();
				all_constraints[col_name] = c;
			}
		}

		if (!all_constraints.empty()) {
			CheckViewQueryConstraints(shadow_con, view_query, all_constraints);
		}

		// Create the staging view table (receives data from clients)
		string staging_ddl = ConstructTable(shadow_con, view_name, true);
		// Make it IF NOT EXISTS for idempotency
		staging_ddl = std::regex_replace(staging_ddl, std::regex(R"(create table )", std::regex_constants::icase),
		                                 "CREATE TABLE IF NOT EXISTS ");
		metadata_queries.push_back(staging_ddl);

		// Create the centralized view table (final aggregated result with metrics)
		string centralized_ddl = ConstructTable(shadow_con, view_name, false);
		centralized_ddl =
		    std::regex_replace(centralized_ddl, std::regex(R"(create table )", std::regex_constants::icase),
		                       "CREATE TABLE IF NOT EXISTS ");
		metadata_queries.push_back(centralized_ddl);

		// Metadata inserts
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_view_constraints VALUES('" +
		                           EscapeSingleQuotes(view_name) + "', " + to_string(vc.window) + ", " +
		                           to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " + to_string(vc.min_agg) +
		                           ", now())");
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_tables VALUES('sidra_staging_view_" +
		                           EscapeSingleQuotes(view_name) + "', " +
		                           to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, true)");
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_tables VALUES('sidra_centralized_view_" +
		                           EscapeSingleQuotes(view_name) + "', " +
		                           to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, false)");
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_current_window VALUES('sidra_staging_view_" +
		                           EscapeSingleQuotes(view_name) + "', 0, now())");

	} else if (data.scope == TableScope::replicated) {
		// Validate the view query in the shadow DB
		auto create_table_query = "CREATE TABLE " + view_name + " AS " + view_query;
		auto r = shadow_con.Query(create_table_query);
		if (r->HasError()) {
			throw ParserException("Error validating view: " + r->GetError());
		}

		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_view_constraints VALUES('" +
		                           EscapeSingleQuotes(view_name) + "', 0, 0, " + to_string(vc.refresh) + ", 0, now())");
	}

	return metadata_queries;
}

//===--------------------------------------------------------------------===//
// Parse function
//===--------------------------------------------------------------------===//

ParserExtensionParseResult SIDRAParserExtension::SIDRAParseFunction(ParserExtensionInfo *info, const string &query) {
	string cleaned = CleanQuery(query);

	auto scope = ExtractScope(cleaned);
	if (scope == TableScope::null) {
		return ParserExtensionParseResult();
	}

	if (!StringUtil::StartsWith(cleaned, "create")) {
		return ParserExtensionParseResult();
	}

	auto data = make_uniq<SIDRAParseData>();
	data->scope = scope;

	if (StringUtil::StartsWith(cleaned, "create table")) {
		data->is_table = true;
		if (scope == TableScope::decentralized) {
			data->table_constraints = ExtractTableConstraints(cleaned);
			if (data->table_constraints.empty()) {
				throw ParserException("Decentralized tables must have privacy-preserving constraints!");
			}
			bool has_fact = false;
			bool has_dimension = false;
			for (auto &[col, c] : data->table_constraints) {
				if (c.fact) {
					has_fact = true;
				}
				if (c.dimension) {
					has_dimension = true;
				}
			}
			if (!has_fact) {
				throw ParserException("Decentralized tables must have at least one FACT column!");
			}
			if (!has_dimension) {
				throw ParserException("Decentralized tables must have at least one DIMENSION column!");
			}
		}
		data->table_name = ExtractTableName(cleaned);
	} else if (StringUtil::StartsWith(cleaned, "create materialized view")) {
		data->is_view = true;
		data->view_constraint = ExtractViewConstraints(cleaned, scope);
		data->view_name = ExtractViewName(cleaned);
		data->view_query = ExtractViewQuery(cleaned);
	}

	data->stripped_sql = cleaned;

	SERVER_DEBUG_PRINT("Parsed SIDRA statement: scope=" + to_string(static_cast<int>(scope)) +
	                   " is_table=" + to_string(data->is_table) + " is_view=" + to_string(data->is_view));

	return ParserExtensionParseResult(std::move(data));
}

//===--------------------------------------------------------------------===//
// Plan function — validate and compile SIDRA DDL, defer execution to bind
//===--------------------------------------------------------------------===//

ParserExtensionPlanResult SIDRAParserExtension::SIDRAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                                  unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = dynamic_cast<SIDRAParseData &>(*parse_data);

	// Load config (non-fatal if missing)
	string config_path;
	string config_file = "server.config";
	unordered_map<string, string> config;
	try {
		config = ParseConfig(config_path, config_file);
	} catch (const InvalidConfigurationException &) {
		SERVER_DEBUG_PRINT("No server.config found, using defaults");
		config["db_name"] = "sidra_server.db";
	}

	// Open shadow DB for schema validation (persists table schemas across sessions)
	string shadow_db_name = GetShadowDBName(context);
	DBConfig shadow_config;
	shadow_config.SetOptionByName("allow_unsigned_extensions", Value(true));
	DuckDB shadow_db(shadow_db_name, &shadow_config);
	Connection shadow_con(shadow_db);

	// Warn if the main DB is in-memory — centralized tables will be lost on exit
	auto &db_path = context.db->config.options.database_path;
	if (db_path.empty() || db_path == ":memory:") {
		Printer::Print("Warning: database is in-memory. Tables and views will be lost when the session ends. "
		               "Use a persistent database file (e.g., duckdb mydb.db) to keep your data.");
	}

	// Compile: validate in shadow DB, produce metadata queries for main DB
	vector<string> metadata_queries;
	if (data.is_table) {
		metadata_queries = CompileTableCreation(shadow_con, data);
	} else if (data.is_view) {
		metadata_queries = CompileViewCreation(shadow_con, data);
	}

	// For centralized/replicated tables, also execute the DDL in the main DB
	// (decentralized tables only exist in the shadow DB — they're virtual on the server)
	if (data.is_table && data.scope != TableScope::decentralized && !data.stripped_sql.empty()) {
		metadata_queries.insert(metadata_queries.begin(), data.stripped_sql);
	}

	// Write decentralized queries to file for client download
	if (data.scope == TableScope::decentralized && !data.stripped_sql.empty()) {
		WriteFile("decentralized_queries.sql", true, data.stripped_sql);
		SERVER_DEBUG_PRINT("Appended to decentralized_queries.sql");
	}

	// Store in thread-local for the bind function
	g_sidra_main_queries = std::move(metadata_queries);

	SERVER_DEBUG_PRINT("Compiled SIDRA DDL, deferring execution to bind phase");

	// Return a table function that executes in bind phase (PAC pattern)
	ParserExtensionPlanResult result;
	result.function = TableFunction("sidra_ddl_executor", {}, SIDRADDLExecuteFunction, SIDRADDLBindFunction);
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement SIDRABind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}

std::string SIDRAParserExtension::Name() {
	return "sidra_parser_extension";
}

} // namespace duckdb
