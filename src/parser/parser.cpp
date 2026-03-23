#include "parser.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parser_helper.hpp"
#include "parser_helpers.hpp"
#include "server_debug.hpp"

#include "duckdb/catalog/catalog.hpp"
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
static thread_local string g_sidra_pending_queries;
// NOLINTEND(cppcoreguidelines-avoid-non-const-global-variables)

//===--------------------------------------------------------------------===//
// DDL executor — bind-phase execution
//===--------------------------------------------------------------------===//

unique_ptr<FunctionData> SIDRADDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	string queries = g_sidra_pending_queries;
	g_sidra_pending_queries.clear();

	if (!queries.empty()) {
		// Execute each query in its own transaction via a separate connection
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection conn(db);
		auto query_list = StringUtil::Split(queries, ';');
		for (auto &query : query_list) {
			StringUtil::Trim(query);
			if (query.empty()) {
				continue;
			}
			auto result = conn.Query(query + ";");
			if (result->HasError()) {
				throw CatalogException("Failed to execute SIDRA DDL: " + result->GetError());
			}
		}
		SERVER_DEBUG_PRINT("Executed compiled SIDRA queries in bind phase");
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("success");

	return make_uniq<SIDRADDLBindData>(queries);
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

		con.BeginTransaction();
		auto r = con.Query(query + ";");
		if (r->HasError()) {
			con.Rollback();
			throw ParserException("Error executing query [" + query + "]: " + r->GetError());
		}
		con.Commit();

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

static string CompileTableCreation(Connection &con, SIDRAParseData &data) {
	string metadata_queries;
	auto &table_name = data.table_name;

	if (data.scope == TableScope::decentralized) {
		// Validate the table creation in the parser DB
		auto r = con.Query(data.stripped_sql);
		if (r->HasError()) {
			throw ParserException("Error validating table: " + r->GetError());
		}

		metadata_queries += "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
		                    to_string(static_cast<int32_t>(data.scope)) + ", NULL, 0);\n";

		for (auto &[col_name, constraint] : data.table_constraints) {
			metadata_queries += "insert into sidra_table_constraints values ('" + EscapeSingleQuotes(table_name) +
			                    "', '" + EscapeSingleQuotes(col_name) + "', " + to_string(constraint.sensitive) + ", " +
			                    to_string(constraint.fact) + ", " + to_string(constraint.dimension) + ");\n";
		}
	} else {
		metadata_queries += "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
		                    to_string(static_cast<int32_t>(data.scope)) + ", NULL, 0);\n";
	}

	return metadata_queries;
}

static string CompileViewCreation(Connection &con, SIDRAParseData &data, const string &db_name) {
	string metadata_queries;
	auto &view_name = data.view_name;
	auto &view_query = data.view_query;
	auto &vc = data.view_constraint;

	if (view_name.find("sidra_staging_view_") == 0) {
		throw ParserException("Views cannot start with sidra_staging_view_");
	}

	metadata_queries += "insert into sidra_tables values('" + EscapeSingleQuotes(view_name) + "', " +
	                    to_string(static_cast<int32_t>(data.scope)) + ", '" + EscapeSingleQuotes(view_query) +
	                    "', 1);\n";

	// Validate the view creation as a table in the parser DB
	auto create_table_query = "create table " + view_name + " as " + view_query;
	auto r = con.Query(create_table_query);
	if (r->HasError()) {
		throw ParserException("Error validating view: " + r->GetError());
	}

	if (data.scope == TableScope::centralized) {
		auto view_constraint_str = "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) +
		                           "', " + to_string(vc.refresh) + ", " + to_string(vc.ttl) + ", " +
		                           to_string(vc.refresh) + ", " + to_string(vc.min_agg) + ", now());\n";
		metadata_queries += view_constraint_str;

	} else if (data.scope == TableScope::decentralized) {
		// Validate the view query against table constraints
		// Fetch constraints of all tables referenced in the view
		unordered_map<string, SIDRAConstraints> all_constraints;
		con.BeginTransaction();
		try {
			auto table_names = con.GetTableNames(view_query);
			con.Rollback();

			// Switch to metadata DB to look up constraints
			auto constraints_query = "select column_name, sidra_sensitive, sidra_fact, sidra_dimension from "
			                         "sidra_parser_internal.sidra_table_constraints";
			auto constraints_result = con.Query(constraints_query);
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
		} catch (...) {
			con.Rollback();
			throw;
		}

		if (!all_constraints.empty()) {
			CheckViewQueryConstraints(con, view_query, all_constraints);
		}

		metadata_queries += "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) + "', " +
		                    to_string(vc.window) + ", " + to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " +
		                    to_string(vc.min_agg) + ", now());\n";

		metadata_queries += "insert into sidra_tables values('sidra_staging_view_" + EscapeSingleQuotes(view_name) +
		                    "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, 1);\n";

		metadata_queries += "insert into sidra_tables values('sidra_centralized_view_" + EscapeSingleQuotes(view_name) +
		                    "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, 0);\n";

		metadata_queries += "insert into sidra_current_window values('sidra_staging_view_" +
		                    EscapeSingleQuotes(view_name) + "', 0, now());\n";

	} else if (data.scope == TableScope::replicated) {
		// Replicated views exist both centralized and decentralized
		auto view_constraint_str = "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) +
		                           "', 0, 0, " + to_string(vc.refresh) + ", 0, now());\n";
		metadata_queries += view_constraint_str;
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

	// Compile metadata queries using the parser-internal DB for validation
	string metadata_db_name = "sidra_parser.db";
	DuckDB metadata_db(metadata_db_name);
	Connection con(metadata_db);

	string db_name;
	auto db_result = con.Query("select current_database();");
	if (!db_result->HasError()) {
		db_name = db_result->GetValue(0, 0).ToString();
	}

	// Attach the parser-internal DB for schema validation
	auto attach_r = con.Query("attach if not exists 'sidra_parser_internal.db' as sidra_parser_internal;");
	if (attach_r->HasError()) {
		throw ParserException("Error attaching parser DB: " + attach_r->GetError());
	}
	con.Query("use sidra_parser_internal");

	string metadata_queries;
	try {
		if (data.is_table) {
			metadata_queries = CompileTableCreation(con, data);
		} else if (data.is_view) {
			metadata_queries = CompileViewCreation(con, data, db_name);
		}
	} catch (...) {
		// Clean up parser DB connection
		con.Query("use " + db_name);
		con.Query("detach sidra_parser_internal");
		throw;
	}

	con.Query("use " + db_name);
	con.Query("detach sidra_parser_internal");

	// Build the full set of compiled queries to execute in bind phase
	// This includes: the stripped DDL itself + metadata inserts
	string all_queries;
	if (!data.stripped_sql.empty()) {
		all_queries += data.stripped_sql + "\n";
	}
	if (!metadata_queries.empty()) {
		all_queries += metadata_queries;
	}

	// Store in thread-local for the bind function to pick up
	g_sidra_pending_queries = all_queries;

	// Write compiled queries to files for portability
	if (!metadata_queries.empty()) {
		WriteFile("metadata_queries.sql", false, metadata_queries);
	}

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
