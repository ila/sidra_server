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
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/planner/planner.hpp"

#include <chrono>
#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Query execution helpers
//===--------------------------------------------------------------------===//

static string GetCurrentDatabaseName(Connection &con) {
	auto db_name = con.Query("select current_database();");
	if (db_name->HasError()) {
		throw ParserException("Error while getting database name: " + db_name->GetError());
	}
	return db_name->GetValue(0, 0).ToString();
}

static void AttachParserDatabase(Connection &con) {
	auto r = con.Query("attach if not exists 'sidra_parser_internal.db' as sidra_parser_internal;");
	if (r->HasError()) {
		throw ParserException("Error while attaching to sidra_parser_internal.db: " + r->GetError());
	}
	r = con.Query("use sidra_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while switching to sidra_parser_internal.db: " + r->GetError());
	}
}

static void SwitchBackToDatabase(Connection &con, const string &db_name) {
	auto r = con.Query("use " + db_name);
	if (r->HasError()) {
		throw ParserException("Error while switching back to the original database: " + r->GetError());
	}
}

static void DetachParserDatabase(Connection &con) {
	auto r = con.Query("detach sidra_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while detaching from parser db: " + r->GetError());
	}
}

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

static void WriteQueries(const string &queries, const string &file_path, bool append) {
	if (!queries.empty()) {
		WriteFile(file_path, append, queries);
	}
}

static void ParseExecuteQuery(Connection &con, const string &query) {
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while executing parser queries: " + r->GetError());
	}
}

static string ConstructTable(Connection &con, const string &view_name_in, bool is_staging) {
	auto table_info = con.TableInfo(view_name_in);
	if (!table_info) {
		throw ParserException("Table not found: " + view_name_in);
	}

	string target_name;
	if (is_staging) {
		target_name = "sidra_staging_view_" + view_name_in;
	} else {
		target_name = "sidra_centralized_view_" + view_name_in;
	}

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

//===--------------------------------------------------------------------===//
// Parse function
//===--------------------------------------------------------------------===//

ParserExtensionParseResult SIDRAParserExtension::SIDRAParseFunction(ParserExtensionInfo *info, const string &query) {
	// Clean and normalize the query
	string cleaned = CleanQuery(query);

	// Try to extract a SIDRA scope
	auto scope = ExtractScope(cleaned);
	if (scope == TableScope::null) {
		// Not a SIDRA statement — let DuckDB handle it
		return ParserExtensionParseResult();
	}

	// Only CREATE statements are handled
	if (!StringUtil::StartsWith(cleaned, "create")) {
		return ParserExtensionParseResult();
	}

	// Build enriched parse data
	auto data = make_uniq<SIDRAParseData>();
	data->scope = scope;

	if (StringUtil::StartsWith(cleaned, "create table")) {
		data->is_table = true;
		if (scope == TableScope::decentralized) {
			data->table_constraints = ExtractTableConstraints(cleaned);
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
// Plan function — compile and execute SIDRA DDL
//===--------------------------------------------------------------------===//

static void CompileTableCreation(Connection &con, SIDRAParseData &data, string &centralized_queries,
                                 string &decentralized_queries, string &metadata_queries) {
	ParseExecuteQuery(con, data.stripped_sql);
	auto &table_name = data.table_name;

	if (data.scope == TableScope::decentralized) {
		con.BeginTransaction();
		Parser parser;
		parser.ParseQuery(data.stripped_sql);
		auto statement = parser.statements[0].get();
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		CheckConstraints(*planner.plan, data.table_constraints);

		auto table_insert = "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
		                    to_string(static_cast<int32_t>(data.scope)) + ", NULL, 0);\n";
		metadata_queries += table_insert;
		decentralized_queries += data.stripped_sql;

		for (auto &[col_name, constraint] : data.table_constraints) {
			metadata_queries += "insert into sidra_table_constraints values ('" + EscapeSingleQuotes(table_name) +
			                    "', '" + EscapeSingleQuotes(col_name) + "', " + to_string(constraint.sensitive) + ", " +
			                    to_string(constraint.fact) + ", " + to_string(constraint.dimension) + ");\n";
		}
		con.Rollback();
	} else {
		if (data.scope == TableScope::replicated) {
			centralized_queries += data.stripped_sql;
			decentralized_queries += data.stripped_sql;
		} else {
			centralized_queries += data.stripped_sql;
		}
		metadata_queries += "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
		                    to_string(static_cast<int32_t>(data.scope)) + ", NULL, 0);\n";
	}
}

static void CompileViewCreation(Connection &con, SIDRAParseData &data, const string &db_name,
                                string &centralized_queries, string &decentralized_queries, string &secure_queries,
                                string &metadata_queries) {
	auto &view_name = data.view_name;
	auto &view_query = data.view_query;
	auto &vc = data.view_constraint;

	if (view_name.find("sidra_staging_view_") == 0) {
		throw ParserException("Views cannot start with sidra_staging_view_");
	}

	metadata_queries += "insert into sidra_tables values('" + EscapeSingleQuotes(view_name) + "', " +
	                    to_string(static_cast<int32_t>(data.scope)) + ", '" + EscapeSingleQuotes(view_query) +
	                    "', 1);\n";

	if (data.scope == TableScope::replicated) {
		ParseExecuteQuery(con, data.stripped_sql);
		centralized_queries += data.stripped_sql;
		decentralized_queries += data.stripped_sql;
	}

	auto create_table_query = "create table " + view_name + " as " + view_query;
	ParseExecuteQuery(con, create_table_query);

	if (data.scope == TableScope::centralized) {
		SwitchBackToDatabase(con, db_name);
		con.BeginTransaction();
		auto tables = con.GetTableNames(view_query);
		string query_copy = data.stripped_sql;
		for (auto &table : tables) {
			auto table_info = con.TableInfo(table);
			if (!table_info) {
				// Table doesn't exist server-side — use centralized view name
				query_copy = StringUtil::Replace(query_copy, table, "sidra_centralized_view_" + table);
			}
		}
		con.Rollback();
		AttachParserDatabase(con);
		centralized_queries += query_copy;

		auto constraint_str = "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) + "', " +
		                      to_string(vc.refresh) + ", " + to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " +
		                      to_string(vc.min_agg) + ", now());\n";
		metadata_queries += constraint_str;

	} else if (data.scope == TableScope::decentralized) {
		decentralized_queries += data.stripped_sql;
		secure_queries += ConstructTable(con, view_name, true);

		metadata_queries += "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) + "', " +
		                    to_string(vc.window) + ", " + to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " +
		                    to_string(vc.min_agg) + ", now());\n";
		metadata_queries += "insert into sidra_tables values('sidra_staging_view_" + EscapeSingleQuotes(view_name) +
		                    "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, 1);\n";

		centralized_queries += ConstructTable(con, view_name, false);
		metadata_queries += "insert into sidra_tables values('sidra_centralized_view_" + EscapeSingleQuotes(view_name) +
		                    "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, 0);\n";
		metadata_queries += "insert into sidra_current_window values('sidra_staging_view_" +
		                    EscapeSingleQuotes(view_name) + "', 0, now());\n";
	}
}

ParserExtensionPlanResult SIDRAParserExtension::SIDRAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                                  unique_ptr<ParserExtensionParseData> parse_data) {
	auto &data = dynamic_cast<SIDRAParseData &>(*parse_data);

	// Load config (non-fatal if missing)
	string config_path = "";
	string config_file = "server.config";
	unordered_map<string, string> config;
	try {
		config = ParseConfig(config_path, config_file);
	} catch (const InvalidConfigurationException &) {
		SERVER_DEBUG_PRINT("No server.config found, using defaults");
		config["db_name"] = "sidra_server.db";
	}

	string centralized_queries;
	string decentralized_queries;
	string secure_queries;
	string metadata_queries;

	string metadata_db_name = "sidra_parser.db";
	string client_db_name = "sidra_client.db";
	string server_db_name = config["db_name"];

	DuckDB db(metadata_db_name);
	Connection con(db);
	string db_name = GetCurrentDatabaseName(con);
	AttachParserDatabase(con);

	if (data.is_table) {
		CompileTableCreation(con, data, centralized_queries, decentralized_queries, metadata_queries);
	} else if (data.is_view) {
		CompileViewCreation(con, data, db_name, centralized_queries, decentralized_queries, secure_queries,
		                    metadata_queries);
	}
	// TODO: implement SELECT, INSERT, UPDATE, DELETE, DROP, ALTER TABLE

	SwitchBackToDatabase(con, db_name);
	DetachParserDatabase(con);

	DuckDB server_db(server_db_name);
	Connection server_con(server_db);
	DuckDB client_db(client_db_name);
	Connection client_con(client_db);

	string path; // TODO: make configurable
	ExecuteAndWriteQueries(server_con, centralized_queries, path + "centralized_queries.sql", false);
	ExecuteAndWriteQueries(con, metadata_queries, path + "metadata_queries.sql", false);
	WriteQueries(decentralized_queries, path + "decentralized_queries.sql", true);
	ExecuteAndWriteQueries(client_con, secure_queries, path + "secure_queries.sql", false);

	// Generate server refresh script
	server_con.BeginTransaction();
	auto r = server_con.Query("pragma generate_server_refresh_script('" + EscapeSingleQuotes(server_db_name) + "');");
	if (r->HasError()) {
		server_con.Rollback();
		throw ParserException("Error while generating server refresh script: " + r->GetError());
	}
	server_con.Commit();

	// TODO: generate client-side refresh script

	ParserExtensionPlanResult result;
	result.function = SIDRAFunction();
	result.parameters.push_back(true);
	result.modified_databases = {};
	result.requires_valid_transaction = false;
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
