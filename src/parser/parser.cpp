#include "parser.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parse_table.hpp"
#include "parse_view.hpp"
#include "parser_helper.hpp"
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
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <regex>
#include <sstream>
#include <stack>

namespace duckdb {

string SIDRAParserExtension::path;
string SIDRAParserExtension::db;

string GetCurrentDatabaseName(Connection &con) {
	auto db_name = con.Query("select current_database();");
	if (db_name->HasError()) {
		throw ParserException("Error while getting database name: " + db_name->GetError());
	}
	return db_name->GetValue(0, 0).ToString();
}

void AttachParserDatabase(Connection &con) {
	auto r = con.Query("attach if not exists 'sidra_parser_internal.db' as sidra_parser_internal;");
	if (r->HasError()) {
		throw ParserException("Error while attaching to sidra_parser_internal.db: " + r->GetError());
	}
	r = con.Query("use sidra_parser_internal");
	if (r->HasError()) {
		throw ParserException("Error while switching to sidra_parser_internal.db: " + r->GetError());
	}
}

void SwitchBackToDatabase(Connection &con, const string &db_name) {
	auto r = con.Query("use " + db_name);
	if (r->HasError()) {
		throw ParserException("Error while switching back to the original database: " + r->GetError());
	}
}

void DetachParserDatabase(Connection &con) {
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
		con.Commit();

		auto end = std::chrono::high_resolution_clock::now();
		std::chrono::duration<double, std::milli> elapsed = end - start;
		int elapsed_ms = static_cast<int>(elapsed.count());

		if (r->HasError()) {
			throw ParserException("Error executing query [" + query + "]: " + r->GetError());
		}

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
			throw ParserException("Error while executing compiled queries: " + r->GetError());
		}
		con.Commit();
	}
}

void WriteQueries(Connection &con, const string &queries, const string &file_path, bool append) {
	if (!queries.empty()) {
		WriteFile(file_path, append, queries);
	}
}

void ParseExecuteQuery(Connection &con, const string &query) {
	auto r = con.Query(query);
	if (r->HasError()) {
		throw ParserException("Error while executing parser queries: " + r->GetError());
	}
}

string ConstructTable(Connection &con, string view_name, bool view) {
	auto table_info = con.TableInfo(view_name);
	if (!table_info) {
		throw ParserException("Table not found: " + view_name);
	}
	if (view) {
		view_name = "sidra_staging_view_" + view_name;
	} else {
		view_name = "sidra_centralized_view_" + view_name;
	}
	string centralized_table_definition = "create table " + view_name + " (";
	for (auto &column : table_info->columns) {
		centralized_table_definition += column.GetName() + " " + StringUtil::Lower(column.GetType().ToString()) + ", ";
	}
	if (view) {
		centralized_table_definition +=
		    "generation timestamptz, arrival timestamptz, sidra_window int, client_id ubigint, action tinyint);\n";
	} else {
		centralized_table_definition += "sidra_window int, client_id ubigint, responsiveness numeric(5, 2), "
		                                "completeness numeric(5, 2), buffer_size numeric(5, 2));\n";
	}
	return centralized_table_definition;
}

ParserExtensionParseResult SIDRAParserExtension::SIDRAParseFunction(ParserExtensionInfo *info, const string &query) {
	auto query_lower = StringUtil::Lower(StringUtil::Replace(query, "\n", " "));
	StringUtil::Trim(query_lower);
	RemoveRedundantWhitespaces(query_lower);
	if (query_lower.back() != ';' && query_lower.substr(query_lower.size() - 2) != " ;") {
		query_lower += ";";
	}
	query_lower += "\n";
	auto scope = ParseScope(query_lower);

	if (query_lower.substr(0, 6) == "create") {
		if (scope != TableScope::null) {
			SERVER_DEBUG_PRINT("Parsed SIDRA CREATE statement with scope " + to_string(static_cast<int>(scope)));
			return ParserExtensionParseResult(
			    make_uniq_base<ParserExtensionParseData, SIDRAParseData>(query_lower, scope));
		}
	}
	return ParserExtensionParseResult();
}

ParserExtensionPlanResult SIDRAParserExtension::SIDRAPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                                  unique_ptr<ParserExtensionParseData> parse_data) {
	auto query = dynamic_cast<SIDRAParseData *>(parse_data.get())->query;
	auto scope = dynamic_cast<SIDRAParseData *>(parse_data.get())->scope;

	// TODO: make config path configurable
	string server_config_path = "";
	string server_config = "server.config";
	unordered_map<string, string> config;
	try {
		config = ParseConfig(server_config_path, server_config);
	} catch (const InvalidConfigurationException &) {
		SERVER_DEBUG_PRINT("No server.config found, using defaults");
		config["db_name"] = "sidra_server.db";
	}

	string centralized_queries;
	string decentralized_queries;
	string secure_queries;
	string metadata_queries;

	string parser_db_name = path + "sidra_parser_internal.db";
	string metadata_db_name = "sidra_parser.db";
	string client_db_name = "sidra_client.db";
	string server_db_name = config["db_name"];

	DuckDB db(metadata_db_name);
	Connection con(db);
	string db_name = GetCurrentDatabaseName(con);
	AttachParserDatabase(con);

	if (query.substr(0, 6) == "create") {
		if (query.substr(0, 12) == "create table") {
			unordered_map<string, SIDRAConstraints> constraints;
			if (scope == TableScope::decentralized) {
				constraints = ParseCreateTable(query);
			}

			ParseExecuteQuery(con, query); // TODO: rollback if error

			auto table_name = ExtractTableName(query);

			if (scope == TableScope::decentralized) {
				con.BeginTransaction();
				Parser parser;
				parser.ParseQuery(query);
				auto statement = parser.statements[0].get();
				Planner planner(*con.context);
				planner.CreatePlan(statement->Copy());

				CheckConstraints(*planner.plan, constraints);

				auto table_string = "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
				                    to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
				metadata_queries += table_string;

				decentralized_queries += query;
				for (auto &constraint : constraints) {
					auto constraint_string =
					    "insert into sidra_table_constraints values ('" + EscapeSingleQuotes(table_name) + "', '" +
					    EscapeSingleQuotes(constraint.first) + "', " + to_string(constraint.second.sensitive) + ", " +
					    to_string(constraint.second.fact) + ", " + to_string(constraint.second.dimension) + ");\n";
					metadata_queries += constraint_string;
				}
				con.Rollback();
			} else {
				if (scope == TableScope::replicated) {
					centralized_queries += query;
					decentralized_queries += query;
				} else {
					centralized_queries += query;
				}
				auto table_string = "insert into sidra_tables values('" + EscapeSingleQuotes(table_name) + "', " +
				                    to_string(static_cast<int32_t>(scope)) + ", NULL, 0);\n";
				metadata_queries += table_string;
			}

		} else if (query.substr(0, 24) == "create materialized view") {
			auto view_constraints = ParseCreateView(query, scope);
			auto view_name = ExtractViewName(query);
			auto view_query = ExtractViewQuery(query);

			if (view_name.substr(0, 19) == "sidra_staging_view_") {
				throw ParserException("Views cannot start with sidra_staging_view_");
			}
			auto view_string = "insert into sidra_tables values('" + EscapeSingleQuotes(view_name) + "', " +
			                   to_string(static_cast<int32_t>(scope)) + ", '" + EscapeSingleQuotes(view_query) +
			                   "', 1);\n";
			metadata_queries += view_string;
			if (scope == TableScope::replicated) {
				// TODO: do we need anything else here?
				ParseExecuteQuery(con, query);
				centralized_queries += query;
				decentralized_queries += query;
			}
			auto create_table_query = "create table " + view_name + " as " + view_query;
			ParseExecuteQuery(con, create_table_query); // TODO: rollback if error

			if (scope == TableScope::centralized) {
				SwitchBackToDatabase(con, db_name);
				con.BeginTransaction();
				auto tables = con.GetTableNames(view_query);
				for (auto &table : tables) {
					auto table_info = con.TableInfo(table);
					if (!table_info) {
						query = std::regex_replace(query, std::regex(table), "sidra_centralized_view_" + table) + "\n";
					}
				}
				con.Rollback();
				AttachParserDatabase(con);
				centralized_queries += query;
				auto view_constraint_string =
				    "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) + "', " +
				    to_string(view_constraints.refresh) + ", " + to_string(view_constraints.ttl) + ", " +
				    to_string(view_constraints.refresh) + ", " + to_string(view_constraints.min_agg) + ", now());\n";
				metadata_queries += view_constraint_string;
			} else if (scope == TableScope::decentralized) {
				// TODO: check that this is defined over decentralized tables/views
				decentralized_queries += query;
				secure_queries += ConstructTable(con, view_name, true);
				auto view_constraint_string =
				    "insert into sidra_view_constraints values('" + EscapeSingleQuotes(view_name) + "', " +
				    to_string(view_constraints.window) + ", " + to_string(view_constraints.ttl) + ", " +
				    to_string(view_constraints.refresh) + ", " + to_string(view_constraints.min_agg) + ", now());\n";
				metadata_queries += view_constraint_string;
				view_string = "insert into sidra_tables values('sidra_staging_view_" + EscapeSingleQuotes(view_name) +
				              "', " + to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL , 1);\n";
				metadata_queries += view_string;
				centralized_queries += ConstructTable(con, view_name, false);
				view_string = "insert into sidra_tables values('sidra_centralized_view_" +
				              EscapeSingleQuotes(view_name) + "', " +
				              to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL , 0);\n";
				metadata_queries += view_string;

				auto window_string = "insert into sidra_current_window values('sidra_staging_view_" +
				                     EscapeSingleQuotes(view_name) + "', 0, now());\n";
				metadata_queries += window_string;
			}
		}
	}
	// TODO: implement SELECT, INSERT, UPDATE, DELETE, DROP, ALTER TABLE

	SwitchBackToDatabase(con, db_name);
	DetachParserDatabase(con);

	DuckDB server_db(server_db_name);
	Connection server_con(server_db);
	DuckDB client_db(client_db_name);
	Connection client_con(client_db);

	ExecuteAndWriteQueries(server_con, centralized_queries, path + "centralized_queries.sql", false);
	ExecuteAndWriteQueries(con, metadata_queries, path + "metadata_queries.sql", false);
	WriteQueries(con, decentralized_queries, path + "decentralized_queries.sql", true);
	// TODO: make the location/execution of secure queries remote
	ExecuteAndWriteQueries(client_con, secure_queries, path + "secure_queries.sql", false);

	server_con.BeginTransaction();
	auto r = server_con.Query("pragma generate_server_refresh_script('" + EscapeSingleQuotes(server_db_name) + "');");
	if (r->HasError()) {
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
