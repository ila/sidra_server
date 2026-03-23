#include "flush_function.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parser.hpp"
#include "server_debug.hpp"
#include "update_metrics.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/binder.hpp"

#include <regex>

namespace duckdb {

static int flush_run = 0; // benchmark run counter

static string UpdateWithMinimumAggregation(string &centralized_view_name, string &centralized_table_name,
                                           string &column_names, string &join_names, string &protected_column_names,
                                           int minimum_aggregation) {
	string query = "update " + centralized_view_name + " x\nset action = 2 \nfrom (\n\tselect ";
	query += protected_column_names + ", ";
	query += "count(distinct client_id)\n\t";
	query += "from " + centralized_view_name + " \n\t";
	query += "group by " + protected_column_names + "\n\t";
	query += "having count(distinct client_id) >= " + std::to_string(minimum_aggregation);
	query += ") y\n";
	query += "where " + join_names.substr(0, join_names.size() - 6) + ";\n\n";
	query += "insert or replace into " + centralized_table_name + " by name\nselect " + column_names + " \nfrom " +
	         centralized_view_name + " \nwhere action = 2;\n\n";
	query += "delete from " + centralized_view_name + " \nwhere action = 2;\n\n";

	return query;
}

static string UpdateWithoutMinimumAggregation(string &centralized_view_name, string &centralized_table_name,
                                              string &column_names) {
	string query = "insert or replace into " + centralized_table_name + " by name \n";
	query += "select " + column_names + "\n";
	query += "from " + centralized_view_name + "\n";
	query += "where sidra_window > (select expired_window from threshold_window);\n\n";
	query += "delete from " + centralized_view_name + ";\n\n";
	return query;
}

void FlushFunction(ClientContext &context, const FunctionParameters &parameters) {
	// TODO: implement refresh logic (insert or replace into)
	// TODO: implement client deletes (opt out)
	// TODO: add index logic for upsert
	// TODO: check expired windows in 1st update with min agg
	// TODO: only remove local (client) aggregations if the window elapses (not at every refresh)

	auto database = StringValue::Get(parameters.values[1]);
	if (database != "duckdb" && database != "postgres") {
		throw ParserException("Invalid database type: " + database + " - only duckdb and postgres are supported!");
	}

	// TODO: make config path configurable
	string config_path = "";
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);

	string server_db_name = config["db_name"];
	string client_catalog_name = "sidra_client";
	string client_db_name = "sidra_client.db";
	string metadata_db_name = "sidra_parser.db";
	DuckDB server_db(server_db_name);
	DuckDB client_db(client_db_name);
	DuckDB metadata_db(metadata_db_name);
	Connection server_con(server_db);
	Connection client_con(client_db);
	Connection metadata_con(metadata_db);

	auto view_name = StringValue::Get(parameters.values[0]);
	auto centralized_view_name = "sidra_staging_view_" + view_name;
	auto centralized_table_name = "sidra_centralized_view_" + view_name;

	LocalFileSystem fs;

	string file_name = centralized_view_name + "_flush.sql";

	if (fs.FileExists(file_name)) {
		bool append = flush_run > 0;
		string queries = ReadFile(file_name);
		ExecuteCommitLogAndWriteQueries(server_con, queries, file_name, view_name, append, flush_run, false);
		flush_run++;
		return;
	}

	if (view_name.find("_min_agg") != string::npos) {
		throw ParserException("View name cannot contain '_min_agg' - this is reserved for internal use!");
	}

	client_con.BeginTransaction();
	auto centralized_view_catalog_entry =
	    Catalog::GetEntry<TableCatalogEntry>(*client_con.context, client_catalog_name, "main", centralized_view_name,
	                                         OnEntryNotFound::RETURN_NULL, QueryErrorContext());
	client_con.Rollback();

	if (!centralized_view_catalog_entry) {
		throw ParserException("Centralized view not found: " + centralized_view_name);
	}
	centralized_view_name = "sidra_client." + centralized_view_name;
	centralized_table_name = server_db_name.substr(0, server_db_name.size() - 3) + "." + centralized_table_name;

	string protected_column_names;
	string join_names;
	string table_column_names;

	string extract_metadata = "WITH stats AS (\n"
	                          "\tSELECT sidra_window, sidra_ttl FROM sidra_parser.sidra_view_constraints\n"
	                          "\tWHERE view_name = '" +
	                          view_name +
	                          "'),\n"
	                          "current_window AS (\n"
	                          "\tSELECT sidra_window FROM sidra_parser.sidra_current_window\n"
	                          "\tWHERE view_name = 'sidra_staging_view_" +
	                          view_name +
	                          "'),\n"
	                          "threshold_window AS (\n"
	                          "\tSELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window\n"
	                          "\tFROM current_window cw, stats s)\n";

	string min_agg_query = "select sidra_min_agg from sidra_view_constraints where view_name = '" + view_name + "';";
	auto r = metadata_con.Query(min_agg_query);
	if (r->HasError()) {
		throw ParserException("Error while querying columns metadata: " + r->GetError());
	}
	if (r->RowCount() == 0) {
		throw ParserException("View metadata not found!");
	}
	auto minimum_aggregation = std::stoi(r->GetValue(0, 0).ToString());

	string view_query = "select query from sidra_tables where name = '" + view_name + "';";
	auto view_query_result = metadata_con.Query(view_query);
	if (view_query_result->HasError()) {
		throw ParserException("Error while querying view definition: " + view_query_result->GetError());
	}

	metadata_con.BeginTransaction();
	auto table_names = metadata_con.GetTableNames(view_query_result->GetValue(0, 0).ToString());
	metadata_con.Rollback();

	string in_table_names = "(";
	for (auto &table : table_names) {
		in_table_names += "'" + EscapeSingleQuotes(table) + "', ";
	}
	in_table_names = in_table_names.substr(0, in_table_names.size() - 2) + ")";

	auto protected_columns_query =
	    "select column_name from sidra_table_constraints where sidra_dimension = 1 and table_name in " +
	    in_table_names + ";";
	auto protected_columns = metadata_con.Query(protected_columns_query);
	if (protected_columns->HasError()) {
		throw ParserException("Error while querying protected columns: " + protected_columns->GetError());
	}
	for (size_t i = 0; i < protected_columns->RowCount(); i++) {
		auto column = protected_columns->GetValue(0, i).ToString();
		protected_column_names += column + ", ";
		join_names += "x." + column + " = y." + column + " \nand ";
	}

	protected_column_names += "sidra_window";
	join_names += "x.sidra_window = y.sidra_window \nand ";

	auto &centralized_view_entry = centralized_view_catalog_entry->Cast<TableCatalogEntry>();
	for (auto &column : centralized_view_entry.GetColumns().GetColumnNames()) {
		if (column != "action" && column != "generation" && column != "arrival") {
			table_column_names += column + ", ";
		}
	}
	table_column_names = table_column_names.substr(0, table_column_names.size() - 2);

	string attach_query;
	string attach_parser = "attach 'sidra_parser.db' as sidra_parser;\n\n";
	string attach_parser_read_only = "attach 'sidra_parser.db' as sidra_parser (read_only);\n\n";
	if (database == "duckdb") {
		attach_query = "attach '" + client_db_name + "' as sidra_client;\n\n";
	} else if (database == "postgres") {
		// TODO: extend config to support postgres credentials
		string postgres_credentials = "'dbname=sidra_client user=ubuntu password=test host=localhost'";
		attach_query = "attach if not exists " + postgres_credentials + " as sidra_client (type postgres);\n\n";
	}

	string detach_query = "detach sidra_client;\n\n";

	string update_query_1;
	if (minimum_aggregation > 1) {
		update_query_1 = UpdateWithMinimumAggregation(centralized_view_name, centralized_table_name, table_column_names,
		                                              join_names, protected_column_names, minimum_aggregation);
	} else {
		update_query_1 = extract_metadata + UpdateWithoutMinimumAggregation(centralized_view_name,
		                                                                    centralized_table_name, table_column_names);
	}

	string update_responsiveness = UpdateResponsiveness(view_name);
	string update_completeness;
	if (minimum_aggregation > 1) {
		update_completeness = attach_parser_read_only + extract_metadata.substr(0, extract_metadata.size() - 1) +
		                      UpdateCompleteness(view_name);
	} else {
		update_completeness =
		    "update sidra_centralized_view_" + view_name + " sidra_metadata_update\nset completeness = 100;\n\n";
	}
	string update_buffer_size;
	if (minimum_aggregation > 1) {
		update_buffer_size = UpdateBufferSize(view_name);
	} else {
		update_buffer_size =
		    "update sidra_centralized_view_" + view_name + " sidra_metadata_update\nset buffer_size = 0;\n\n";
	}
	string cleanup_expired_clients = CleanupExpiredClients(config);

	string delete_query_2;
	if (minimum_aggregation > 1) {
		delete_query_2 = extract_metadata + "delete from " + centralized_view_name +
		                 " where sidra_window <= (SELECT expired_window FROM threshold_window);\n\n";
	}

	string queries;
	if (database == "duckdb") {
		queries = attach_query + attach_parser + update_query_1 + detach_query + cleanup_expired_clients +
		          update_responsiveness + attach_query + update_completeness + delete_query_2 + update_buffer_size +
		          detach_query;
	} else if (database == "postgres") {
		queries = attach_query + attach_parser + update_query_1 + cleanup_expired_clients + update_responsiveness +
		          update_completeness + delete_query_2 + update_buffer_size;
	}

	bool append = flush_run > 0;
	ExecuteCommitLogAndWriteQueries(server_con, queries, file_name, view_name, append, flush_run, true);
	flush_run++;
}

} // namespace duckdb
