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

static string UpdateWithMinimumAggregation(string &staging_view, string &centralized_table, string &column_names,
                                           string &join_names, string &dimension_columns, int minimum_aggregation) {
	// Match handwritten script pattern: mark rows meeting min_agg threshold,
	// insert into centralized, delete marked from staging.
	// Also filter by expired window (fixes TODO #51).
	string query = "update " + staging_view + " x\nset action = 2 \nfrom (\n\tselect ";
	query += dimension_columns + ", ";
	query += "count(distinct client_id)\n\t";
	query += "from " + staging_view + " \n\t";
	query += "where sidra_window > (select expired_window from threshold_window)\n\t";
	query += "group by " + dimension_columns + "\n\t";
	query += "having count(distinct client_id) >= " + std::to_string(minimum_aggregation);
	query += ") y\n";
	query += "where " + join_names.substr(0, join_names.size() - 6) + ";\n\n";

	query += "insert into " + centralized_table + " by name\nselect " + column_names + " \nfrom " + staging_view +
	         " \nwhere action = 2;\n\n";
	query += "delete from " + staging_view + " \nwhere action = 2;\n\n";

	return query;
}

static string UpdateWithoutMinimumAggregation(string &staging_view, string &centralized_table, string &column_names,
                                              const string &extract_metadata) {
	// Insert non-expired rows that arrived AFTER last flush (avoid re-processing)
	string query = "insert into " + centralized_table + " by name \n";
	query += "select " + column_names + "\n";
	query += "from " + staging_view + "\n";
	query += "where sidra_window > (select expired_window from threshold_window)\n";
	query += "and arrival > (select last_refresh from stats);\n\n";

	// Delete expired rows from staging (needs its own CTE since it's a separate statement)
	query += extract_metadata;
	query += "delete from " + staging_view + "\n";
	query += "where sidra_window <= (select expired_window from threshold_window);\n\n";

	return query;
}

void FlushFunction(ClientContext &context, const FunctionParameters &parameters) {
	// TODO: implement client deletes (opt out)
	// TODO: add index logic for upsert

	auto database = StringValue::Get(parameters.values[1]);
	if (database != "duckdb" && database != "postgres") {
		throw ParserException("Invalid database type: " + database + " - only duckdb and postgres are supported!");
	}

	string config_path;
	Value config_path_val;
	if (context.TryGetCurrentSetting("sidra_config_path", config_path_val)) {
		config_path = config_path_val.ToString();
	}
	string config_file = "server.config";
	auto config = ParseConfig(config_path, config_file);

	// Use the caller's database connection — opening a separate DuckDB instance
	// on the same file causes WAL isolation (writes aren't visible to the caller).
	Connection server_con(*context.db);
	string server_db_name = context.db->config.options.database_path;

	auto view_name = StringValue::Get(parameters.values[0]);

	// Check if this is a CMV (centralized materialized view) — flush via delta_sql + merge_template
	auto cmv_check = server_con.Query("SELECT delta_sql, merge_template, data_table_name FROM sidra_cmv_queries "
	                                  "WHERE view_name = '" +
	                                  EscapeSingleQuotes(view_name) + "'");
	if (!cmv_check->HasError() && cmv_check->RowCount() > 0) {
		string cmv_delta_sql = cmv_check->GetValue(0, 0).ToString();
		string cmv_merge_template = cmv_check->GetValue(1, 0).ToString();
		string cmv_flush_sql = "WITH ivm_cte AS (\n" + cmv_delta_sql + "\n)\n" + cmv_merge_template + ";";
		SERVER_DEBUG_PRINT("[CMV FLUSH] " + view_name + ":\n" + cmv_flush_sql);
		server_con.BeginTransaction();
		auto cmv_r = server_con.Query(cmv_flush_sql);
		if (cmv_r->HasError()) {
			server_con.Rollback();
			throw ParserException("CMV flush failed for '" + view_name + "': " + cmv_r->GetError());
		}
		server_con.Commit();
		return;
	}

	auto staging_view = "sidra_staging_view_" + view_name;
	auto centralized_table = "sidra_centralized_view_" + view_name;

	LocalFileSystem fs;
	string file_name = staging_view + "_flush.sql";

	// If a pre-compiled flush script exists, execute it directly
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

	SERVER_DEBUG_PRINT("[FLUSH] db=" + server_db_name + " staging=" + staging_view +
	                   " centralized=" + centralized_table);
	auto staging_count = server_con.Query("SELECT COUNT(*) FROM " + staging_view);
	if (!staging_count->HasError()) {
		SERVER_DEBUG_PRINT("[FLUSH] staging rows: " + staging_count->GetValue(0, 0).ToString());
	}

	// Look up the staging view in the server DB
	auto staging_info = server_con.TableInfo(staging_view);
	if (!staging_info) {
		throw ParserException("Staging view not found: " + staging_view);
	}

	string dimension_columns;
	string join_names;
	string table_column_names;

	// CTE to extract TTL metadata (metadata is in the server DB, not an external sidra_parser.db)
	string extract_metadata = "WITH stats AS (\n"
	                          "\tSELECT sidra_window, sidra_ttl, last_refresh FROM sidra_view_constraints\n"
	                          "\tWHERE view_name = '" +
	                          EscapeSingleQuotes(view_name) +
	                          "'),\n"
	                          "current_window AS (\n"
	                          "\tSELECT sidra_window FROM sidra_current_window\n"
	                          "\tWHERE view_name = 'sidra_staging_view_" +
	                          EscapeSingleQuotes(view_name) +
	                          "'),\n"
	                          "threshold_window AS (\n"
	                          "\tSELECT cw.sidra_window - (s.sidra_ttl / s.sidra_window) AS expired_window\n"
	                          "\tFROM current_window cw, stats s)\n";

	// Query metadata from the server DB (not sidra_parser.db)
	string min_agg_query =
	    "select sidra_min_agg from sidra_view_constraints where view_name = '" + EscapeSingleQuotes(view_name) + "';";
	auto r = server_con.Query(min_agg_query);
	if (r->HasError()) {
		throw ParserException("Error while querying view metadata: " + r->GetError());
	}
	if (r->RowCount() == 0) {
		throw ParserException("View metadata not found for: " + view_name);
	}
	auto minimum_aggregation = std::stoi(r->GetValue(0, 0).ToString());

	// Get the view query to find referenced tables
	string view_query_sql = "select query from sidra_tables where name = '" + EscapeSingleQuotes(view_name) + "';";
	auto view_query_result = server_con.Query(view_query_sql);
	if (view_query_result->HasError()) {
		throw ParserException("Error while querying view definition: " + view_query_result->GetError());
	}

	// Find dimension columns: columns in the staging view that are DIMENSION-annotated
	// AND actually present in the view's output (not all base table dimensions)
	unordered_set<string> staging_columns;
	for (auto &column : staging_info->columns) {
		auto col_name = column.GetName();
		if (col_name != "action" && col_name != "generation" && col_name != "arrival" && col_name != "sidra_window" &&
		    col_name != "client_id") {
			staging_columns.insert(col_name);
		}
	}

	server_con.BeginTransaction();
	auto table_names = server_con.GetTableNames(view_query_result->GetValue(0, 0).ToString());
	server_con.Rollback();

	string in_table_names = "(";
	for (auto &table : table_names) {
		in_table_names += "'" + EscapeSingleQuotes(table) + "', ";
	}
	in_table_names = in_table_names.substr(0, in_table_names.size() - 2) + ")";

	auto dim_columns_query = "select column_name from sidra_table_constraints where sidra_dimension = 1 "
	                         "and table_name in " +
	                         in_table_names + ";";
	auto dim_columns = server_con.Query(dim_columns_query);
	if (dim_columns->HasError()) {
		throw ParserException("Error while querying dimension columns: " + dim_columns->GetError());
	}
	for (size_t i = 0; i < dim_columns->RowCount(); i++) {
		auto column = dim_columns->GetValue(0, i).ToString();
		// Only include dimension columns that are actually in the staging view
		if (staging_columns.count(column)) {
			dimension_columns += column + ", ";
			join_names += "x." + column + " = y." + column + " \nand ";
		}
	}

	dimension_columns += "sidra_window";
	join_names += "x.sidra_window = y.sidra_window \nand ";

	// Get column names from the staging view (excluding metadata columns)
	for (auto &column : staging_info->columns) {
		auto col_name = column.GetName();
		if (col_name != "action" && col_name != "generation" && col_name != "arrival") {
			table_column_names += col_name + ", ";
		}
	}
	table_column_names = table_column_names.substr(0, table_column_names.size() - 2);

	// For DuckDB mode, everything is in one DB — no attach needed.
	// For Postgres mode, attach the external DB.
	string attach_query;
	string detach_query;
	if (database == "postgres") {
		// TODO: extend config to support postgres credentials
		string postgres_credentials = "'dbname=sidra_client user=ubuntu password=test host=localhost'";
		attach_query = "attach if not exists " + postgres_credentials + " as sidra_client (type postgres);\n\n";
		detach_query = "detach sidra_client;\n\n";
	}

	// Build the main data movement query
	string update_query;
	if (minimum_aggregation > 1) {
		update_query =
		    extract_metadata + UpdateWithMinimumAggregation(staging_view, centralized_table, table_column_names,
		                                                    join_names, dimension_columns, minimum_aggregation);
	} else {
		update_query = extract_metadata + UpdateWithoutMinimumAggregation(staging_view, centralized_table,
		                                                                  table_column_names, extract_metadata);
	}

	// Build metric updates
	string update_responsiveness = UpdateResponsiveness(view_name);
	string update_completeness;
	if (minimum_aggregation > 1) {
		update_completeness = extract_metadata.substr(0, extract_metadata.size() - 1) + UpdateCompleteness(view_name);
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
	string cleanup_clients = CleanupExpiredClients(config);

	// TTL cleanup in staging (only for min_agg path — non-min-agg already deletes expired)
	string ttl_cleanup;
	if (minimum_aggregation > 1) {
		ttl_cleanup = extract_metadata + "delete from " + staging_view +
		              "\nwhere sidra_window <= (SELECT expired_window FROM threshold_window);\n\n";
	}

	// Zero aggregate cleanup (matching handwritten scripts)
	string zero_cleanup = "delete from " + centralized_table + "\nwhere " +
	                      table_column_names.substr(table_column_names.find_last_of(',') + 2) + " = 0;\n\n";

	// Update last_refresh timestamp so next flush only processes new arrivals
	string update_last_refresh =
	    "UPDATE sidra_view_constraints SET last_refresh = now()::TIMESTAMP WHERE view_name = '" +
	    EscapeSingleQuotes(view_name) + "';\n\n";

	// Assemble the complete flush script
	string queries;
	if (database == "duckdb") {
		queries = attach_query + update_query + detach_query + cleanup_clients + update_responsiveness + attach_query +
		          update_completeness + ttl_cleanup + update_buffer_size + zero_cleanup + update_last_refresh +
		          detach_query;
	} else if (database == "postgres") {
		queries = attach_query + update_query + cleanup_clients + update_responsiveness + update_completeness +
		          ttl_cleanup + update_buffer_size + zero_cleanup + update_last_refresh;
	}

	bool append = flush_run > 0;
	ExecuteCommitLogAndWriteQueries(server_con, queries, file_name, view_name, append, flush_run, true);
	flush_run++;

	// Phase 3: Update dependent CMVs
	// delta_sql reads from centralized tables (no timestamps), merge_template does the upsert
	auto cmv_result =
	    server_con.Query("SELECT view_name, delta_sql, merge_template, data_table_name "
	                     "FROM sidra_cmv_queries WHERE source_view = '" +
	                     EscapeSingleQuotes(view_name) + "' OR source_view LIKE '" + EscapeSingleQuotes(view_name) +
	                     ",%' OR source_view LIKE '%," + EscapeSingleQuotes(view_name) + "' OR source_view LIKE '%," +
	                     EscapeSingleQuotes(view_name) + ",%'");
	if (cmv_result->HasError() || cmv_result->RowCount() == 0) {
		return; // No dependent CMVs
	}

	for (idx_t i = 0; i < cmv_result->RowCount(); i++) {
		string cmv_name = cmv_result->GetValue(0, i).ToString();
		string cmv_delta_sql = cmv_result->GetValue(1, i).ToString();
		string cmv_merge_template = cmv_result->GetValue(2, i).ToString();
		string cmv_data_table = cmv_result->GetValue(3, i).ToString();

		// Assemble fresh SQL at flush time (no stale timestamps)
		string cmv_flush_sql = "WITH ivm_cte AS (\n" + cmv_delta_sql + "\n)\n" + cmv_merge_template + ";";

		SERVER_DEBUG_PRINT("[CMV FLUSH] Executing for: " + cmv_name);
		SERVER_DEBUG_PRINT("[CMV FLUSH] SQL:\n" + cmv_flush_sql);

		server_con.BeginTransaction();
		auto cmv_r = server_con.Query(cmv_flush_sql);
		if (cmv_r->HasError()) {
			server_con.Rollback();
			SERVER_DEBUG_PRINT("[CMV FLUSH] ERROR: " + cmv_r->GetError());
			Printer::Print("Warning: CMV update failed for " + cmv_name + ": " + cmv_r->GetError());
		} else {
			server_con.Commit();
			server_con.Query("UPDATE sidra_cmv_queries SET last_flush = now() WHERE view_name = '" +
			                 EscapeSingleQuotes(cmv_name) + "'");
			SERVER_DEBUG_PRINT("[CMV FLUSH] Successfully updated CMV: " + cmv_name);
		}
	}
}

} // namespace duckdb
