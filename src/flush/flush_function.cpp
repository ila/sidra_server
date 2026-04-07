#include "flush_function.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parser.hpp"
#include "server_debug.hpp"
#include "update_metrics.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

void FlushFunction(ClientContext &context, const FunctionParameters &parameters) {
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

	Connection server_con(*context.db);
	auto view_name = StringValue::Get(parameters.values[0]);

	// Look up CMV metadata
	auto cmv_check = server_con.Query("SELECT delta_sql, merge_template, data_table_name, source_view "
	                                  "FROM sidra_cmv_queries WHERE view_name = '" +
	                                  EscapeSingleQuotes(view_name) + "'");
	if (cmv_check->HasError() || cmv_check->RowCount() == 0) {
		// Check if staging view exists (user might be flushing a DMV without a CMV)
		auto staging_check = server_con.TableInfo("sidra_staging_view_" + view_name);
		if (staging_check) {
			throw ParserException("No centralized MV defined for '" + view_name +
			                      "'. Create a centralized MV (CREATE CENTRALIZED MATERIALIZED VIEW) first.");
		}
		throw ParserException("View '" + view_name + "' not found in SIDRA metadata.");
	}

	string cmv_delta_sql = cmv_check->GetValue(0, 0).ToString();
	string cmv_merge_template = cmv_check->GetValue(1, 0).ToString();
	string centralized_view = cmv_check->GetValue(2, 0).ToString();
	string source_views_str = cmv_check->GetValue(3, 0).ToString();
	auto source_list = StringUtil::Split(source_views_str, ',');

	// Inject TTL + arrival filters into staging view scans
	for (auto &sv : source_list) {
		StringUtil::Trim(sv);
		string staging_table = "sidra_staging_view_" + sv;

		auto vc = server_con.Query("SELECT sidra_window, sidra_ttl, last_refresh FROM sidra_view_constraints "
		                           "WHERE view_name = '" +
		                           EscapeSingleQuotes(sv) + "'");
		auto cw = server_con.Query("SELECT sidra_window FROM sidra_current_window WHERE view_name = '" +
		                           EscapeSingleQuotes(staging_table) + "'");

		if (vc->HasError() || vc->RowCount() == 0) {
			Printer::Print("Warning: no view constraints found for '" + sv + "' — filters not applied");
			continue;
		}
		if (cw->HasError() || cw->RowCount() == 0) {
			Printer::Print("Warning: no current window found for '" + staging_table + "' — TTL not applied");
		}

		auto window_size = vc->GetValue(0, 0).GetValue<int32_t>();
		auto ttl = vc->GetValue(1, 0).GetValue<int32_t>();
		auto last_refresh = vc->GetValue(2, 0).ToString();

		// Build WHERE clause: TTL filter + arrival filter
		string where_parts;
		if (!cw->HasError() && cw->RowCount() > 0 && window_size > 0) {
			auto current_window = cw->GetValue(0, 0).GetValue<int32_t>();
			int32_t expired_window = current_window - (ttl / window_size);
			where_parts += "sidra_window > " + to_string(expired_window);
		}
		if (last_refresh != "NULL" && !last_refresh.empty()) {
			if (!where_parts.empty()) {
				where_parts += " AND ";
			}
			where_parts += "arrival > '" + EscapeSingleQuotes(last_refresh) + "'::TIMESTAMPTZ";
		}

		if (!where_parts.empty()) {
			string filter = staging_table + " WHERE " + where_parts;
			cmv_delta_sql = StringUtil::Replace(cmv_delta_sql, staging_table + ")", filter + ")");
			SERVER_DEBUG_PRINT("[FLUSH] Injected filter for " + staging_table + ": " + where_parts);
		}
	}

	// Execute the CMV flush (delta query + MERGE)
	string cmv_flush_sql = "WITH ivm_cte AS (\n" + cmv_delta_sql + "\n)\n" + cmv_merge_template + ";";
	SERVER_DEBUG_PRINT("[FLUSH] SQL:\n" + cmv_flush_sql);

	server_con.BeginTransaction();
	auto cmv_r = server_con.Query(cmv_flush_sql);
	if (cmv_r->HasError()) {
		server_con.Rollback();
		throw ParserException("Flush failed for '" + view_name + "': " + cmv_r->GetError());
	}
	server_con.Commit();

	// Update metrics (read from staging, write to centralized)
	string first_staging = "sidra_staging_view_" + source_list[0];
	StringUtil::Trim(first_staging);

	auto resp_sql = UpdateResponsiveness(first_staging, centralized_view);
	auto resp_r = server_con.Query(resp_sql);
	if (resp_r->HasError()) {
		Printer::Print("Warning: responsiveness update failed: " + resp_r->GetError());
	}

	auto buf_sql = UpdateBufferSize(first_staging, centralized_view);
	auto buf_r = server_con.Query(buf_sql);
	if (buf_r->HasError()) {
		Printer::Print("Warning: buffer_size update failed: " + buf_r->GetError());
	}

	// Cleanup expired clients
	auto cleanup_sql = CleanupExpiredClients(config);
	server_con.Query(cleanup_sql);

	// Update last_refresh for all source views so next flush only reads new arrivals
	for (auto &sv : source_list) {
		StringUtil::Trim(sv);
		server_con.Query("UPDATE sidra_view_constraints SET last_refresh = now()::TIMESTAMP WHERE view_name = '" +
		                 EscapeSingleQuotes(sv) + "'");
	}

	SERVER_DEBUG_PRINT("[FLUSH] Completed for CMV: " + view_name);
}

} // namespace duckdb
