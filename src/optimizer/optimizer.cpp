#include "optimizer.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "server_debug.hpp"

#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

static void CheckQueryResult(const unique_ptr<MaterializedQueryResult> &result, const string &context) {
	if (result->HasError()) {
		Printer::Print("SIDRA DROP cleanup warning (" + context + "): " + result->GetError());
	}
}

void SIDRADropTableRule::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	if (!plan || plan->type != LogicalOperatorType::LOGICAL_DROP) {
		return;
	}

	auto &simple = plan->Cast<LogicalSimple>();
	if (simple.info->info_type != ParseInfoType::DROP_INFO) {
		return;
	}

	auto &drop_info = simple.info->Cast<DropInfo>();
	if (drop_info.type != CatalogType::TABLE_ENTRY) {
		return;
	}

	string table_name = drop_info.name;
	SERVER_DEBUG_PRINT("DROP TABLE detected for: " + table_name);

	// Clean up SIDRA metadata from the main DB
	try {
		auto &db = DatabaseInstance::GetDatabase(input.context);
		Connection con(db);

		auto check = con.Query("SELECT name FROM sidra_tables WHERE name = '" + EscapeSingleQuotes(table_name) + "'");
		if (check->HasError() || check->RowCount() == 0) {
			return;
		}

		CheckQueryResult(con.Query("DELETE FROM sidra_current_window WHERE view_name LIKE 'sidra_staging_view_" +
		                           EscapeSingleQuotes(table_name) + "'"),
		                 "current_window");
		CheckQueryResult(
		    con.Query("DELETE FROM sidra_view_constraints WHERE view_name = '" + EscapeSingleQuotes(table_name) + "'"),
		    "view_constraints");
		CheckQueryResult(con.Query("DELETE FROM sidra_table_constraints WHERE table_name = '" +
		                           EscapeSingleQuotes(table_name) + "'"),
		                 "table_constraints");
		CheckQueryResult(con.Query("DELETE FROM sidra_tables WHERE name = '" + EscapeSingleQuotes(table_name) + "'"),
		                 "tables");
		CheckQueryResult(con.Query("DELETE FROM sidra_tables WHERE name = 'sidra_staging_view_" +
		                           EscapeSingleQuotes(table_name) + "'"),
		                 "staging_view");
		CheckQueryResult(con.Query("DELETE FROM sidra_tables WHERE name = 'sidra_centralized_view_" +
		                           EscapeSingleQuotes(table_name) + "'"),
		                 "centralized_view");

		SERVER_DEBUG_PRINT("Cleaned up SIDRA metadata for: " + table_name);
	} catch (const std::exception &e) {
		Printer::Print("SIDRA DROP metadata cleanup failed for '" + table_name + "': " + e.what());
	}

	// Clean up the shadow DB
	try {
		string shadow_db_name = GetShadowDBName(input.context);
		DuckDB shadow(shadow_db_name);
		Connection shadow_con(shadow);
		auto r = shadow_con.Query("DROP TABLE IF EXISTS " + table_name);
		if (r->HasError()) {
			Printer::Print("SIDRA DROP shadow cleanup warning: " + r->GetError());
		}
		SERVER_DEBUG_PRINT("Dropped table from shadow DB: " + table_name);
	} catch (const std::exception &e) {
		Printer::Print("SIDRA DROP shadow cleanup failed for '" + table_name + "': " + e.what());
	}
}

} // namespace duckdb
