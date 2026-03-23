#include "optimizer.hpp"
#include "server_debug.hpp"

#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"

namespace duckdb {

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

	// Clean up SIDRA metadata for the dropped table by querying the metadata DB.
	// We use a try-catch because the metadata DB may not exist (e.g., in test environments).
	try {
		DuckDB metadata_db("sidra_parser.db");
		Connection con(metadata_db);

		// Remove from sidra_current_window (views referencing this table's staging view)
		con.Query("DELETE FROM sidra_current_window WHERE view_name LIKE 'sidra_staging_view_" + table_name + "';");

		// Remove from sidra_view_constraints
		con.Query("DELETE FROM sidra_view_constraints WHERE view_name = '" + table_name + "';");

		// Remove from sidra_table_constraints
		con.Query("DELETE FROM sidra_table_constraints WHERE table_name = '" + table_name + "';");

		// Remove from sidra_tables
		con.Query("DELETE FROM sidra_tables WHERE name = '" + table_name + "';");

		// Also remove derived entries (staging and centralized views)
		con.Query("DELETE FROM sidra_tables WHERE name = 'sidra_staging_view_" + table_name + "';");
		con.Query("DELETE FROM sidra_tables WHERE name = 'sidra_centralized_view_" + table_name + "';");
		con.Query("DELETE FROM sidra_current_window WHERE view_name = 'sidra_staging_view_" + table_name + "';");

		SERVER_DEBUG_PRINT("Cleaned up SIDRA metadata for dropped table: " + table_name);
	} catch (...) {
		// Metadata DB doesn't exist or table has no SIDRA metadata — nothing to clean up
		SERVER_DEBUG_PRINT("No SIDRA metadata to clean up for: " + table_name);
	}
}

} // namespace duckdb
