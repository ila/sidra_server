#include "update_metrics.hpp"
#include "common.hpp"

namespace duckdb {

string UpdateResponsiveness(string &view_name) {
	// Metadata tables (sidra_clients) are in the main DB — no attach needed
	string query = "WITH distinct_clients_per_window AS (\n"
	               "\tSELECT sidra_window, COUNT(DISTINCT client_id) AS window_client_count\n"
	               "\tFROM sidra_centralized_view_" +
	               view_name +
	               "\n"
	               "\tGROUP BY sidra_window),\n"
	               "total_clients AS (\n"
	               "\tSELECT COUNT(DISTINCT id) AS total_client_count FROM sidra_clients),\n"
	               "percentages AS (\n"
	               "\tSELECT d.sidra_window, "
	               "LEAST((d.window_client_count::decimal / t.total_client_count) * 100, 100) AS percentage\n"
	               "\tFROM distinct_clients_per_window d, total_clients t)\n"
	               "UPDATE sidra_centralized_view_" +
	               view_name +
	               " sidra_metadata_update\n"
	               "SET responsiveness = p.percentage\n"
	               "FROM percentages p\n"
	               "WHERE sidra_metadata_update.sidra_window = p.sidra_window;\n\n";

	return query;
}

string UpdateCompleteness(string &view_name) {
	// Continues a CTE chain (starts with ",\n" to join with extract_metadata CTE)
	string query = ",\n"
	               "to_discard AS (\n"
	               "\tSELECT sidra_window, COUNT(*) AS discarded_count\n"
	               "\tFROM sidra_staging_view_" +
	               view_name +
	               "\n"
	               "\tWHERE sidra_window <= (SELECT expired_window FROM threshold_window)\n"
	               "\tGROUP BY sidra_window),\n"
	               "to_keep AS (\n"
	               "\tSELECT sidra_window, COUNT(*) AS kept_count\n"
	               "\tFROM sidra_centralized_view_" +
	               view_name +
	               "\n"
	               "\tGROUP BY sidra_window),\n"
	               "combined AS (\n"
	               "\tSELECT\n"
	               "\t\tCOALESCE(d.sidra_window, k.sidra_window) AS sidra_window,\n"
	               "\t\tCOALESCE(d.discarded_count, 0) AS discarded,\n"
	               "\t\tCOALESCE(k.kept_count, 0) AS kept\n"
	               "\tFROM to_discard d\n"
	               "\tFULL OUTER JOIN to_keep k\n"
	               "\tON d.sidra_window = k.sidra_window),\n"
	               "discard_stats AS (\n"
	               "\tSELECT sidra_window, discarded, kept, \n"
	               "\t\tCASE WHEN (discarded + kept) > 0 THEN (kept::decimal / (discarded + kept)) * 100 "
	               "ELSE 100 END AS discard_percentage\n"
	               "\tFROM combined)\n"
	               "UPDATE sidra_centralized_view_" +
	               view_name +
	               " sidra_metadata_update\n"
	               "SET completeness = ds.discard_percentage\n"
	               "FROM discard_stats ds\n"
	               "WHERE sidra_metadata_update.sidra_window = ds.sidra_window;\n\n";

	return query;
}

string UpdateBufferSize(string &view_name) {
	string query = "WITH buffer_counts AS (\n"
	               "\tSELECT sidra_window, COUNT(*) AS buffer_count\n"
	               "\tFROM sidra_staging_view_" +
	               view_name +
	               "\n"
	               "\tGROUP BY sidra_window),\n"
	               "centralized_counts AS (\n"
	               "\tSELECT sidra_window, COUNT(*) AS centralized_count\n"
	               "\tFROM sidra_centralized_view_" +
	               view_name +
	               "\n"
	               "\tGROUP BY sidra_window),\n"
	               "combined AS (\n"
	               "\tSELECT\n"
	               "\tCOALESCE(b.sidra_window, c.sidra_window) AS sidra_window,\n"
	               "\tCOALESCE(b.buffer_count, 0) AS buffer,\n"
	               "\tCOALESCE(c.centralized_count, 0) AS centralized\n"
	               "\tFROM buffer_counts b\n"
	               "\tFULL OUTER JOIN centralized_counts c\n"
	               "\tON b.sidra_window = c.sidra_window),\n"
	               "buffer_stats AS (\n"
	               "\tSELECT sidra_window, buffer, centralized,\n"
	               "\t\tCASE WHEN (buffer + centralized) > 0 THEN (buffer::decimal / (buffer + centralized)) * "
	               "100 ELSE 0 END AS buffer_percentage\n"
	               "\tFROM combined) "
	               "UPDATE sidra_centralized_view_" +
	               view_name +
	               " sidra_metadata_update\n"
	               "SET buffer_size = bs.buffer_percentage\n"
	               "FROM buffer_stats bs\n"
	               "WHERE sidra_metadata_update.sidra_window = bs.sidra_window;\n\n";

	return query;
}

string CleanupExpiredClients(std::unordered_map<string, string> &config) {
	int keep_alive_days = std::stoi(config["keep_alive_clients_days"]);
	// sidra_clients is in the main DB — no sidra_parser prefix needed
	string query = "DELETE FROM sidra_clients WHERE last_update < current_date - interval " +
	               std::to_string(keep_alive_days) + " day;\n\n";
	return query;
}

} // namespace duckdb
