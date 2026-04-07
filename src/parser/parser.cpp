#include "parser.hpp"
#include "common.hpp"
#include "compiler_utils.hpp"
#include "parser_helper.hpp"
#include "parser_helpers.hpp"
#include "server_debug.hpp"

#include "logical_plan_to_sql.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/local_file_system.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
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
// Plan manipulation helpers for CMV compilation
//===--------------------------------------------------------------------===//

//! Walk the plan tree and replace GET nodes that reference old_table with new_table
static void RedirectGetNodes(unique_ptr<LogicalOperator> &op, const string &old_table, const string &new_table,
                             ClientContext &context) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.GetTable().get() && get.GetTable()->name == old_table) {
			auto catalog_name = get.GetTable()->catalog.GetName();
			auto schema_name = get.GetTable()->schema.name;
			QueryErrorContext error_context;
			auto opt_entry = Catalog::GetEntry<TableCatalogEntry>(context, catalog_name, schema_name, new_table,
			                                                      OnEntryNotFound::THROW_EXCEPTION, error_context);
			auto &table_entry = opt_entry->Cast<TableCatalogEntry>();

			unique_ptr<FunctionData> bind_data;
			auto scan_function = table_entry.GetScanFunction(context, bind_data);

			idx_t max_oid = 0;
			for (auto &col : table_entry.GetColumns().Logical()) {
				if (col.Oid() > max_oid) {
					max_oid = col.Oid();
				}
			}
			vector<LogicalType> return_types(max_oid + 1, LogicalType::ANY);
			vector<string> return_names(max_oid + 1, "");
			for (auto &col : table_entry.GetColumns().Logical()) {
				return_types[col.Oid()] = col.Type();
				return_names[col.Oid()] = col.Name();
			}

			auto new_get = make_uniq<LogicalGet>(get.table_index, scan_function, std::move(bind_data),
			                                     std::move(return_types), std::move(return_names));
			new_get->SetColumnIds(std::move(get.GetMutableColumnIds()));
			new_get->projection_ids = std::move(get.projection_ids);
			new_get->table_filters = std::move(get.table_filters);

			SERVER_DEBUG_PRINT("[PLAN] Redirected GET: " + old_table + " -> " + new_table);
			op = std::move(new_get);
			return;
		}
	}
	for (auto &child : op->children) {
		RedirectGetNodes(child, old_table, new_table, context);
	}
}

//! Add a named column to the GET node's output and return its OUTPUT binding position.
static ColumnBinding AddColumnToGet(unique_ptr<LogicalOperator> &op, const string &column_name) {
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		idx_t schema_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < get.names.size(); i++) {
			if (get.names[i] == column_name) {
				schema_idx = i;
				break;
			}
		}
		if (schema_idx == DConstants::INVALID_INDEX) {
			return ColumnBinding();
		}

		// Check if already in column_ids
		auto &col_ids = get.GetMutableColumnIds();
		for (idx_t i = 0; i < col_ids.size(); i++) {
			if (!col_ids[i].IsRowIdColumn() && col_ids[i].GetPrimaryIndex() == schema_idx) {
				// Already present — return its output position
				idx_t output_pos = i;
				if (!get.projection_ids.empty()) {
					// With projection_ids, find which output maps to this column_ids entry
					for (idx_t j = 0; j < get.projection_ids.size(); j++) {
						if (get.projection_ids[j] == i) {
							output_pos = j;
							break;
						}
					}
				}
				return ColumnBinding(get.table_index, output_pos);
			}
		}

		// Add client_id to column_ids
		idx_t new_col_ids_pos = col_ids.size();
		col_ids.push_back(ColumnIndex(schema_idx));

		// If projection_ids is used, add a mapping for the new column
		idx_t output_pos = new_col_ids_pos;
		if (!get.projection_ids.empty()) {
			output_pos = get.projection_ids.size();
			get.projection_ids.push_back(new_col_ids_pos);
		}

		SERVER_DEBUG_PRINT("[PLAN] Added " + column_name + " to GET output at position " + to_string(output_pos));
		return ColumnBinding(get.table_index, output_pos);
	}
	for (auto &child : op->children) {
		auto result = AddColumnToGet(child, column_name);
		if (result.table_index != DConstants::INVALID_INDEX) {
			return result;
		}
	}
	return ColumnBinding();
}

static ColumnBinding AddClientIdToGet(unique_ptr<LogicalOperator> &op) {
	return AddColumnToGet(op, "client_id");
}

//! Inject sidra_window as a GROUP BY column in the AGGREGATE node.
//! Returns the ColumnBinding for sidra_window in the AGGREGATE's group output.
static ColumnBinding InjectWindowGroupBy(unique_ptr<LogicalOperator> &op, ColumnBinding window_binding) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		auto window_ref = make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, window_binding);
		idx_t group_idx = agg.groups.size();
		agg.groups.push_back(std::move(window_ref));
		SERVER_DEBUG_PRINT("[PLAN] Added sidra_window to GROUP BY at index " + to_string(group_idx));
		// The group output binding: group_index + position
		return ColumnBinding(agg.group_index, group_idx);
	}
	for (auto &child : op->children) {
		auto result = InjectWindowGroupBy(child, window_binding);
		if (result.table_index != DConstants::INVALID_INDEX) {
			return result;
		}
	}
	return ColumnBinding();
}

//! Inject sidra_window into the top-level PROJECTION node.
static void InjectWindowProjection(unique_ptr<LogicalOperator> &op, ColumnBinding agg_window_binding) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto window_ref = make_uniq<BoundColumnRefExpression>("sidra_window", LogicalType::INTEGER, agg_window_binding);
		op->expressions.push_back(std::move(window_ref));
		SERVER_DEBUG_PRINT("[PLAN] Added sidra_window to PROJECTION");
		return;
	}
	for (auto &child : op->children) {
		InjectWindowProjection(child, agg_window_binding);
	}
}

//! Inject COUNT(DISTINCT client_id) >= min_agg as a HAVING filter into the AGGREGATE node.
//! client_id_binding is the ColumnBinding from the GET node's output.
static void InjectMinAggHaving(unique_ptr<LogicalOperator> &op, unique_ptr<LogicalOperator> &root, int min_agg,
                               ColumnBinding client_id_binding, ClientContext &context) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();

		// Create BoundColumnRef for client_id using its GET output binding
		auto client_id_ref = make_uniq<BoundColumnRefExpression>(LogicalType::UBIGINT, client_id_binding);

		// Create COUNT(DISTINCT client_id) aggregate expression
		vector<unique_ptr<Expression>> count_children;
		count_children.push_back(std::move(client_id_ref));
		auto count_functions = CountFun::GetFunctions();
		auto count_func = count_functions.GetFunctionByArguments(context, {LogicalType::UBIGINT});
		auto count_distinct = make_uniq<BoundAggregateExpression>(count_func, std::move(count_children), nullptr,
		                                                          nullptr, AggregateType::DISTINCT);

		agg.expressions.push_back(std::move(count_distinct));
		idx_t count_agg_idx = agg.expressions.size() - 1;

		// Create HAVING filter: COUNT(DISTINCT client_id) >= min_agg
		auto having_ref =
		    make_uniq<BoundColumnRefExpression>(LogicalType::BIGINT, ColumnBinding(agg.aggregate_index, count_agg_idx));
		auto threshold = make_uniq<BoundConstantExpression>(Value::BIGINT(min_agg));
		auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHANOREQUALTO,
		                                                       std::move(having_ref), std::move(threshold));

		// Insert FILTER node between aggregate's parent and the aggregate
		std::function<bool(unique_ptr<LogicalOperator> &)> insert_filter;
		insert_filter = [&](unique_ptr<LogicalOperator> &parent) -> bool {
			for (auto &child : parent->children) {
				if (child.get() == &agg) {
					auto filter = make_uniq<LogicalFilter>();
					filter->expressions.push_back(std::move(comparison));
					filter->children.push_back(std::move(child));
					child = std::move(filter);
					return true;
				}
				if (insert_filter(child)) {
					return true;
				}
			}
			return false;
		};
		if (!insert_filter(root)) {
			SERVER_DEBUG_PRINT("[PLAN] WARNING: Could not insert HAVING filter");
		} else {
			SERVER_DEBUG_PRINT("[PLAN] Injected HAVING COUNT(DISTINCT client_id) >= " + to_string(min_agg));
		}
		return;
	}
	for (auto &child : op->children) {
		InjectMinAggHaving(child, root, min_agg, client_id_binding, context);
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

static vector<string> CompileViewCreation(Connection &shadow_con, SIDRAParseData &data, const string &shadow_db_name) {
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
		// Compile CMV: OpenIVM for MERGE template, plan traversal + LPTS for delta SQL

		// Step 1: Load OpenIVM, inject sidra_window into query plan, compile MV
		auto load_r = shadow_con.Query("LOAD './openivm.duckdb_extension'");
		if (load_r->HasError()) {
			throw ParserException("Failed to load OpenIVM extension: " + load_r->GetError());
		}

		// Inject sidra_window into the CMV query so OpenIVM groups by window natively.
		// TODO: do this at the plan level once LPTS supports CTE_SCAN round-trip.
		string cmv_query = view_query;
		StringUtil::Trim(cmv_query);
		if (!cmv_query.empty() && cmv_query.back() == ';') {
			cmv_query.pop_back();
			StringUtil::Trim(cmv_query);
		}
		if (!cmv_query.empty() && cmv_query.front() == '(' && cmv_query.back() == ')') {
			cmv_query = cmv_query.substr(1, cmv_query.size() - 2);
			StringUtil::Trim(cmv_query);
		}

		// Find the source DMVs and redirect to staging views (which have sidra_window)
		shadow_con.BeginTransaction();
		auto src_tables = shadow_con.GetTableNames(cmv_query);
		shadow_con.Rollback();
		string first_alias;
		for (auto &t : src_tables) {
			auto staging_check = shadow_con.TableInfo("sidra_staging_view_" + t);
			if (staging_check) {
				// Find if this table has an alias in the query (e.g., "daily_steps_user a")
				string lower_q = StringUtil::Lower(cmv_query);
				string lower_t = StringUtil::Lower(t);
				auto pos = lower_q.find(lower_t);
				if (pos != string::npos) {
					// Check for alias after table name: "table_name <alias>"
					idx_t after = pos + t.size();
					if (after < cmv_query.size() && cmv_query[after] == ' ') {
						idx_t alias_start = after + 1;
						idx_t alias_end = alias_start;
						while (alias_end < cmv_query.size() && cmv_query[alias_end] != ' ' &&
						       cmv_query[alias_end] != ',' && cmv_query[alias_end] != ')') {
							alias_end++;
						}
						string maybe_alias = StringUtil::Lower(cmv_query.substr(alias_start, alias_end - alias_start));
						// Skip SQL keywords that aren't aliases
						if (maybe_alias != "join" && maybe_alias != "on" && maybe_alias != "where" &&
						    maybe_alias != "group" && maybe_alias != "order" && maybe_alias != "having" &&
						    maybe_alias != "limit" && maybe_alias != "inner" && maybe_alias != "left" &&
						    maybe_alias != "right" && maybe_alias != "full" && maybe_alias != "cross" &&
						    !maybe_alias.empty() && first_alias.empty()) {
							first_alias = cmv_query.substr(alias_start, alias_end - alias_start);
						}
					}
				}
				cmv_query = StringUtil::Replace(cmv_query, t, "sidra_staging_view_" + t);
			}
		}

		// Inject sidra_window into SELECT and GROUP BY
		string window_col = first_alias.empty() ? "sidra_window" : first_alias + ".sidra_window";
		{
			string lower = StringUtil::Lower(cmv_query);
			auto select_pos = lower.find("select ");
			auto group_pos = lower.find("group by ");
			if (select_pos != string::npos && group_pos != string::npos) {
				// Insert GROUP BY first (higher position)
				cmv_query.insert(group_pos + 9, window_col + ", ");
				// Then SELECT
				cmv_query.insert(select_pos + 7, window_col + ", ");
			}
		}
		SERVER_DEBUG_PRINT("[CMV] Modified query:\n" + cmv_query);

		string create_mv = "CREATE MATERIALIZED VIEW " + view_name + " AS " + cmv_query;
		auto mv_r = shadow_con.Query(create_mv);
		if (mv_r->HasError()) {
			throw ParserException("Error compiling centralized MV: " + mv_r->GetError());
		}
		SERVER_DEBUG_PRINT("[CMV] OpenIVM compiled MV in shadow DB");

		// Step 2: Extract OpenIVM metadata
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

		string ivm_data_table = "_ivm_data_" + view_name;
		auto data_info = shadow_con.TableInfo(ivm_data_table);
		if (!data_info) {
			throw ParserException("OpenIVM data table not found: " + ivm_data_table);
		}

		// Find ALL source views (DMVs referenced by CMV query)
		shadow_con.BeginTransaction();
		auto source_tables = shadow_con.GetTableNames(view_query);
		shadow_con.Rollback();
		vector<string> source_views;
		for (auto &t : source_tables) {
			// Check if this table has a staging view (i.e., it's a DMV)
			auto staging_check = shadow_con.TableInfo("sidra_staging_view_" + t);
			if (staging_check) {
				source_views.push_back(t);
			}
		}
		if (source_views.empty()) {
			throw ParserException("Could not determine source views for CMV: " + view_name);
		}
		// Comma-separated list of source views for metadata storage
		string source_views_str;
		for (size_t i = 0; i < source_views.size(); i++) {
			if (i > 0) {
				source_views_str += ",";
			}
			source_views_str += source_views[i];
		}
		// Use first source for backward compat in single-source case
		string staging_table = "sidra_staging_view_" + source_views[0];

		// Step 3: Plan the CMV query (already has sidra_window + staging redirects), call LPTS
		string clean_query = cmv_query;

		// Plan in shadow DB where source table exists
		duckdb::Parser query_parser;
		query_parser.ParseQuery(clean_query);
		shadow_con.BeginTransaction();
		Planner planner(*shadow_con.context);
		planner.CreatePlan(std::move(query_parser.statements[0]));
		auto plan = std::move(planner.plan);

		// Walk plan: replace GET(source_view) → GET(staging_view) for ALL source views
		// CMVs read from staging views (SMVs) per the paper (Eq. 10), apply min_agg,
		// and write to the centralized view (CMV data table)
		for (auto &sv : source_views) {
			RedirectGetNodes(plan, sv, "sidra_staging_view_" + sv, *shadow_con.context);
		}

		// sidra_window and metric columns are added via ALTER TABLE after CMV creation

		// Inject HAVING COUNT(DISTINCT client_id) >= min_agg at the plan level
		if (vc.min_agg > 0) {
			auto client_id_binding = AddClientIdToGet(plan);
			if (client_id_binding.table_index != DConstants::INVALID_INDEX) {
				InjectMinAggHaving(plan, plan, static_cast<int>(vc.min_agg), client_id_binding, *shadow_con.context);
			} else {
				throw ParserException("MINIMUM AGGREGATION requires client_id column in staging view, "
				                      "but it was not found for CMV '" +
				                      view_name + "'");
			}
		}

		// Convert modified plan to SQL via LPTS, passing planner.names for correct output aliases
		LogicalPlanToSql lpts(*shadow_con.context, plan, planner.names);
		auto cte_list = lpts.LogicalPlanToCteList();
		string delta_sql = LogicalPlanToSql::CteListToSql(cte_list);
		StringUtil::Trim(delta_sql);
		if (!delta_sql.empty() && delta_sql.back() == ';') {
			delta_sql.pop_back();
		}
		// Strip shadow DB catalog prefix (LPTS generates fully qualified names)
		string shadow_catalog = shadow_db_name;
		if (shadow_catalog.size() > 3 && shadow_catalog.substr(shadow_catalog.size() - 3) == ".db") {
			shadow_catalog = shadow_catalog.substr(0, shadow_catalog.size() - 3);
		}
		delta_sql = StringUtil::Replace(delta_sql, shadow_catalog + ".main.", "");
		shadow_con.Rollback();
		SERVER_DEBUG_PRINT("[CMV] LPTS delta SQL:\n" + delta_sql);

		// Step 4: Get upsert template from OpenIVM
		// The MV was already compiled in Step 1. Now run PRAGMA ivm to get the upsert SQL.
		string ivm_files_dir = "/tmp/sidra_ivm_compile";
		LocalFileSystem local_fs;
		local_fs.CreateDirectory(ivm_files_dir);
		shadow_con.Query("SET ivm_files_path = '" + ivm_files_dir + "'");
		auto ivm_pragma_result = shadow_con.Query("PRAGMA ivm('" + EscapeSingleQuotes(view_name) + "')");
		if (ivm_pragma_result->HasError()) {
			throw ParserException("PRAGMA ivm failed for CMV '" + view_name + "': " + ivm_pragma_result->GetError());
		}

		string ivm_file = ivm_files_dir + "/ivm_upsert_queries_" + view_name + ".sql";
		string compiled_ivm;
		{
			std::ifstream f(ivm_file);
			if (f.is_open()) {
				std::stringstream buf;
				buf << f.rdbuf();
				compiled_ivm = buf.str();
			}
		}
		SERVER_DEBUG_PRINT("[CMV] OpenIVM compiled:\n" + compiled_ivm);

		// Extract the upsert portion (INSERT OR REPLACE INTO or MERGE INTO)
		string upsert_portion;
		for (auto &marker : {"insert or replace into", "INSERT OR REPLACE INTO", "MERGE INTO", "merge into"}) {
			auto pos = compiled_ivm.find(marker);
			if (pos != string::npos) {
				auto end = compiled_ivm.find(';', pos);
				if (end != string::npos) {
					upsert_portion = compiled_ivm.substr(pos, end - pos);
					break;
				}
			}
		}
		if (upsert_portion.empty()) {
			throw ParserException("Could not extract upsert from OpenIVM compiled SQL for CMV '" + view_name + "'");
		}

		// Redirect table references to CMV data table
		string cmv_data_table = "sidra_centralized_view_" + view_name;
		// Replace _ivm_data_<name> with CMV data table
		upsert_portion = StringUtil::Replace(upsert_portion, ivm_data_table, cmv_data_table);
		// Replace bare view name references (e.g., "left join combined on")
		// but not inside cmv_data_table which already contains the view name
		upsert_portion = StringUtil::Replace(upsert_portion, "left join " + view_name + " on",
		                                     "left join " + cmv_data_table + " on");

		// Step 5: Store delta SQL and merge template separately (assembled at flush time)
		SERVER_DEBUG_PRINT("[CMV] Delta SQL:\n" + delta_sql);
		SERVER_DEBUG_PRINT("[CMV] Merge template:\n" + upsert_portion);

		// Step 6: Create centralized view table on main DB
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

		// Add observability metric columns via ALTER TABLE (Table 1 in paper)
		// sidra_window is already in the IVM data table (injected into query before OpenIVM)
		metadata_queries.push_back("ALTER TABLE " + cmv_data_table +
		                           " ADD COLUMN responsiveness NUMERIC(5,2) DEFAULT 0");
		metadata_queries.push_back("ALTER TABLE " + cmv_data_table +
		                           " ADD COLUMN completeness NUMERIC(5,2) DEFAULT 100");
		metadata_queries.push_back("ALTER TABLE " + cmv_data_table + " ADD COLUMN buffer_size NUMERIC(5,2) DEFAULT 0");

		// Store metadata
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_view_constraints VALUES('" +
		                           EscapeSingleQuotes(view_name) + "', " + to_string(vc.refresh) + ", " +
		                           to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " + to_string(vc.min_agg) +
		                           ", now())");
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_cmv_queries VALUES('" + EscapeSingleQuotes(view_name) +
		                           "', '" + EscapeSingleQuotes(source_views_str) + "', '" +
		                           EscapeSingleQuotes(delta_sql) + "', '" + EscapeSingleQuotes(upsert_portion) +
		                           "', '" + EscapeSingleQuotes(cmv_data_table) + "', '" + EscapeSingleQuotes(ivm_type) +
		                           "', now())");

		SERVER_DEBUG_PRINT("[CMV] Compiled and stored: " + view_name + " (type=" + ivm_type + " source=" + source_view +
		                   " data_table=" + cmv_data_table + ")");

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

		// Also create staging table in shadow DB (needed for CMV plan traversal later)
		shadow_con.Query(staging_ddl);

		// The centralized view table is created by the CMV compilation path.
		// Users must define a CMV to flush data from staging to centralized.

		// Metadata inserts
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_view_constraints VALUES('" +
		                           EscapeSingleQuotes(view_name) + "', " + to_string(vc.window) + ", " +
		                           to_string(vc.ttl) + ", " + to_string(vc.refresh) + ", " + to_string(vc.min_agg) +
		                           ", now())");
		metadata_queries.push_back("INSERT OR IGNORE INTO sidra_tables VALUES('sidra_staging_view_" +
		                           EscapeSingleQuotes(view_name) + "', " +
		                           to_string(static_cast<int32_t>(TableScope::centralized)) + ", NULL, true)");
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
		metadata_queries = CompileViewCreation(shadow_con, data, shadow_db_name);
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
