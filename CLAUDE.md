# CLAUDE.md

## Build & Test
```bash
GEN=ninja make           # Build release
make format-fix          # Format before commit
```

Server binary: `./build/release/duckdb`

## Architecture
SIDRA is a decentralized analytics system. Server compiles DDL, clients execute locally, send deltas back, server flushes to centralized views.

### Key Lesson: DuckDB Connection Isolation
**NEVER** open a separate `DuckDB` instance on the same database file within a pragma function. DuckDB instances have independent WAL/buffer pools — writes from one instance are invisible to another until checkpoint. Always use the caller's `context.db` connection:
```cpp
// WRONG: opens separate instance, writes invisible to caller
DuckDB server_db("mydb.db");
Connection server_con(server_db);

// RIGHT: shares caller's connection, writes visible immediately
Connection server_con(*context.db);
```

### Server Flow
1. `CREATE DECENTRALIZED TABLE` — defines schema in shadow DB + writes `decentralized_queries.sql`
2. `CREATE DECENTRALIZED MATERIALIZED VIEW` — compiles IVM DDL, writes to decentralized queries
3. `pragma run_server` — listens on TCP, accepts client connections
4. Client `pragma refresh` — sends MV data to server staging tables
5. `pragma flush('view', 'duckdb')` — moves staging → centralized, computes metrics

### Debug
Set `SERVER_DEBUG 1` in `src/include/server_debug.hpp` for verbose output.

### Testing Flush Without Client
1. Define schema: `CREATE DECENTRALIZED TABLE ... ; CREATE DECENTRALIZED MATERIALIZED VIEW ...`
2. Manually populate staging: `INSERT INTO sidra_staging_view_<name> VALUES (...)`
3. Run flush: `pragma flush('<name>', 'duckdb')`
4. Verify: `SELECT * FROM sidra_centralized_view_<name>`

### Common Pitfalls
- Delete stale `*_flush.sql` files after code changes (flush reads pre-compiled SQL)
- Delete stale `decentralized_queries.sql` (appends, never truncates)
- In-memory server (`./duckdb` without file) loses all tables on exit
- `current_date` requires ICU extension — use `now()::TIMESTAMP` instead
- OpenIVM extension must be loaded for materialized view operations

---

## OpenIVM Reference (`/home/ila/Code/openivm`)

OpenIVM is a DuckDB extension for Incremental View Maintenance (IVM). SIDRA uses it to compile centralized MVs.

### How to plan a query and get a LogicalOperator
```cpp
// 1. Parse
Parser parser;
parser.ParseQuery(sql);
// 2. Plan
Planner planner(*con.context);
planner.CreatePlan(std::move(parser.statements[0]));
auto plan = std::move(planner.plan);        // unique_ptr<LogicalOperator>
auto &binder = *planner.binder;
auto &output_names = planner.names;         // vector<string>
```

### How to replace a GET node (table redirection)
OpenIVM's `CreateDeltaGetNode` (`src/rules/ivm_helpers.cpp`) shows the pattern:
1. Cast to `LogicalGet`: `auto &old_get = op->Cast<LogicalGet>();`
2. Get original table name: `old_get.GetTable()->name`
3. Look up target table in catalog: `Catalog::GetEntry<TableCatalogEntry>(...)`
4. Create new GET with **same table_index** (preserves all parent bindings):
   ```cpp
   auto new_get = make_uniq<LogicalGet>(old_get.table_index, scan_function, ...);
   ```
5. Copy column_ids, add extra columns as needed
6. Replace in plan tree: `op = std::move(new_get);`

**Key**: keeping the same `table_index` means no parent rewiring needed.

### How to convert a plan back to SQL (LPTS)
```cpp
#include "logical_plan_to_sql.hpp"  // from lpts
LogicalPlanToSql lpts(*con.context, plan, output_names);
auto cte_list = lpts.LogicalPlanToCteList();
string sql = LogicalPlanToSql::CteListToSql(cte_list);
```

### OpenIVM compilation at CREATE MV time
File: `src/core/openivm_parser.cpp`
1. Parser intercepts `CREATE MATERIALIZED VIEW`
2. Plans the query → LogicalOperator tree
3. Applies IVM rewrites (AVG→SUM+COUNT, DISTINCT→GROUP BY+COUNT)
4. Converts to SQL via LPTS → stores in `_duckdb_ivm_views`
5. Creates: `_ivm_data_<name>` (physical table), `delta_<table>` (change tracking), unique index on GROUP BY keys

### OpenIVM refresh (`PRAGMA ivm('view_name')`)
File: `src/upsert/openivm_upsert.cpp`
1. `DoIVM()` table function triggers optimizer → `IVMRewriteRule` rewrites plan to read from delta tables
2. LPTS converts rewritten plan back to SQL → `INSERT INTO delta_<view> SELECT ...`
3. `CompileAggregateGroups()` generates MERGE SQL from delta into data table
4. All queries assembled and executed. `ivm_files_path` setting writes SQL to disk.

### IVM types and upsert strategies
| IVMType | Strategy | Use case |
|---|---|---|
| `AGGREGATE_GROUP` | CTE + MERGE (additive) | SUM, COUNT GROUP BY |
| `AGGREGATE_HAVING` | Group-recompute (delete + re-insert) | HAVING filters |
| `SIMPLE_AGGREGATE` | CTE + UPDATE | SUM/COUNT without GROUP BY |
| `SIMPLE_PROJECTION` | ROW_NUMBER + generate_series | No aggregates |
| `FULL_REFRESH` | DELETE + INSERT | MIN/MAX, ORDER BY LIMIT |

### Key files
- `src/core/openivm_parser.cpp` — CREATE MV compilation
- `src/rules/ivm_helpers.cpp` — CreateDeltaGetNode (GET node replacement)
- `src/rules/openivm_rewrite_rule.cpp` — IVM rule dispatcher
- `src/upsert/openivm_upsert.cpp` — PRAGMA ivm() orchestration
- `src/upsert/openivm_compile_upsert.cpp` — MERGE/UPDATE SQL generation
- `src/include/core/openivm_constants.hpp` — column prefixes, table prefixes

---

## LPTS Reference (`/home/ila/Code/lpts`)

LPTS (Logical Plan To SQL) converts DuckDB logical plans back to SQL strings. Used by OpenIVM to serialize rewritten plans.

### API
```cpp
#include "logical_plan_to_sql.hpp"
// Constructor: takes context, plan root, optional output column names
LogicalPlanToSql lpts(context, plan, output_names);
// Convert: bottom-up traversal → CTE list
auto cte_list = lpts.LogicalPlanToCteList();
// Serialize: CTE list → SQL string
string sql = LogicalPlanToSql::CteListToSql(cte_list);
```

### How it works
1. **Bottom-up DFS** via `RecursiveTraversal()` — processes children before parent
2. Each `LogicalOperator` becomes a **CteNode** (GET → GetNode, FILTER → FilterNode, etc.)
3. **column_map** tracks `ColumnBinding(table_index, col_index)` → column name/alias across the tree
4. **ExpressionToAliasedString()** resolves bound column refs to CTE column names
5. Output is a `WITH ... SELECT ...` CTE chain

### column_map (central state)
- Type: `map<MappableColumnBinding, unique_ptr<ColStruct>>`
- GET node registers: `column_map[CB(table_idx, col_idx)] = ColStruct("column_name")`
- Parent operators look up children: `column_map.at(binding)->ToUniqueColumnName()` → `"t0_column_name"`
- Projection/aggregate create NEW bindings for their outputs

### Supported operators
GET, FILTER, PROJECTION, AGGREGATE (GROUP BY + HAVING), COMPARISON_JOIN (all types), UNION ALL, EXCEPT, ORDER BY, LIMIT, DISTINCT, INSERT, CROSS PRODUCT, table functions

### Two pipelines
1. **CTE pipeline** (production): LogicalOperator → CteList → SQL. Complete and tested.
2. **AST pipeline** (experimental): LogicalOperator → AstNode tree → CteList → SQL. Supports dialect variants (Postgres).

### Key files
- `src/logical_plan_to_sql.cpp` — Core conversion (RecursiveTraversal, CreateCteNode, ExpressionToAliasedString)
- `src/include/logical_plan_to_sql.hpp` — CteNode hierarchy, LogicalPlanToSql class, column_map
- `src/lpts_pipeline.cpp` — AST pipeline (LogicalPlanToAst, AstToCteList)
- `src/include/lpts_ast.hpp` — AST node hierarchy
- `src/lpts_extension.cpp` — PRAGMA lpts(), lpts_check(), print_ast()

### Build & test
```bash
cd /home/ila/Code/lpts && GEN=ninja make && make test
```
