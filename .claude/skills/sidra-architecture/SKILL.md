---
name: sidra-architecture
description: SIDRA project and paper reference. Auto-loaded when working on SIDRA SQL, decentralized tables/materialized views, staging-to-central flushes, privacy annotations, PDS/client-server flow, or the SIDRA paper in SIDRA.pdf.
---

# SIDRA Architecture

Use this skill when editing or explaining `/home/ila/Code/sidra_server`, especially parser, flush, server, privacy-constraint, or paper-related work.

## Local Paper

The paper draft is `/home/ila/Code/sidra_server/SIDRA.pdf`.

Current paper thesis: SIDRA is a local-first analytics architecture where raw base data remains in Personal Data Stores (PDSs), decentralized materialized views are computed close to users, and central warehouses store only aggregated/staged/centralized views. SIDRA SQL declares placement, privacy constraints, windows, TTL, refresh cadence, and minimum aggregation. The implementation compiles SIDRA SQL into DuckDB/OpenIVM/LPTS artifacts.

## Implementation Flow

1. `CREATE DECENTRALIZED TABLE` defines the local schema, extracts column constraints, records metadata, and appends client-facing SQL to `decentralized_queries.sql`.
2. `CREATE DECENTRALIZED MATERIALIZED VIEW` creates a staging table named `sidra_staging_view_<name>` with `sidra_window`, `client_id`, and `action` metadata columns. PDS clients refresh locally and send DMV deltas into this table.
3. `CREATE CENTRALIZED MATERIALIZED VIEW` compiles the CMV over staging views. SIDRA injects `sidra_window`, redirects source views to staging tables, applies `MINIMUM AGGREGATION` through `COUNT(DISTINCT client_id)`, asks OpenIVM for merge templates, and stores the delta SQL/template in `sidra_views`.
4. `PRAGMA flush('<view>', 'duckdb')` assembles the stored delta SQL and merge template, applies TTL and arrival filters, merges into `sidra_centralized_view_<view>`, updates metrics, and advances `last_flush`.
5. `PRAGMA run_server` accepts client connections and inserts incoming JSON into staging tables.

## Key Files

- `src/parser/parser.cpp` - parser extension, DDL compilation, staging/centralized table construction, OpenIVM/LPTS integration.
- `src/parser/parser_helpers.cpp` - SQL keyword/constraint extraction.
- `src/parser/parser_helper.cpp` - privacy constraint checks for decentralized tables and views.
- `src/flush/flush_function.cpp` - staging-to-centralized flush path.
- `src/flush/update_metrics.cpp` - responsiveness, completeness, and buffer-size metrics.
- `src/server/run_server.cpp` and `src/server_extension.cpp` - TCP server and extension registration.
- `src/optimizer/optimizer.cpp` - metadata cleanup for dropped SIDRA tables/views.
- `test/sql/parser.test`, `test/sql/query_checker.test`, `test/sql/flush.test` - main behavioral coverage.

## Local Rules

- Do not open a second `DuckDB` instance on the same database from a pragma or parser callback. Use `Connection con(*context.db)` so writes are visible to the caller connection.
- Do not edit the `duckdb/` submodule.
- Delete stale generated SQL files before debugging parser/flush changes: `decentralized_queries.sql` and `*_flush.sql`.
- Use `SERVER_DEBUG_PRINT` from `src/include/server_debug.hpp` for debug traces.
- Use `now()::TIMESTAMP` instead of `current_date`; ICU may be required for `current_date`.
- OpenIVM must be loaded for materialized view compilation and refresh scheduling.

## Testing Patterns

Build with `GEN=ninja make`.

Run one SQL test:

```bash
build/release/test/unittest "test/sql/flush.test"
```

For flush work, test without clients by creating SIDRA tables/views, inserting rows into `sidra_staging_view_<name>`, running `PRAGMA flush(...)`, and checking `sidra_centralized_view_<name>`.
