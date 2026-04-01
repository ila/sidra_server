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
