---
name: privacy-systems-literature
description: Research map for SIDRA-related literature: decentralized analytics, personal data stores, local-first systems, materialized view maintenance, streaming DP, SQL DP, PAC privacy, secure computation, and privacy-preserving database systems.
---

# Privacy Systems Literature

Use this skill when writing or revising SIDRA paper sections, comparing related systems, or choosing privacy mechanisms for decentralized analytics.

Read the focused reference files as needed:

- `references/sidra-literature.md` - SIDRA paper's current citation map and missing literature.
- `references/streaming-dp.md` - DP for continual/staged/streaming releases.
- `references/sql-dp-systems.md` - SQL/query-system DP including elastic sensitivity and Google DP.
- `references/decentralized-analytics.md` - PDS/local-first/federated/edge analytics context.
- `references/classic-dp-platforms.md` - PINQ, Airavat, GUPT, Chorus, OpenDP/SmartNoise/Tumult.
- `references/secure-analytics.md` - MPC, secure outsourced DBs, and DP+cryptography systems.
- `references/ivm-streaming-systems.md` - IVM, streaming MVs, Materialize, DBSP, IncShrink, Enzyme.
- `references/privacy-attacks.md` - reconstruction, differencing, membership inference, and aggregate disclosure attacks.

## High-Level Positioning

SIDRA should be framed as a declarative architecture/compiler for data minimization: raw user data stays in PDS/client stores, while only privacy-filtered or privatized materialized-view deltas reach central storage. This is distinct from:

- Federated learning, which usually protects model training rather than relational analytics.
- Secure computation, which protects computation visibility but can be expensive and does not by itself minimize central materialization.
- Differentially private SQL systems, which usually assume a centralized curator and static/batch queries.
- Stream processors, which handle windows/triggers but generally do not provide PDS placement and column-level privacy semantics.

## Citation Discipline

- For claims about the SIDRA implementation, cite local files or `SIDRA.pdf`.
- For paper claims, prefer primary sources: arXiv, PVLDB, ACM/USENIX/Microsoft/Google/OpenDP/DuckDB docs.
- Be explicit about dates for fast-moving projects. The current session date is May 1 2026.
