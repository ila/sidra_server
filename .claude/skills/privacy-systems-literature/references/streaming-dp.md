# Streaming and Continual Differential Privacy

## Core Continual Observation

- Dwork, Naor, Pitassi, and Rothblum, "Differential Privacy Under Continual Observation", STOC 2010. It starts the formal study of releasing statistics repeatedly over time and introduces tree/binary mechanisms for counters. Source: https://www.microsoft.com/en-us/research/publication/differential-privacy-under-continual-observation/

Use this for SIDRA's tree-DP or continual-release foundation.

## Production Stream DP

- Zhang, Doroshenko, Kairouz, Steinke, Thakurta, Ma, Cohen, Apte, and Spacek, "Differentially Private Stream Processing at Scale", PVLDB 17(12), 2024; arXiv:2303.18086. It presents DP-SQLP, a production streaming DP aggregation system on Spanner/F1-like infrastructure. It handles user-level DP over an output stream, unknown keys for GROUP BY, scalable DP key selection, and continual DP histograms. Sources: https://www.vldb.org/pvldb/vol17/p4145-zhang.pdf and https://arxiv.org/abs/2303.18086

SIDRA connection: DP-SQLP is a strong reference for formal streaming DP mechanisms that could be inserted into SIDRA's staging-to-central flush path. Difference: DP-SQLP assumes centralized streaming infrastructure, while SIDRA's raw data stays in PDS/client stores and uses SQL MVs/staging as the architecture boundary.

- Mayfly, "Private Aggregate Insights from Ephemeral Streams of On-Device User Data", Google Research, 2024. Mayfly computes aggregate queries over ephemeral on-device streams without central persistence of sensitive raw data. It uses on-device windowing, contribution bounding, SQL programmability, streaming DP, and immediate in-memory cross-device aggregation. Source: https://research.google/pubs/mayfly-private-aggregate-insights-from-ephemeral-streams-of-on-device-user-data/

SIDRA connection: Mayfly is probably the closest production-adjacent system to SIDRA's data-minimization goal. The key difference is that Mayfly is federated/on-device aggregate analytics, while SIDRA is a declarative DuckDB/OpenIVM architecture for PDS-resident relational base data and centrally materialized MVs.

- Cardoso and Rogers, "Differentially Private Histograms under Continual Observation: Streaming Selection into the Unknown", arXiv:2103.16787. Useful for unknown-key/group-by streaming histograms and top-k selection under continual release. Source: https://arxiv.org/abs/2103.16787

## Practical SIDRA Implications

- Per-refresh composition is simple but noisy over long TTL horizons.
- Tree/binary mechanisms reduce cumulative noise for continual counters but require storing/releasing tree state and careful handling of late events and window expiry.
- Unknown group keys require private key selection or thresholding; a deterministic "only release keys with enough users" rule is not DP by itself unless the release decision is privatized.
- Local DP at each PDS avoids trusting staging with raw local DMV values, but utility is typically worse than global/staging DP because noise is added before cross-user aggregation.
