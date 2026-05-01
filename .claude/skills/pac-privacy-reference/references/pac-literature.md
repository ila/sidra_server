# PAC Privacy Literature

## Core PAC

- Xiao and Devadas, "PAC Privacy: Automatic Privacy Measurement and Control of Data Processing", arXiv:2210.03458, v4 June 19 2023. Main claim: PAC Privacy is a simulatable, instance-based metric for inference hardness under arbitrary leakage. It contrasts with DP's input-independent worst-case view and supports Monte Carlo analysis, online composition, and lower perturbation for some practical releases. Source: https://arxiv.org/abs/2210.03458

Use this as the SIDRA paper citation for the PAC Privacy definition, "instance-based" framing, black-box simulation, and composition claims.

## PAC in DuckDB

- Battiston, Yuan, Zhu, and Boncz, "SIMD-PAC-DB: Pretty Performant PAC Privacy", arXiv:2603.15023, submitted March 16 2026 and revised March 19 2026. The paper implements PAC-DB efficiently by treating bits of a hashed primary/privacy key as membership in 64 stochastic 50% sub-samples. It computes the same privatized answer in one query rather than the original 128 stochastic executions, reports SIMD-friendly aggregate algorithms, and releases a DuckDB community extension with SQL query rewriting. Source: https://arxiv.org/abs/2603.15023

- DuckDB community extension page for `pac`. It describes installation via `INSTALL pac FROM community; LOAD pac;`, PAC/PDS-style privacy-unit DDL, supported aggregates, `pac_mi`, seeds, and the function/settings surface. Source: https://duckdb.org/community_extensions/extensions/pac.html

For code-level truth, prefer `/home/ila/Code/pac` over the community page because the local repo may have newer names and the dual PAC/DP-elastic mode.

## Membership Inference Context

- Pyrgelis, Troncoso, and De Cristofaro, "Knock Knock, Who's There? Membership Inference on Aggregate Location Data", arXiv:1708.06145, 2017. Useful as an aggregate time-series MIA motivation for SIDRA-style continuous releases. Source: https://arxiv.org/abs/1708.06145

- Shokri, Stronati, Song, and Shmatikov, "Membership Inference Attacks against Machine Learning Models", IEEE S&P 2017 / arXiv:1610.05820. General MIA background; less directly relevant than aggregate-location MIA but useful for threat model language. Source: https://arxiv.org/abs/1610.05820

## DP Contrast Points

- Differential privacy gives worst-case protection for neighboring datasets; PAC Privacy gives distribution/instance-sensitive inference-hardness measurements. Do not describe PAC as DP.
- DP's strength is formal composition and worst-case robustness. PAC's strength is automation and lower noise when instance structure makes inference hard.
- For SIDRA, PAC can be positioned as complementary to `MINIMUM AGGREGATION`, staging TTL, and DP: PAC quantifies/controls MIA leakage on actual releases; DP supplies legal/formal guarantees where required.
