# SIDRA Literature Map

## What Is Already in SIDRA.pdf

The local draft `/home/ila/Code/sidra_server/SIDRA.pdf` cites or discusses:

- CQL and streaming relations for time-varying data.
- OpenIVM as the SQL-to-SQL IVM compiler used by SIDRA.
- DBToaster, Differential Dataflow, DBSP, Noria, Materialize, RisingWave, and commercial materialized views.
- dbt, Delta Live Tables, Dynamic Tables, BigQuery/Redshift/Synapse/Oracle materialized views.
- SOLID and personal data stores.
- Google DP, PINQ, PrivateSQL, SQL DP with bounded user contribution, HDPView, DP-SQLP, InkShrink, shuffle DP, federated DP, and secure computation.
- A placeholder "PAC Privacy. [todo]" section near the end.

## Strong Additions/Clarifications

- Add PAC Privacy as the missing instance-based privacy citation: Xiao and Devadas, arXiv:2210.03458. This gives the theory behind PAC as a black-box, simulation-oriented, information-theoretic metric.
- Add SIMD-PAC-DB, arXiv:2603.15023, as the concrete DuckDB/PAC implementation. This is directly connected to SIDRA authorship and to the local `/home/ila/Code/pac` code.
- For streaming DP, prefer Zhang et al. PVLDB 2024 "Differentially Private Stream Processing at Scale" as the nearest systems paper. It solves user-level DP for continuous key/value stream aggregates, including unknown key selection and continual histograms, but assumes a centralized Google-style streaming stack rather than PDS-owned base data.
- For DP SQL, connect Johnson/Near/Song VLDB 2018, Wilson et al. PoPETs 2020, Google DP libraries, and Qrlew as SQL/query rewriting systems. SIDRA differs by moving raw-base-data placement and staging/flush into the language.
- For decentralized analytics, distinguish SOLID/PDS data ownership from federated learning. SIDRA's contribution is declarative relational MVs over decentralized stores, not model training.
- Add Mayfly (Google, 2024) as the closest production-style relative for on-device, ephemeral, SQL-programmable federated analytics with streaming DP. SIDRA differs by compiling SQL MVs over PDS data and by materializing staging/central views.
- Add IncShrink/Longshot/Secrecy as secure outsourced analytics systems combining MPC and DP. These are useful foils for a stronger-trust/no-raw-centralization story.
- Add OpenPDS/SafeAnswers, Databox, and Libertas as PDS systems that move computation to personal stores. SIDRA can distinguish itself by SQL DDL, IVM, staging, and warehouse-compatible MVs.

## SIDRA Gaps To Watch

- Repeated releases need a clear accounting story. Minimum aggregation, TTL, and staging are useful controls, but they are not equivalent to DP composition.
- Local DP vs global/staging DP should not be described as interchangeable. Local DP adds noise before sharing from every PDS and is usually noisier; global/staging DP can aggregate first but needs trust or secure enforcement in staging.
- PAC and DP should be kept separate. PAC is empirical/instance-based MIA resistance with mutual-information style bounds; DP is a worst-case neighboring-dataset guarantee.
- SIDRA's privacy levels should specify whether they protect against raw-value disclosure, singling-out through low-count groups, repeated-query differencing, contribution outliers, or reconstruction through correlated attributes.
- The "privacy-friendly staging area" needs an adversary model: trusted curator, audited enclave, MPC-backed staging, local DP from PDSs, or PAC/DP release mechanism. Different literature clusters assume different trust.
