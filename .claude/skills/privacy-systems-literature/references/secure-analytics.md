# Secure Analytics, MPC, and DP

## Secrecy

- Liagouris, Kalavri, Faisal, and Varia, "Secrecy: Secure collaborative analytics on secret-shared data", BU technical report 2021 / NSDI 2023 system line. Secrecy composes relational query plans over replicated secret sharing and exposes MPC costs to logical/physical optimization. Source: https://open.bu.edu/items/70163b36-ee53-4f91-8c35-39569a2cf362

SIDRA connection: Secrecy is the "strong cryptographic collaboration" foil. SIDRA is lighter-weight and SQL/MV/PDS-oriented, but a Secrecy-like substrate could harden staging or cross-PDS computation.

## IncShrink

- Wang, Bater, Nayak, and Machanavajjhala, "IncShrink: Architecting Efficient Outsourced Databases using Incremental MPC and Differential Privacy", SIGMOD 2022. IncShrink privately maintains materialized views for secure outsourced growing databases using incremental MPC, and uses DP to bound leakage from update-aware optimizations. Source: https://scholars.duke.edu/publication/1526749

SIDRA connection: Very relevant because it combines IVM, MPC, DP, and materialized views. Difference: IncShrink targets secure outsourced databases and view-based query answering; SIDRA targets decentralized PDS-owned base data and SQL-declared staging/flush architecture.

## Longshot

- Zhang, Bater, Nayak, and Machanavajjhala, "Longshot: Indexing Growing Databases using MPC and Differential Privacy", PVLDB 2023. It maintains DP-leaky indexes/synopses/stores to reduce expensive MPC for growing outsourced databases. Source: https://scholars.duke.edu/publication/1583583

SIDRA connection: Useful if SIDRA later wants private auxiliary structures for faster staging or central query answering.

## General MPC Query Processing

- Sepehri, Cimato, and Damiani, "Privacy-Preserving Query Processing by Multi-Party Computation", The Computer Journal 2015. It studies relational query processing over horizontally partitioned databases using SMC/MPC, selection, and equi-joins. Source: https://academic.oup.com/comjnl/article/58/10/2195/453660

## Practical Implications

- MPC can remove trust in the staging server but changes the cost model and protocol complexity.
- DP protects released outputs; MPC protects computation visibility. They answer different parts of SIDRA's threat model and can be combined.
- Secure enclaves are another route for staging trust, but they require hardware assumptions and remote attestation and still need output privacy.
