# Decentralized Analytics and PDS Context

## Personal Data Stores and Local-First

- SOLID is the central PDS reference in SIDRA.pdf. It gives users control over personal data pods and application access. Source: https://solidproject.org/
- Fallatah, Barhamgi, and Perera, "Personal Data Stores (PDS): A Review", Sensors 2023. Broad survey of PDS concepts and platforms including OpenPDS, Databox, SOLID, MyData, HAT, Mydex, Personicle, Meeco, and Digi.me. Source: https://www.mdpi.com/1424-8220/23/3/1477
- Janssen, Cobbe, Norval, and Singh, "Decentralized data processing: personal data stores and the GDPR", International Data Privacy Law 2020. Useful legal/architecture reference for PDS under GDPR, consent, control, monitoring, and data-controller/processor complications. Source: https://academic.oup.com/idpl/article/10/4/356/5903974
- OpenPDS/SafeAnswers answers questions over personal data rather than sharing raw data, and can aggregate across users while avoiding direct raw-data release. It is a close conceptual ancestor for SIDRA's "central warehouse only sees answers/views" stance. Source: https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0098790
- Databox is a local processing platform for household/IoT personal data. It is a concrete PDS architecture for local apps, stores, drivers, and arbiters. Source: https://doi.org/10.1145/3010079.3010082
- Libertas, "Privacy-Preserving Collaborative Computation for Decentralised Personal Data Stores", PACMHCI 2025. It integrates MPC with Solid-like PDSs while preserving user autonomy and user-specific trust preferences. Source: https://www.cs.ox.ac.uk/publications/publication16239-abstract.html
- Local-first software is useful framing for user-owned data and offline/client-side state, but SIDRA's technical contribution is specifically relational SQL/MV compilation over decentralized stores.
- MotherDuck is relevant for DuckDB-based local/cloud hybrid analytics, but it is not a privacy enforcement system by itself.

## Federated and Edge Analytics

- Federated learning and federated analytics distribute computation over clients but usually focus on model/update aggregation or telemetry, not declarative SQL MVs and IVM.
- Edge-local DP papers are relevant when PDS/client output is noised before leaving the edge. Emphasize utility tradeoffs versus global/staging DP.
- Shuffle DP sits between local and central DP by anonymizing/shuffling messages before aggregation. It may fit SIDRA if there is an intermediate messaging layer, but it changes the trust assumptions and protocol.

## Secure Computation

- MPC/secure aggregation protects computation visibility and can reduce trust in staging, but it is a heavier mechanism than SIDRA's current SQL/OpenIVM path.
- Secure enclaves can protect staging execution but require hardware trust and remote attestation. They do not remove the need for contribution bounding or output privacy.
- Encryption/secure columnar storage protects at rest/in transit, not aggregate output disclosure.

## How To Position SIDRA

SIDRA occupies the gap between PDS/local-first data ownership and database-style analytical MVs. Its novelty should be stated as:

- Declarative placement and privacy policy in SQL.
- Automatic compilation into decentralized/client-side and centralized/server-side artifacts.
- Streaming/IVM refresh through staging tables.
- Output release controls at the staging-to-central boundary.

Avoid implying that decentralization alone proves privacy. SIDRA minimizes raw data centralization; output privacy still depends on minimum aggregation, DP/PAC/noise mechanisms, contribution limits, and auditing.
