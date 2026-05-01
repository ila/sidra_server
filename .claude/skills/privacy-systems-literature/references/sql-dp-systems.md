# SQL and Database DP Systems

## Elastic Sensitivity

- Johnson, Near, and Song, "Towards Practical Differential Privacy for SQL Queries", PVLDB 11(5), 2018. It introduces elastic sensitivity for SQL queries, providing tractable sensitivity bounds for many SQL queries including joins. Source: https://www.vldb.org/pvldb/vol11/p526-johnson.pdf

Local connection: `/home/ila/Code/pac` implements `privacy_mode = 'dp_elastic'` using FK-chain max frequencies and Laplace noise.

## Bounded User Contribution SQL

- Wilson, Zhang, Lam, Desfontaines, Simmons-Marengo, and Gipson, "Differentially Private SQL with Bounded User Contribution", PoPETs 2020. It provides a relational-operator approach for user-level DP when individuals contribute many rows, and reports deployment lessons and stochastic testing. Source: https://petsymposium.org/popets/2020/popets-2020-0025.php

SIDRA connection: This is directly relevant to PDS/user-level contribution bounding before grouped aggregates leave staging.

## Google DP Libraries

- Google Differential Privacy libraries include C++, Go, Java, Privacy on Beam, PipelineDP4j, PipelineDP, accounting, stochastic testing, DP Auditorium, and a command-line interface for DP SQL with ZetaSQL. The low-level building blocks assume contribution bounds are already enforced by the caller/framework. Source: https://github.com/google/differential-privacy

SIDRA connection: Useful implementation substrate for `DPAgg`, but SIDRA must own contribution bounding, group/key selection, accounting, and repeated-release semantics.

## Other Systems

- PINQ (McSherry 2009) is the classic integrated query language/runtime for DP with privacy budget tracking.
- PrivateSQL (Kotsogiannis et al. 2019) explores practical DP SQL over relational workloads.
- HDPView (Kato et al. PVLDB 2022) creates DP materialized views for high-dimensional relational data exploration. It is more about workload-independent DP views over cubes than SIDRA-style PDS/staging.
- Qrlew is a SQL-to-SQL DP rewriting framework. It is relevant as a compiler comparison because SIDRA also compiles privacy-aware SQL, but Qrlew is focused on DP query rewriting rather than decentralized placement. Source: https://qrlew.github.io/
- Chorus rewrites statistical SQL queries to embed DP mechanisms before execution and was designed to work without DBMS modifications. It is a useful comparison for SIDRA's SQL-to-SQL/privacy compilation angle. Source: https://deepai.org/publication/chorus-differential-privacy-via-query-rewriting
- SmartNoise SQL wraps database/Spark connections and uses metadata files describing identifiers, row privacy, contribution bounds, and value bounds. It is useful as a practical reference for the metadata SIDRA would need if it offers DP releases over arbitrary SQL. Source: https://docs.smartnoise.org/sql/index.html
- OpenDP is a broader DP platform with a SQL data access layer and analysis-graph validation. Source: https://www.microsoft.com/en-us/research/publication/platform-for-differential-privacy/

## Practical SIDRA Implications

- SQL DP systems usually need contribution bounding per privacy unit, value bounds/clipping for sums, private group/key selection, and budget accounting.
- `MINIMUM AGGREGATION` is useful for k-anonymity-like suppression but should not be described as DP unless randomized thresholding and accounting are implemented.
- If SIDRA uses Google DP libraries, document exactly where contribution bounding and privacy-unit grouping happen.
