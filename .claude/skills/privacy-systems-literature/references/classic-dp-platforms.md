# Classic and Practical DP Platforms

## PINQ

- McSherry, "Privacy Integrated Queries", SIGMOD 2009 / CACM 2010. PINQ provides a SQL-like/LINQ-style interface over raw data while enforcing DP structurally and tracking privacy budget. Source: https://www.microsoft.com/en-us/research/publication/privacy-integrated-queries/

SIDRA connection: PINQ is the canonical query-language DP platform. SIDRA differs by compiling placement and materialized-view flow across PDS, staging, and central warehouse.

## Airavat

- Roy, Setty, Kilzer, Shmatikov, and Witchel, "Airavat: Security and Privacy for MapReduce", NSDI 2010. Airavat combines mandatory access control and differential privacy for MapReduce computations over sensitive data. Source: https://www.microsoft.com/en-us/research/publication/airavat-security-and-privacy-for-mapreduce/

SIDRA connection: Useful for the "trusted runtime constrains untrusted analytics code" line. SIDRA currently constrains SQL/MV definitions rather than arbitrary MapReduce code.

## GUPT

- Mohan, Thakurta, Shi, Song, and Culler, "GUPT: Privacy Preserving Data Analysis Made Easy", SIGMOD 2012. It targets DP for programs not written with privacy in mind, with budget allocation and side-channel considerations. Source: https://www.cs.umd.edu/~elaine/docs/gupt.pdf

SIDRA connection: Useful if arguing that SIDRA's declarative restrictions are simpler than privatizing arbitrary programs.

## Chorus

- Johnson et al., "Chorus: Differential Privacy via Query Rewriting", 2018. Chorus rewrites SQL queries so privacy mechanisms run inside the database engine, without changing the DBMS or the user's query. Source: https://deepai.org/publication/chorus-differential-privacy-via-query-rewriting

SIDRA connection: Closest classic comparison for SQL rewriting. SIDRA also rewrites/compiles SQL, but its compiler also partitions execution across decentralized and centralized storage.

## SmartNoise, OpenDP, Tumult

- SmartNoise SQL wraps SQL/Spark/Pandas connections, reads metadata describing identifiers/contribution/value bounds, and returns DP query results. Source: https://docs.smartnoise.org/sql/index.html
- OpenDP provides vetted DP building blocks, validators, and SQL data-access tooling. Source: https://www.microsoft.com/en-us/research/publication/platform-for-differential-privacy/
- Tumult Analytics is a production-oriented DP analytics library used in public-sector and enterprise deployments. Source: https://www.tmlt.io/resources/tumult-analytics-open-source

SIDRA connection: These are implementation substrates or metadata-design references, not direct architecture matches. SIDRA should learn from their metadata requirements: privacy unit, contribution limits, value bounds, domain/key handling, and budget accounting.
