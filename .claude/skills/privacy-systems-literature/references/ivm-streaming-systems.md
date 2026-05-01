# IVM and Streaming Systems

## Classical and Stream IVM

- Gupta and Mumick's view-maintenance line is the classic background for self-maintainability and auxiliary views. For distributed/PDS settings, "Using partial information to update materialized views" is especially relevant because it studies updating views when not all base relations are accessible. Source: https://www.sciencedirect.com/science/article/pii/0306437995000356
- Yang, Golab, and Ozsu, "ViewDF: Declarative Incremental View Maintenance for Streaming Data", Information Systems 2017. ViewDF specifies update logic for streaming materialized views and sliding windows. Source: https://www.sciencedirect.com/science/article/abs/pii/S0306437917303897
- Griffin and Libkin-style aggregate/outerjoin maintenance and change-table approaches are useful if SIDRA expands supported SQL operators. Source example: https://www.sciencedirect.com/science/article/pii/S030643790400119X

## Modern Differential/Dataflow Systems

- Differential Dataflow and Materialize are central for incrementally maintaining SQL over streams using update tuples with logical timestamps and differences. Source: https://materialize.com/blog/self-correcting-materialized-views/
- DBSP is relevant for algebraic streaming/IVM foundations and z-set style updates.
- Noria is relevant for partially materialized dynamic data-flow in web applications.

## Industrial Data Engineering IVM

- Enzyme, "Incremental View Maintenance for Data Engineering", arXiv:2603.27775, 2026. Databricks/Spark Declarative Pipelines IVM engine; useful as evidence that production data engineering systems are moving toward first-class MVs and automated refresh planning. Source: https://arxiv.org/abs/2603.27775

## SIDRA Takeaways

- SIDRA should emphasize that its novelty is not IVM alone; it is IVM compiled across a privacy/data-minimizing architecture.
- "Partial information" and disconnected/mobile view maintenance are directly relevant to PDS clients that may be offline or unwilling to expose base tables.
- Window semantics, late arrivals, and TTL are core correctness/privacy issues, not just scheduling concerns.
