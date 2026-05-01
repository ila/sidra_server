---
name: pac-privacy-reference
description: PAC Privacy and PAC DuckDB extension reference. Auto-loaded when discussing PAC privacy, instance-based privacy, membership inference resistance, mutual-information bounds, PAC SQL rewriting, or integrating PAC/privacy extension ideas into SIDRA.
---

# PAC Privacy Reference

Use this skill for PAC Privacy theory, the local PAC DuckDB extension at `/home/ila/Code/pac`, and SIDRA paper work that needs the missing PAC Privacy discussion.

## What PAC Is

PAC Privacy is an instance-based privacy notion: it quantifies how hard it is to infer sensitive data from a particular data-processing pipeline and data distribution, rather than proving a worst-case neighbor guarantee as DP does. Its published framing is "Probably Approximately Correct Privacy"; it uses information-theoretic leakage and simulation to produce privacy parameters for black-box processing.

Read `references/pac-literature.md` before writing paper text or making theory claims.

## Local PAC Extension

The local code is in `/home/ila/Code/pac`. Read its `CLAUDE.md` before editing or comparing implementation details.

Core facts:

- Extension name in the local repo/docs is `privacy`; community extension listing calls it `pac`.
- It supports `privacy_mode = 'pac'` and `privacy_mode = 'dp_elastic'`.
- PAC mode rewrites standard aggregates to PAC aggregates over 64 sub-samples keyed by a hashed privacy unit.
- DP-elastic mode uses elastic sensitivity and Laplace noise for formal user-level `(epsilon, delta)`-DP.
- DDL declares privacy units and propagation paths using `PRIVACY_KEY`, `SET PU`, `PRIVACY_LINK`, and `PROTECTED` in local docs. The community extension page still documents older `PAC_KEY`/`PAC_LINK` names, so verify the code/docs before changing user-facing examples.

Important files in `/home/ila/Code/pac`:

- `src/core/privacy_optimizer.cpp` - optimizer hook and mode dispatch.
- `src/compiler/pac_bitslice_compiler.cpp` - PAC compilation entry.
- `src/compiler/dp_elastic_compiler.cpp` - DP-elastic compilation.
- `src/query_processing/pac_expression_builder.cpp` - aggregate rewriting and clipping.
- `src/categorical/pac_categorical_rewriter.cpp` - categorical PAC query transformations.
- `src/include/aggregates/pac_aggregate.hpp` - PAC bind data, noise calibration, p-tracking.
- `docs/pac/functions.md`, `docs/pac/query_operators.md`, `docs/pac/runtime_checks.md`, `docs/pac/ptracking.md` - user/internal docs.
- `attacks/attacks.md`, `attacks/clip_attack_results.md` - empirical MIA/clip attack analysis.

## SIDRA Integration Guidance

- PAC is useful as a staging/flush release mechanism when SIDRA wants empirical MIA resistance and no per-query sensitivity analysis.
- DP remains the right claim when the paper needs a formal neighboring-dataset guarantee.
- PAC and SIDRA share the "privacy unit" concept. In SIDRA this maps naturally to PDS/client/user identity and `client_id`; in PAC it maps to the declared PU key and linked rows.
- For repeated SIDRA flushes, be explicit about composition. PAC Privacy has composition bounds in the original theory, but SIDRA needs a concrete accounting story for repeated windows, correlated refreshes, and p-tracking if outputs are linkable.
- PAC's known weak spot is small groups/extreme outliers unless clipping or minimum aggregation is enforced. SIDRA's `MINIMUM AGGREGATION`, TTL windows, and potential per-PDS pre-aggregation should be presented as complementary controls, not as a proof by themselves.

## Build/Test PAC

From `/home/ila/Code/pac`:

```bash
GEN=ninja make
make test
build/release/test/unittest "test/sql/pac_sum.test"
build/release/extension/privacy/pac_test_runner
```
