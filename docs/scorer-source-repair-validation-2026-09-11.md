# Scorer source repair and parser reproducibility

The source repair preserves official `info,oscorer` and administrative `info,scorer` separately through Rust metadata, CSV, and Parquet. The legacy `scorer` field retains its compatibility behavior. The [downstream contract](scorer-provenance-contract.md) exposes the two raw fields without using either as proof of batted-ball label authorship.

## Full-corpus finding

The original default-thread comparison found a real reproducibility defect: two parses could choose different occurrences of duplicated box-score games. Canonical row sorting did not remove the difference. One affected table had 39 different rows in each direction across three game IDs. The first failed comparison is retained; it is not counted as a scorer-parity pass.

Selection now uses lexical file order and then occurrence within the file, separately for each account pass. Conventional play-by-play retains priority over deduced play-by-play, and box-score records retain their separate namespace. The parser records duplicate source ranks. This is a reproducibility policy; conflicting baseball facts still require source review.

A header prepass fixes the preferred occurrence before parallel processing. A record error that makes the rank unreliable in a duplicate-containing file fails the export. Failure to initialize a selected duplicate also fails instead of silently dropping the game or accepting whichever alternate finishes first. Already accepted higher-priority play-by-play remains authoritative over duplicated deduced records. The worker-count minimum was also corrected so a one-thread run can terminate.

## Controlled comparison

The final comparator is explicitly `b759b9c` plus the same deterministic-duplicate repair used in the scorer build. Comparing that controlled baseline with the repaired scorer build passes all 30 exported Parquet tables. Legacy schemas, row counts, and canonical values match. Only `official_scorer` and `source_scorer` are added to the two game tables. This does not claim equality between every possible output of the original nondeterministic parser and the new selected-source output.

The full-corpus code inventory audit, both controlled parses, and both Arrow 14 conversions completed successfully. The comparison covers 147,143,678 exported rows, including 205,886 play-by-play and deduced game rows and 221,558 box-score game rows. The parity report has SHA-256 `321e6159992a2f866e825aa3921b7037bab57e1dac6041347eff7babcdfe5777`. Fixture checks cover different scheduling orders and one, two, and four threads, plus malformed records and account precedence. All 153 Rust unit tests, 18 integration tests, and six Arrow conversion tests pass; formatting, strict Clippy, and the documented Python strict checker pass. Full-corpus validation compares software outputs opaquely; it does not supply new modeling labels or consume the deferred Statcast-angle evaluation.

The repaired parser is integrated locally at `415494c` on its default `master` branch. Evidence is archived at `artifacts/statistical/backtests/geometry_reliability/20260911-scorer-source-repair-v1`, root manifest SHA-256 `75e0e7163f76fc8fa1bfeb7da7914c278047e832f721dfcdd6760dae6f1b3309`. All 59 entries in the original full-corpus evidence manifest were independently rehashed before archival. The 71 archived files include the complete source patch, binary and corpus hashes, failed and accepted comparisons, checker setup, logs, and corrected game Parquets. The normalized warning inventories match across the controlled builds; no warning was treated as evidence of improved baseball accuracy.

## Materialization boundary

Corrected game Parquets are retained as local validation artifacts. Existing published source objects, production databases, and frozen modeling artifacts are unchanged. A future source release must publish the compatible source schema before downstream models requiring these columns can be materialized. Existing scorer-based artifacts remain legacy references until explicitly rebuilt and revalidated.

This repairs source provenance and build reproducibility. The airborne translation still fails its prespecified season calibration requirements in development, and historical reconstruction remains unvalidated.
