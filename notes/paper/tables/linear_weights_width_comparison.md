# Linear-weights band-width comparison (Dirichlet finite-sample)

Old = fixed transition-count weights (`dirichlet_alpha=None`). New = per-(season, league) Jeffreys Dirichlet(`n`+0.5) combo weights, one draw per RE-posterior draw. Both share the same published `run_expectancy` posterior and the same prod transition counts, so the only difference is the finite-sample weight uncertainty.

- Cells (season, league, play): **5,099**
- RE posterior draws: **4,000**
- Transition-count rows (combos): **194,913**

**Update (paper-revision):** `propagate_linear_weights_draws` now sorts `transition_counts` by the full grain
(`season, league, play, run_expectancy_start_key, run_expectancy_end_key, runs_on_play`) before assigning
per-cell Dirichlet draws — the prior code's per-cell draw assignment followed the input frame's row order,
so a rebuild that scanned `linear_weights_transition_counts` in a different physical order (DuckDB gives no
row-order guarantee without `ORDER BY`) silently reproduced a different, equally-valid-but-different
realization of the Dirichlet weights. Re-running the regeneration snippet below against the same published
`run_expectancy` posterior and the same prod transition counts (now order-independent) shifted the summary
statistics below at the second-to-third decimal for the bulk of the distribution and somewhat more in the
sparse-cell tail (p90/p99/max, and the largest-widenings table), since sparse cells are exactly where the
Dirichlet draw is most sensitive to which physical row draws which random values. Nothing else about the
methodology changed. (Confirmed empirically: re-running the pre-fix code against the same DuckDB scan
reproduces the previously published numbers exactly.)

## Acceptance check

- Width non-decreasing on **99.57%** of cells (target >= 99%). Cells where width decreased: **22** (0.43%).
- 10 largest widenings sit at median n_events **21** vs overall median n_events **544** — largest widenings are on the smallest-n cells.

## Band-width ratio (new / old), overall

| stat | value |
| --- | --- |
| min | 0.9905 |
| p10 | 1.0597 |
| p25 | 1.1790 |
| median | 1.5588 |
| p75 | 2.3800 |
| p90 | 4.1936 |
| p99 | 11.0640 |
| max | 59.8331 |
| mean | 2.2347 |

## Ratio by cell sample-size decile (n_events)

| decile | n range | cells | ratio median | ratio p90 | old width med | new width med | share decreased |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 1–42 | 507 | 4.683 | 8.723 | 0.04901 | 0.18723 | 0.20% |
| 1 | 43–119 | 513 | 2.001 | 3.818 | 0.04031 | 0.08989 | 0.00% |
| 2 | 120–228 | 503 | 1.981 | 3.484 | 0.03322 | 0.06638 | 0.00% |
| 3 | 229–379 | 516 | 1.527 | 2.441 | 0.03297 | 0.05600 | 0.58% |
| 4 | 381–543 | 510 | 1.295 | 1.938 | 0.07035 | 0.07988 | 1.18% |
| 5 | 544–787 | 510 | 1.130 | 3.000 | 0.05995 | 0.07255 | 1.57% |
| 6 | 788–1,212 | 510 | 1.476 | 3.437 | 0.02425 | 0.05005 | 0.78% |
| 7 | 1,213–2,967 | 510 | 1.267 | 4.426 | 0.03350 | 0.04457 | 0.00% |
| 8 | 2,969–8,642 | 510 | 1.173 | 2.014 | 0.02155 | 0.02498 | 0.00% |
| 9 | 8,668–45,039 | 510 | 1.592 | 1.798 | 0.00575 | 0.00993 | 0.00% |

## 10 largest widenings

| season | league | play | n_events | old width | new width | ratio |
| --- | --- | --- | --- | --- | --- | --- |
| 1941 | NN2 | Double | 8 | 0.02068 | 1.23719 | 59.83 |
| 1921 | NN1 | Triple | 3 | 0.05687 | 1.91121 | 33.61 |
| 1924 | NN1 | Double | 5 | 0.05141 | 1.35633 | 26.38 |
| 1914 | AL | HomeRun | 148 | 0.00655 | 0.15644 | 23.89 |
| 1924 | ECL | Double | 7 | 0.04999 | 1.09432 | 21.89 |
| 1910 | AL | HomeRun | 147 | 0.00786 | 0.16459 | 20.94 |
| 1939 | NAL | HomeRun | 25 | 0.01925 | 0.40272 | 20.92 |
| 1913 | AL | HomeRun | 159 | 0.00757 | 0.15534 | 20.53 |
| 1940 | NN2 | Double | 22 | 0.02978 | 0.58937 | 19.79 |
| 1913 | NL | OtherAdvanceOut | 20 | 0.01173 | 0.23170 | 19.75 |

## Regenerate

Read-only; touches no database or artifact state. From `bc/`, run
`PYTHONPATH=$PWD uv run python -` with the snippet below. It resolves the
published `run_expectancy` pointer via `find_published_manifest` (branch root
then global fallback), reads its `run_expectancy_posterior.parquet`, reads
`main_models.linear_weights_transition_counts` from `bc.db`
(`read_only=True`), and propagates both weight modes over the same posterior:
`dirichlet_alpha=None` (old, fixed counts) vs the default Jeffreys Dirichlet
(new). The full 4000-draw run peaks near 20 GB; the ratio distribution is
stable under a deterministic draw subsample (join to the first N of
`chain,draw` sorted) if memory is tight.

```python
import duckdb, polars as pl
from python_models.statistical.linear_weights_estimated import propagate_linear_weights_draws
from python_models.statistical.manifests import find_published_manifest
from python_models.statistical.schemas import PublishedPointer

ptr = PublishedPointer.model_validate_json(find_published_manifest("run_expectancy").read_text())
re_draws = pl.read_parquet(str(ptr.manifest_path.parent / "exports" / "run_expectancy_posterior.parquet"))
con = duckdb.connect("bc.db", read_only=True)
counts = con.sql("SELECT season, league, play, play_category, run_expectancy_start_key, "
                 "run_expectancy_end_key, runs_on_play, n "
                 "FROM main_models.linear_weights_transition_counts").pl()
con.close()

old = propagate_linear_weights_draws(counts, re_draws, dirichlet_alpha=None)
new = propagate_linear_weights_draws(counts, re_draws)
w = lambda d: pl.col("run_value_hdi_upper") - pl.col("run_value_hdi_lower")
joined = (old.select("season", "league", "play", w(old).alias("width_old"), pl.col("n_events"))
          .join(new.select("season", "league", "play", w(new).alias("width_new")),
                on=["season", "league", "play"])
          .with_columns((pl.col("width_new") / pl.col("width_old")).alias("ratio"),
                        (pl.col("width_new") < pl.col("width_old")).alias("decreased")))
print(joined.select("ratio", "decreased").describe())
print("share_decreased", joined.get_column("decreased").mean())
```
