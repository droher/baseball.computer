# Modeling handoff — airborne recording-regime development, 2026-09-11

## Start here

The regime-model validation runner and reporter are repaired, tested, and were run once on the bound development frame. The predeclared development screen did not pass; see [the results](geometry-air-regime-development-results-2026-09-11.md). Since then, full-season Statcast angles for 2016 to 2018 were acquired and a bridging-hitter transport check was run; see [the bridge check](geometry-air-bridge-check-2026-09-11.md). The larger goal remains unachieved: make the models reliable enough. No model is promoted or published. Do not resume expensive work until the user asks.

Repository: `/Users/davidroher/Repos/baseball.computer`. The regime work is on branch `codex/air-regime-development` and the bridge work on `air-bridge-statcast-2016-2018`, both squash merged into local `main`; nothing was pushed.

## User's baseball decisions

- Produce both original-source classifications and a separately standardized output; predicting what the source would record is a distinct task from predicting the standardized category.
- Preserve recorded Ground versus Air. Most scorers agree on that distinction.
- Standardize only airborne subtypes against current Statcast angle bands. Keep bunts separate; never turn a recorded ground bunt into an airborne ball because its angle is 22 degrees.
- Scorer judgment and press-box perspective can affect line/fly/pop calls. Available data do not identify separate person and park effects.
- Ask the user for consequential baseball SME. An optional earlier question about who authored historical batted-ball labels remains unanswered; do not assume official-scorer headers establish authorship.

## What is established

The pooled airborne model improved average predictive scores but failed season calibration with opposing early/late biases (`docs/geometry-air-development-results-2026-09-11.md`).

The regime model (`bc/python_models/statistical/backtests/geometry_air_regime.py`) has one `q ~ Dirichlet(1,1,1)` per predictor cell, `p_regime | q ~ Dirichlet(kappa*q)`, and categorical counts. Regimes are the represented seasons 2015/2019 (early) and 2023/2025 (late). Unknown regimes abstain. Four arms use marginal, result, recorded subtype, or recorded subtype by result cells. Concentration 30 is primary; 3 and 300 are fixed sensitivities. Shared posterior draws retain covariance across events using the same cell and regime. Computational checks: `docs/geometry-air-regime-computation-2026-09-11.md`.

The scored regime run (`artifacts/statistical/backtests/geometry_reliability/20260911-air-regime-development-full-v2`, manifest SHA-256 `573d02c1…`; report `20260911-air-regime-report-full-v2`, manifest `42372f39…`; commit `50699f6`) shows:

- Positive paired bootstrap gain intervals against both reference arms in every split family, and against the archived pooled candidate on identical events.
- Every per-class 15-bin ECE at most 0.03 on supported slices; every season class-share bias within 0.013 in the game and park splits.
- The nominal 95% whole-game count coverage screen passes at every concentration (0.92 to 1.00 on supported slices).
- One failure at the primary concentration: leave-one-season-out 2015 LineDrive class-share bias -0.0203 against the 0.02 limit. Concentration 3 passes (bias -0.0185); concentration 300 fails every split family. The bias is monotone in concentration. The verdict is not robust to the prior strength, which the protocol classifies as a failure requiring investigation.

The gain over the recorded-only arm is erased by adversarially relabeling about 1.2% (log loss) or 1.5% (Brier) of references; the gain over the result-only arm needs 4.5% or 11%.

The recorded Fly/LineDrive/PopUp vocabulary has hard breaks at 2009 and 2020 while the Statcast true band mix is flat: recorded PopUp is 0.15 to 0.17 of airborne labels through 2008, about 0.02 from 2009 to 2019 (0.003 in 2015, 2017, and 2018), and about 0.12 from 2020. The early-versus-late regime gap is a label pipeline change, not hitters. The [literature review](geometry-air-regime-grounding-research-2026-09-11.md) covers the applicable methods. The [bridge check](geometry-air-bridge-check-2026-09-11.md) shows the 2016 to 2018 translation reproduces the same hitters' recorded mix in 2009 to 2011 within 0.02 and in 2012 to 2015 and 2019 within 0.06, which is the held-out reference floor; 2020 to 2022 misses by 0.22. Treat 2009 to 2019 as one pipeline with a season level, 2020 onward as a second pipeline referenced by the 2023 and 2025 matched games, and 1989 to 2008 as partially identified with no same-pipeline reference.

## Data boundaries: preserve these

All development uses the already inspected, content-bound 373 fitting games: 19,436 matched events, 9,869 eligible local-Air references from 367 games, 899 unresolved local-Air cases kept in denominators. The sample is balanced by park/season, not league prevalence.

Bound frame: `artifacts/statistical/backtests/geometry_reliability/20260911-air-development-full-v1/coverage_frame.parquet`, SHA-256 `cfdf2918180812f6b1333a76a2875f87cd75b581fab07c35bb23dd648c4e3e70`. Old pooled predictions in the same directory: `oof_predictions.parquet`, SHA-256 `9187b8dc…`.

Full-season 2016 to 2018 Statcast angles for 4,962 PRIMARY TRAIN single games (reserve anti-joined) are in `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-bridge-fitting-2016-2018-v1`, manifest SHA-256 `714fff58…`: 4,955 games paired, 260,941 batted balls, 255,710 with angles, 131,344 eligible airborne references (bunts kept separate). Seven games failed or paired incompletely and stay in the denominator (`docs/geometry-air-bridge-check-2026-09-11.md` lists them). These are development evidence under `docs/geometry-statcast-bridge-protocol.md` and may not score evaluation or reserve games.

**121 deferred modern-angle games remain unopened. The 6,105-game historical component-local reserve remains sealed.** Do not acquire evaluation angles or inspect reserve labels to finish development. Reference angle origin is unknown by row; some Statcast numbers can be provider estimates based on observer labels and outcomes. On 2026-09-11 season-level aggregate label counts were once computed over all folds including the reserve before the anti-join was checked; no game- or event-level reserve labels were viewed, the numbers were identical TRAIN-only, and the user delegated the ruling. The reserve is treated as still sealed.

## Code and how to run it

- `geometry_air_regime_predictive.py`: `predictive_counts` (one row per held-out game and season with shared draw indices) and `summarize_counts` (90%/95% intervals, coverage flags, split-batch Monte Carlo spans).
- `geometry_air_regime_development.py`: `build_oof_predictions`, `run_experiment`, and the CLI. It binds the protocol, every loaded `python_models` module, the frame hash, seeds, draws, bootstrap repetitions, and NumPy/Polars/SciPy/Pydantic/Python versions before scoring, checkpoints every fit's predictive counts, and writes `manifest.json` with status `smoke_complete`, `development_scoring_complete`, or `failed`.
- `geometry_air_regime_report.py`: verifies the run manifest, the frozen source copies, and its own loaded dependencies against the run bindings; reproduces the run's metrics and bootstrap; then writes per-class calibration, interval summaries, aggregate and held-out-season coverage, adversarial tipping, unresolved bounds, the pooled comparison, and (full runs only) the combined decision with per-criterion sensitivity changes and an explicit support-grid check. Smoke runs make no decision.
- Tests: `bc/tests/statistical/test_geometry_air_regime_{predictive,development,report}.py` (27 tests), plus the kernel and validation tests.
- `geometry_statcast_bridge_selection.py`: builds the frozen 2016 to 2018 game selection from game metadata only. `geometry_statcast_acquire.py`: runs the smoke or full acquisition against a selection root, verifies the bound mechanics artifact and, when the inputs declare smoke gates, the bound smoke artifact, and resumes with `--resume` by keeping every game whose report has a terminal status (`paired`, `pairing_incomplete`, `failed`), re-acquiring the rest, and refusing if the inputs, selection, population, or copied code differ from the stored plan. `geometry_air_bridge_check.py`: standardizes the paired events, builds the reference translation and per-hitter true profiles, and runs the gap test and translation refit per `--window START:END`. Tests: `test_geometry_statcast_bridge_selection.py`, `test_geometry_statcast_bridge.py`, `test_geometry_air_bridge_check.py`.

```sh
PYTHONPATH=bc uv run --no-sync pytest -q bc/tests/statistical/test_geometry_air_regime_predictive.py bc/tests/statistical/test_geometry_air_regime_development.py bc/tests/statistical/test_geometry_air_regime_report.py

PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_air_regime_development --source-frame artifacts/statistical/backtests/geometry_reliability/20260911-air-development-full-v1/coverage_frame.parquet --output-root <new run root> [--smoke]

PYTHONPATH=bc uv run --no-sync python -m python_models.statistical.backtests.geometry_air_regime_report --run-root <run root> --output-root <new report root> [--smoke]
```

The full run takes about four minutes and the report about thirty seconds. Output roots must not exist. The reporter refuses to run if any module it loads differs from the run's frozen copy, so regenerate a report from the commit that produced the run.

## Next steps

1. Freeze a protocol revision that models the translation per recording pipeline (1989 to 2008, 2009 to 2019, 2020 onward) with a season level inside the 2009 to 2019 pipeline sized by the bridge check's held-out floor, references the 2020 pipeline from the 2023 and 2025 matched games, and declares clue-based bounds and a prior for 1989 to 2008. Do not relax thresholds or adopt concentration 3. Do not read the per-window translation refits as transport evidence; only the gap test is identified.
2. Independent modern confirmation, naturally missing-label validity, historical transport, original-source label reconstruction, measurement/source uncertainty, and reliable aggregate uncertainty all remain open. The separate historical side/location model still fails backward calibration.
3. Upstream source repairs are locally integrated in `/Users/davidroher/Repos/baseball.computer.rs` master `415494c`; compatible source publication and downstream materialization remain outstanding. Do not alter frozen fitting artifacts retroactively.

Runtime prefix used throughout: `PYTHONPATH=bc UV_CACHE_DIR=/private/tmp/bc-audit-uv`. Strict type checking used a scratch basedpyright config with the checker-only stubs at `/private/tmp/bc-air-regime-type-stubs`; both are temporary files. Artifacts under `artifacts/statistical/backtests/geometry_reliability/` are ignored local files, not assumed remotely available.
