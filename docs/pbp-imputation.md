# Full-history PBP imputation

The additive imputation layer covers existing play-by-play games across the available historical span, including games before 1910 and before 1989. It preserves the source tables and exposes recorded values, estimated values, methods, and uncertainty separately. Box-score and season-only games are outside this project.

The September 13, 2026 source inventory contains 205,886 PBP games and 18,141,020 events from 1903–2025. The registry classifies 179 columns across ten source surfaces: 116 targets and 63 bookkeeping or derived duplicates. The full baseline finds nulls or unspecified codes in 28 target fields; accounting for absent pitch blocks raises the affected target count to 35. This is an inventory of the acquired PBP source schema, not a claim that every historical game or every unrecorded physical measurement has been acquired.

## Outputs

| Surface | Purpose |
| --- | --- |
| `pbp_imputed_games` | Game context, duration, and recorded game results in one view. |
| `pbp_imputed_events` | Existing event spine with estimated counts, geometry, pitch counters, run/win changes, raw evidence, and methods. |
| `pbp_imputed_game_context` | Ten context quantities with raw values, empirical donor counts, and numeric donor dispersion. |
| `pbp_imputed_geometry` | Joint location taxonomy, trajectory, handler, modifiers, strength treatment, and historical airborne standardization. |
| `pbp_imputed_pitches` | Event increments within actual PBP appearances, reconstructed sequences, counts, counters, and conflict dispositions. |
| `pbp_imputed_pitch_items` | Individual imputed sequence items and matched source evidence. |
| `pbp_imputed_pitch_totals` | Counters and derived rates by game, batter, and pitcher. |
| `pbp_imputed_runners` | Recorded and derived destinations, explicit conflict support, and pitcher responsibility. |
| `pbp_imputed_fielding_plays` | Ordered fielding credits, eligible player candidates, and compatible box allocation. |
| `pbp_imputed_fielding_totals` | Counts derived from imputed fielding credits. |
| `pbp_imputed_officials` | Recorded identities, contemporaneous candidate distributions, and explicit unresolved slots. |
| `pbp_imputed_event_values` | Run and win changes for every PBP event, including early and postseason games. |
| `pbp_imputed_park_factors` | Imputed park metrics for park, league, and season combinations present in PBP. |
| `pbp_imputed_run_expectancy` | Expected remaining runs for base/out states occurring in PBP. |
| `pbp_imputed_state_transitions` | Normalized transition distributions for PBP state contexts. |
| `pbp_imputed_linear_weights` | Existing posterior values or explicit deterministic fallbacks for PBP league-seasons. |

All names are in `main_models`. Unconfigured component models have typed empty outputs; the imputed event/game interfaces become populated when a full artifact root is selected. Official tables retain their existing meaning.

## Build and verify

Run from the repository root. The artifact builder opens its database read-only. Its default is a small smoke sample.

```sh
PYTHONPATH=bc uv run --no-sync python -m python_models.imputation \
  --database bc.db --output artifacts/imputation/my-smoke \
  --sample-games 100 --threads 1 --memory-limit 6GB

PYTHONPATH=bc uv run --no-sync python -m python_models.imputation \
  --database bc.db --output artifacts/imputation/my-full \
  --full --threads 1 --memory-limit 8GB
```

`--components` selects `coverage`, `context`, `geometry`, `pitches`, `fielding`, `runners`, `officials`, `event_values`, `park_factors`, `run_expectancy`, `state_transitions`, or `linear_weights`. A component is recorded as ready only after its coverage and integrity checks pass. `--resume` reuses verified imputed components and builds newly requested ones; the source database content must match the original fingerprint. Imputed files are never silently replaced. Retain failed component files for diagnosis before explicitly removing or moving those generated files for a retry.

The root manifest records the exact source database SHA-256, scope, settings, component hashes, and validation counts. Each component also binds its SQL, frozen implementation files, and any intermediate allocation dependencies. Fielding retains both its raw candidate output and the assignment patch. `components_complete` means the requested components finished; `project_complete` stays false until whole-project reconciliation and release are verified.

Full geometry, pitch, fielding, runner, and event-value builds use disjoint five-year partitions. Each imputed partition is a reusable checkpoint bound to its SQL, schema, source snapshot, and implementation. Failed attempts remain available for diagnosis. The final component binds every contributing partition and dependency; partitioning does not change the donor population.

Set `BC_PBP_IMPUTATION_ROOT` to the absolute full-build directory when planning the imputed models. SQLMesh rejects smoke, failed, missing, tampered, or incompatible artifacts. Use the development gateway and inspect the selected plan before production promotion. A full-population component export does not itself publish anything.

For legacy statistical consumers, changing `BC_STATS_ARTIFACTS_ROOT` alone does not change the SQLMesh model fingerprint. A development restatement can rebuild a candidate physical table while the consumer view still references its production counterpart. Check artifact identities in the actual consumer schema. Test the production restatement procedure inside an isolated database and state copy when verifying a change of legacy artifact roots.

The publish workflow accepts the same absolute path through its `BC_PBP_IMPUTATION_ROOT` repository variable on the self-hosted runner. All sixteen imputed surfaces have positive row-count guards. An unset or unusable root cannot silently publish empty imputed tables.

Before publication, run the independent pitch audit over the selected full artifact and retain the grouped coverage report under the same artifact root:

```sh
PYTHONPATH=bc uv run --no-sync python -m python_models.imputation.pitch_validation \
  --input artifacts/imputation/my-full/pitches.parquet \
  --output artifacts/imputation/my-full/validation/pitch \
  --threads 1 --memory-limit 4GB
```

The publisher reruns candidate validation against the production `main_models` tables before attaching or writing the DuckLake catalog. It checks full PBP population coverage, all 116 completion mappings, exact artifact identities in all sixteen materialized consumers, independent pitch evidence, grouped coverage evidence, and pitch/fielding rollup conservation. A same-sized stale table, NULL counter contribution, partial-history build, or missing report fails the gate. Recorded source conflicts remain separately counted.

## September 13 candidate evidence

The imputed candidate artifact root is `artifacts/imputation/20260913-pbp-imputed-v1`. Its eleven families contain the source-equivalent candidate rows and retained component evidence; new validation is pending.

Read the files selected by the manifest. This retained build selects `geometry_v2.parquet`, which repairs 1,123 early non-bunt standardization gaps. The superseded `geometry.parquet` and failed build attempts remain for diagnosis; they are not the release inputs. Bunts retain their explicit standardization non-applicability.

The independent pitch audit passed across 15,822,702 appearances: all seventeen counter checks were zero, with no unknown tokens, unclassified dispositions, or unflagged appearance violations. Explicit source-conflict evidence includes 26,996 ball-boundary, 51,245 strike-boundary, 816 post-terminal, and 375 terminal-outcome violations. These categories can overlap; they are disclosed conflicts, not verified legal historical sequences.

The full coverage breakdown reconciles all 116 target fields by era, league, source type, and game type, with no baseline or grouping mismatches. `completion_registry.json` binds each target to its actual output and evidence columns. The source snapshot is SHA-256 `fc94ec74507ceb0e7ab60296006de534253d364b0c9d5b64f352c82749c23f9c`.

The renamed candidate passes all sixteen model audits and the full publication validator in `main_models__pbp_imputed_verification`. A separate namespace check confirms sixteen `pbp_imputed_*` surfaces and no old model-name metadata. All eleven component data files are byte-identical to the prior candidate; the 116 output mappings use the new names. The rename passes 141 targeted tests, Ruff, and strict type checks. The retained release bundle is `artifacts/imputation/release-candidate-v2`; production promotion and public publication remain pending approval.

The legacy promotion rehearsal rebuilt all twelve consumers inside the isolated database copy. All model audits and 56 subsequent identity, schema, row-count, geometry-label, and probability checks passed against its `main_models` schema. The PBP candidate passed reconciliation again after that rehearsal. The original production database checksum remains unchanged.

## Meaning and limitations

- Context estimates sample comparable PBP donors, backing off through park, month, decade, league, and broader history. Reported donor dispersion is conditional empirical variation, not a calibrated interval for total historical uncertainty.
- Geometry uses fixed full-history donor populations and normalized empirical distributions with broad fallback. General location determines global side, depth, and edge jointly. The legacy six-class angle-modifier artifact is exposed as `location_angle`; it cannot be confused with the current global-side taxonomy.
- The geometry builder caches distributions by their conditioning keys before joining events. Comparison with the original implementation on 1,000 games preserved selected classes and methods; probability differences were below `4e-16`.
- `Default` contact strength is an unspecified or neutral source code. It is preserved with its own method; missing strength receives a declared neutral/unspecified proxy. It is not converted into a falsely observed `Hard` or `Soft` extreme.
- Pre-1989 airborne standardization transports the nearest available 1989 translation backward with an explicit weak-identification flag. The existing airborne research decision remains closed; this adds coverage rather than new confirmation evidence.
- Pitch estimates conserve their defined counters and disclose rough token assumptions. Raw parser statuses and sequences are never rewritten. Known source contradictions must remain visible; source parsing resolution is not evidence that a reconstructed sequence is historically accurate.
- Runner identity at the recorded end state resolves 634 of the 642 source-not-out rows lacking an ending base. Eight terminal/frame cases retain explicit conflict support. Original base markers remain available even when the runner was out.
- Compatible fielding allocation uses official totals minus raw known credits at the same grain as the unknown rows. It requires eligible personnel and an exact integer-capacity match. Partial or contradictory evidence receives an explicit disposition; it is never labeled as conserved.
- `aggregate_constraint_complete` describes availability of the aggregate constraint inputs. Successful allocation is identified by `constraint_disposition='aggregate_capacity_assignment_satisfied'` and a zero allocation delta. Complete inputs can still be incompatible with the event records.
- Missing official identities use same-season, league, game-type, and role candidates when available. Unresolved slots are not person identifiers. Missing secondary umpire roles may mean absence or unrecorded presence, so observed role frequency is not presented as the probability that the role existed.
- Derived values retain existing nonmissing estimates. Missing event run/win changes use available expectancy matrices and then pooled base/out or neutral win priors. Missing park factors use nearby seasons for the same park and league before broader pools or a neutral factor. Run expectancy and transition distributions can transport estimates from another season; transitions transport whole normalized vectors. Reported transported dispersions are conditional donor uncertainty. Linear weights fall back to the existing deterministic estimates.
- These are exploratory estimates. Conservation, normalization, source preservation, and full row coverage do not establish historical predictive accuracy. No sealed confirmation labels are acquired or rescored by the builder.

The older statistical pointer gate and its historical manifests remain separate evidence. The new imputation artifacts do not restamp an old fit as validated or repair a missing legacy inference file. See the [evidence contract](modeling-evidence-contract.md) and [imputation plan](../notes/full-history-imputation-plan.md) for the remaining release checks.

## Recorded-context stress test

A 1,000-game test withheld all ten context fields jointly for complete seasons 1903, 1910, 1940, 1960, 1980, 2000, and 2025. No held-out value remained in its field's donor pool. Against the recorded test values, mean absolute errors were 7.53 degrees Fahrenheit, 10,227 attendees, 4.32 mph of wind, 25.67 minutes of duration, and 234.34 clock minutes for start time. Categorical exact agreement ranged from 19.7% for wind direction to 94.0% for precipitation.

This deliberately removes context that is often available when filling one missing field. It demonstrates how rough joint historical reconstructions can be; it does not calibrate historical missingness, identify an unrecorded game condition, or validate the separate air-trajectory model. The retained report and reproducible query are under `artifacts/imputation/20260913-context-holdout-v1`.
