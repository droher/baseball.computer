# Joint anchored sensitivity ribbon — trajectory GroundBall share

Per-era expected GroundBall share on the unrecorded (imputed) slice as the joint
selection offset is scaled by `t` along the anchor direction, for the published
trajectory geometry fit `e-v12-noprop-trajectory-shrunk`. `t = 0` is the
missing-at-random default (`δ = 0`), which reproduces the published imputation
share exactly; `t = 1` applies the full anchored offset `δ_raw` for that era; the
grid extends to `t = 1.5` to show super-anchor behavior. The band at `t = 1` is
the minimum-to-maximum GroundBall share as each non-focal class offset is
perturbed by ±0.25 nats one at a time, holding the anchor direction fixed.

The offset is a single GroundBall-only softmax direction (all other class offsets
zero), because the anchor is one-dimensional: trajectory deduction from fielding
strings recovers only ground balls, so the derived slice is 100% GroundBall and
supplies a selection contrast for that one class alone. That is a property of the
record, not a modeling choice.

| era | δ_raw (full anchor, nats) | t=0.0 (MAR) | t=0.25 | t=0.5 | t=0.75 | t=1.0 | t=1.25 | t=1.5 | t=1 perturbation band |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pre-1950  | +1.241 | 0.3204 | 0.3822 | 0.4478 | 0.5151 | 0.5818 | 0.6456 | 0.7047 | [0.5627, 0.5984] |
| 1950-1987 | +0.917 | 0.3760 | 0.4226 | 0.4704 | 0.5186 | 0.5663 | 0.6129 | 0.6575 | [0.5394, 0.5905] |
| 1988+     | +0.835 | 0.3918 | 0.4362 | 0.4811 | 0.5257 | 0.5693 | 0.6111 | 0.6505 | [0.5461, 0.5891] |

<!-- src: artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/exports/sensitivity_ribbon_joint.parquet (sweep_kind=joint_anchor for the t columns; sweep_kind=joint_anchor_perturbed for the band) -->

Headline: for the pre-1950 unrecorded slice the MAR default puts the GroundBall
share at 0.3204; the full anchor raises it to 0.5818, with a ±0.25-nat
perturbation band of [0.5627, 0.5984]. The MAR default understates pre-1950
ground balls by nearly half. The graded surface across `t` is the published
object, not a single point.

## Anchor magnitudes and provenance

The per-era anchor offset is the raw single-class selection log-odds
`δ_raw = log(p_masked / p_obs)` for GroundBall, contrasting the observed slice
(`observed_status='observed'`) against the derived slice
(`observed_status='derived'`) of `main_models.model_input_geometry`:
`+1.241` (pre-1950), `+0.917` (1950-1987), `+0.835` (1988+).
<!-- src: artifacts/statistical/anchors/trajectory/real-run/anchor_offsets.parquet (bucketing='paper', class_label='GroundBall', column delta_raw); artifacts/statistical/anchors/trajectory/real-run/summary.json -->

Class-universe decision: the ribbon recomputes the GroundBall offset over the
paper's classifiable class universe, excluding the unclassifiable bunt labels
`UnspecifiedBunt` and `FoulBunt` from the observed-slice denominator, matching the
geometry model's trajectory vocabulary `{Bunt, Fly, GroundBall, LineDrive,
PopUp}`. Those two labels are 0.01–0.14% of observed rows, so the realignment
moves the offset by under 0.001 nats. Because softmax offsets are identified only
up to an additive constant and the derived slice is a single class, the raw
single-class offset (`δ_raw`, not the degenerate zero-mean centered `δ`) is the
valid joint direction.
<!-- src: artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/exports/sensitivity_ribbon_joint_summary.json (class_universe_decision) -->

Partial-truth assumption: the derived slice recovers only deduced-recoverable
classes — for trajectory, ground balls deduced from fielding strings — so
`p_masked` is the derived slice alone and is a partial-truth estimate of the
masked-slice distribution, not full truth. Rows with `observed_status` outside
`{observed, derived}` (`unknown_code`, `missing`) remain uncharacterized by the
anchor.
<!-- src: artifacts/statistical/anchors/trajectory/real-run/summary.json (partial_truth_assumption) -->

## Regeneration

```
uv run --group stats python scripts/mnar_anchor.py --run-id real-run
just sensitivity-ribbon --joint artifacts/statistical/anchors/trajectory/real-run
```

The first command reads `bc.db` read-only and writes
`artifacts/statistical/anchors/trajectory/real-run/{summary.json,anchor_offsets.parquet}`.
The second consumes that anchor run dir and writes
`sensitivity_ribbon_joint.parquet` + `sensitivity_ribbon_joint_summary.json` beside
the published `e-v12-noprop-trajectory-shrunk` fit. The joint export carries
`era_bucket`, `sweep_kind` (`joint_anchor` | `joint_anchor_perturbed`), `t`,
`perturbed_class`, `perturbation_nats`, `marginal_share`, and `baseline_share`.
