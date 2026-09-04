# Trajectory unrecorded slice — GroundBall derived-slice bounds

Regeneration: `uv run --group stats python scripts/mnar_anchor.py --run-id bound-run`
(3.1 s wall-clock; reads the frozen `model_input_geometry` dataset `e-v12` the
published trajectory fit `e-v12-noprop-trajectory-shrunk` was trained on, plus
that fit's MAR export `exports/geometry_probabilities.parquet`, both read-only;
writes `artifacts/statistical/anchors/trajectory/bound-run/{derived_slice_bounds.parquet,summary.json}`).
The frozen dataset is used rather than prod `bc.db` so the numbers reproduce
against the same rows the fit scored; `--source db` reads `bc.db` instead.

The trajectory slice of the dataset has three `observed_status` values:
`observed` (scorer recorded the trajectory), `derived` (trajectory deduced from
the fielding string; every such row is `GroundBall`), and `unknown_code`
(nothing recorded, nothing deducible). The MAR export scores exactly the
unrecorded slice (`derived` + `unknown_code`, 5,867,709 events) and none of the
observed slice. Class universe is the model vocabulary `{Bunt, Fly, GroundBall,
LineDrive, PopUp}`, with the bunt variant labels remapped to `Bunt` by
`GEOMETRY_DIMENSIONS['trajectory'].remap`; the observed share is over that
vocabulary.

| era | n_observed | observed GB share | n_unrecorded | n_derived (all GB) | n_unknown | lower bound P(GB \| unrecorded) | MAR share, unrecorded | MAR share, unknown rows | point estimate | MAR mean p(GB) on derived rows | log loss on derived rows |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pre-1950 | 712,469 | 0.2892 | 2,615,579 | 763,993 | 1,851,586 | 0.2921 | 0.3204 | 0.3204 | 0.5189 | 0.3203 | 1.3590 |
| 1950-1987 | 699,353 | 0.3997 | 3,117,160 | 1,057,776 | 2,059,384 | 0.3393 | 0.3760 | 0.3627 | 0.5790 | 0.4020 | 1.0949 |
| 1988+ | 4,759,451 | 0.4337 | 134,970 | 53,922 | 81,048 | 0.3995 | 0.3918 | 0.4028 | 0.6414 | 0.3751 | 1.5710 |

<!-- src: artifacts/statistical/anchors/trajectory/bound-run/derived_slice_bounds.parquet (bucketing='paper', class_label='GroundBall'; columns n_observed, observed_share, n_unrecorded, n_derived_class, n_unknown, share_lower_bound, mar_share_unrecorded, mar_share_unknown, share_point_estimate, mar_share_on_derived, mar_log_loss_on_derived); artifacts/statistical/anchors/trajectory/bound-run/summary.json (elapsed_seconds, slice_row_counts) -->

Column definitions, per era with `n_unrecorded = n_derived + n_unknown`:

- lower bound `= n_derived / n_unrecorded`. Every derived event is an
  unrecorded event whose trajectory is known to be GroundBall, so
  P(GB | unrecorded) cannot be below this. It is a hard floor, not an
  estimate.
- MAR share, unrecorded `=` the published export's mean p(GB) over the whole
  unrecorded slice; this is the `delta = 0` marginal of the sensitivity ribbon.
- MAR share, unknown rows `=` the same mean restricted to the `unknown_code`
  rows.
- point estimate `= (n_derived + sum of p(GB) over unknown rows) / n_unrecorded`:
  truth on the derived rows, MAR applied only to the rows whose class is
  genuinely unknown. It is a partial-truth estimate under the assumption that
  MAR holds on the unknown remainder, and that assumption is doubtful in a
  specific direction: deduction pulls every ground ball with a deducible
  fielding string into the derived slice, so the remainder is depleted of
  ground balls relative to the unrecorded slice as a whole. Read it as what MAR
  on the remainder implies, not as a corrected share.
- MAR mean p(GB) on derived rows and its log loss score the published MAR
  shares against a truth of 1.0 on the derived rows. The export gives derived
  rows essentially the same p(GB) as unknown rows (0.3203 vs 0.3204 pre-1950),
  because the fielding string that identifies them is not a model covariate;
  the numbers measure how far the MAR shares sit from truth on the one
  unrecorded subslice where truth is known.

What changed from the previous version of this table: it pooled the observed
and derived slices into an "observed + derived" ground share (0.680 pre-1950)
and read its gap over the observed share as evidence of selective
under-recording. That pooled share mixes two slices with different selection
and is not an estimate of any population quantity, and the anchor offset built
from the same contrast, `log(p_derived / p_obs)`, was `-ln(p_obs_GB)`, a
function of the observed slice alone (modeling review H2). The bound column is
the quantity the derived slice actually supports.
