# Trajectory sensitivity ribbon against the derived-slice bound — GroundBall

Regeneration:

```
uv run --group stats python scripts/mnar_anchor.py --run-id bound-run
just sensitivity-ribbon --bound artifacts/statistical/anchors/trajectory/bound-run
```

The first command (3.1 s) writes the derived-slice bounds under
`artifacts/statistical/anchors/trajectory/bound-run/`. The second (2.7 s) reads
that run dir plus the published trajectory fit `e-v12-noprop-trajectory-shrunk`
(export `exports/geometry_probabilities.parquet`, era attached by joining
`event_key` to the frozen `e-v12` dataset parquet, both read-only) and writes
`exports/sensitivity_ribbon_by_era.parquet`, `exports/trajectory_bound_offset.parquet`
and `exports/trajectory_bound_offset_summary.json` beside the fit.

Per era, the corrected GroundBall share on the unrecorded (imputed) slice as
the GroundBall selection offset `delta` sweeps the marginal ribbon grid (all
other class offsets 0; `delta = 0` is the published MAR share), the lower bound
P(GB | unrecorded) >= n_derived / n_unrecorded from `groundball_mnar.md`, and
`delta_GB` at bound: the smallest offset at which the corrected share reaches
the bound, found by bisection on the same reweight.

| era | delta=-1.0 | -0.5 | -0.25 | 0 (MAR) | +0.25 | +0.5 | +1.0 | lower bound | delta_GB at bound (nats) | bound inside +-1.0 grid |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| pre-1950 | 0.1621 | 0.2325 | 0.2744 | 0.3204 | 0.3699 | 0.4221 | 0.5302 | 0.2921 | -0.1514 | yes |
| 1950-1987 | 0.2021 | 0.2818 | 0.3274 | 0.3760 | 0.4270 | 0.4793 | 0.5836 | 0.3393 | -0.1871 | yes |
| 1988+ | 0.2071 | 0.2918 | 0.3403 | 0.3918 | 0.4451 | 0.4989 | 0.6026 | 0.3995 | +0.0367 | yes |

<!-- src: artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/exports/sensitivity_ribbon_by_era.parquet (class_label='GroundBall', marginal_share by delta_logodds); artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/exports/trajectory_bound_offset.parquet (target_share, delta_at_target, target_in_grid); artifacts/statistical/bayes/geometry_trajectory/e-v12-noprop-trajectory-shrunk/exports/trajectory_bound_offset_summary.json (elapsed_seconds) -->

Reading: the grid contains the bound in every era, and the offset needed to
respect it is small. Pre-1988 the MAR share already sits above the floor
(`delta_GB` is negative: the share could fall by 0.15-0.19 nats before
violating it). In 1988+ the MAR share (0.3918) sits 0.008 below the floor
(0.3995) and needs `delta_GB = +0.037` nats to reach it, so the MAR default is
inconsistent with the record there by a small margin. The floor is weak
relative to the grid, which is the point: the derived slice constrains the
GroundBall share from below only, and the +-1.0 band (0.16-0.53 pre-1950)
remains an assumed range, not a data-identified one.

The earlier "joint anchored ribbon" in `joint_ribbon_trajectory.md` scaled a
per-era offset `log(p_derived / p_obs)`; because the derived slice is 100%
GroundBall that offset was `-ln(p_obs_GB)`, a function of the observed slice
alone, and the 0.58 pre-1950 share it produced carried no information about the
unrecorded slice. The bound column replaces it.
