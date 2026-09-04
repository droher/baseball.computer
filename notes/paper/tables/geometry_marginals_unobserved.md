Mean posterior expected share of each trajectory class (Model E, `imputed_batted_ball_geometry`, `e-v12-noprop-trajectory-shrunk`) among events whose batted-ball trajectory was not directly recorded by the official scorer, by era bucket; this table's grain is already restricted to that unobserved/imputed slice, so no additional propensity join was needed.

| era_bucket | class_label | avg_expected_share | n_rows  |
|------------|-------------|--------------------:|--------:|
| pre-1950   | Bunt        | 0.076               | 2615579 |
| pre-1950   | Fly         | 0.224               | 2615579 |
| pre-1950   | GroundBall  | 0.320               | 2615579 |
| pre-1950   | LineDrive   | 0.224               | 2615579 |
| pre-1950   | PopUp       | 0.155               | 2615579 |
| 1950-1987  | Bunt        | 0.068               | 3117160 |
| 1950-1987  | Fly         | 0.087               | 3117160 |
| 1950-1987  | GroundBall  | 0.376               | 3117160 |
| 1950-1987  | LineDrive   | 0.305               | 3117160 |
| 1950-1987  | PopUp       | 0.165               | 3117160 |
| 1988+      | Bunt        | 0.100               | 134970  |
| 1988+      | Fly         | 0.229               | 134970  |
| 1988+      | GroundBall  | 0.392               | 134970  |
| 1988+      | LineDrive   | 0.217               | 134970  |
| 1988+      | PopUp       | 0.063               | 134970  |

Note: `class_label` for `geometry_dimension = 'trajectory'` has 5 distinct values in the data (Bunt, Fly, GroundBall, LineDrive, PopUp), giving 15 rows (3 era buckets × 5 classes). `n_rows` is constant within an era bucket because `expected_share` rows are dense across all 5 classes for every imputed event (one row per class per event, shares summing to 1). The trajectory rows cover 5,867,709 distinct event_keys against 18,141,020 total events in `event_states_full` (about 32%), consistent with a restriction to the not-directly-recorded slice rather than all batted-ball events; the four location dimensions each cover 6,989,832 events.

## Location dimensions, deep-free refit (2026-09-04)

Pooled mean expected share by class for the three location dimensions now published under the `gamma_dl_zero` flavor (`e-v12-noprop-location_{side,depth,edge}-zero`), over all 6,989,832 imputed events per dimension. Era-bucket means differ from the pooled values by at most 0.02 (for example `location_depth` `Default` is 0.605 pre-1950, 0.605 in 1950-1987, and 0.587 in 1988+), so only the pooled shares are shown.

| geometry_dimension | class_label | avg_expected_share |
|---|---|---:|
| location_side | Default | 0.692 |
| location_side | Middle | 0.135 |
| location_side | FoulLine | 0.054 |
| location_side | Left | 0.052 |
| location_side | Right | 0.045 |
| location_side | Foul | 0.022 |
| location_depth | Default | 0.604 |
| location_depth | Deep | 0.185 |
| location_depth | Shallow | 0.161 |
| location_depth | ExtraDeep | 0.050 |
| location_edge | Middle | 0.623 |
| location_edge | Left | 0.195 |
| location_edge | Right | 0.173 |
| location_edge | All | 0.010 |

Query: `SELECT geometry_dimension, class_label, ROUND(AVG(expected_share), 3), COUNT(*) FROM main_models.imputed_batted_ball_geometry WHERE geometry_dimension IN ('location_side', 'location_depth', 'location_edge') GROUP BY ALL`. The previously published location surfaces carried the constant per-class shift disclosed in the paper's §6 (`location_edge` `All` at 0.173, `location_side` `Default` at 0.24, `location_depth` `ExtraDeep` at 0.22); the refit shares sit on the training-slice shares (0.009, 0.70, 0.06).
