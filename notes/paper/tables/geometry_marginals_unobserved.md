Mean posterior expected share of each trajectory class (Model E, `imputed_batted_ball_geometry`) among events whose batted-ball trajectory was not directly recorded by the official scorer, by era bucket — this table's grain is already restricted to that unobserved/imputed slice, so no additional propensity join was needed.

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

Note: substituted 5 trajectory classes (Bunt, Fly, GroundBall, LineDrive, PopUp) for the 4 assumed in the task spec — `class_label` for `geometry_dimension = 'trajectory'` actually has 5 distinct values in the data, giving 15 rows (3 era buckets × 5 classes) rather than 12. `n_rows` is constant within an era bucket because `expected_share` rows are dense across all 5 classes for every imputed event (one row per class per event, shares summing to 1). Verified the "already unobserved" claim: `imputed_batted_ball_geometry` trajectory rows cover 5,867,709 distinct event_keys against 18,141,020 total events in `event_states_full` (~32%), consistent with a restriction to the not-directly-recorded slice rather than all batted-ball events.
