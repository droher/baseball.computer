Mean posterior probability that a batted-ball geometry dimension was directly observed by the official scorer (Model A, `scorer_observation_propensities`, `10k-v6-unseen-fix` for all six dimensions), averaged by decade of the event's season and by dimension.

| decade | trajectory | location_side | location_depth | location_edge | general_location | ball_handler_position |
|-------:|-----------:|--------------:|---------------:|--------------:|-----------------:|----------------------:|
| 1910   | 0.247      | 0.040         | 0.056          | 0.047         | 0.055            | 0.819                 |
| 1920   | 0.285      | 0.102         | 0.092          | 0.077         | 0.077            | 0.868                 |
| 1930   | 0.203      | 0.053         | 0.042          | 0.070         | 0.046            | 0.815                 |
| 1940   | 0.150      | 0.034         | 0.028          | 0.033         | 0.041            | 0.741                 |
| 1950   | 0.202      | 0.058         | 0.057          | 0.054         | 0.050            | 0.903                 |
| 1960   | 0.187      | 0.071         | 0.082          | 0.060         | 0.062            | 0.931                 |
| 1970   | 0.187      | 0.037         | 0.039          | 0.054         | 0.043            | 0.943                 |
| 1980   | 0.300      | 0.210         | 0.199          | 0.189         | 0.196            | 0.912                 |
| 1990   | 0.953      | 0.935         | 0.955          | 0.952         | 0.946            | 0.938                 |
| 2000   | 0.979      | 0.949         | 0.956          | 0.954         | 0.953            | 0.946                 |
| 2010   | 0.975      | 0.958         | 0.948          | 0.956         | 0.960            | 0.948                 |
| 2020   | NULL       | 0.910         | 0.979          | 0.930         | 0.945            | 0.943                 |

Note: the 2020 `trajectory` cell is NULL because no events in that decade have a Model A posterior for that dimension; see the QA note below. All other cells are non-null decade-level means. Against the previously published fits the trajectory and location decade means move by at most 0.007 and `ball_handler_position` rises by 0.01 to 0.05 before 1990.

Note (QA, confirmed cause): `prepare_event_observation_inputs` (`bc/python_models/statistical/models/_event_data.py`) drops any season whose rare-class count is < `MIN_RARE_CLASS_COUNT_PER_SEASON=100` or rare-class rate is < `MIN_RARE_CLASS_RATE_PER_SEASON=0.005`, and `build_observation_scoring_frame` scores only the surviving seasons; dropped seasons get no exported `p_observed_mean` row at all. For `trajectory`, every 2020s season fails that filter: unobserved-row counts (rare class) in `model_input_observation_batted_ball` are season 2020=6, 2021=31, 2022=2, 2023=2, 2024=1 (season is effectively 100% observed by scoring convention), so all six 2020-2025 seasons are dropped and `scorer_observation_propensities` has zero `trajectory` rows for the whole decade, hence the decade-average NULL. The four location dimensions are saturated too from 2021 on (rare rates 0.09%-0.02%), but season 2020 alone clears both thresholds (rare_count=1444, rare_rate=3.1%), so those dims keep exactly one 2020s season in the published artifact (`location_side/depth/edge/general_location` rows are all season 2020; `ball_handler_position` covers all six seasons, since its rare class never saturates). This is intended behavior, explicitly documented in the `prepare_event_observation_inputs` docstring: a missing per-event `p_observed_mean` should be read by downstream consumers as "fully observed by construction" (p≈1), not a modeling gap. If the paper wants the 2020 trajectory cell to show a number instead of NULL, the by-decade query (not the model) should `COALESCE` missing posterior means to 1.0 per that documented convention.
