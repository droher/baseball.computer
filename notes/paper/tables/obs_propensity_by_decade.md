Mean posterior probability that a batted-ball geometry dimension was directly observed by the official scorer (Model A, `scorer_observation_propensities`), averaged by decade of the event's season and by dimension.

| decade | trajectory | location_side | location_depth | location_edge | general_location | ball_handler_position |
|-------:|-----------:|--------------:|---------------:|--------------:|-----------------:|----------------------:|
| 1910   | 0.252      | 0.042         | 0.059          | 0.05          | 0.057            | 0.765                 |
| 1920   | 0.289      | 0.106         | 0.095          | 0.081         | 0.08             | 0.829                 |
| 1930   | 0.206      | 0.054         | 0.043          | 0.072         | 0.047            | 0.778                 |
| 1940   | 0.152      | 0.035         | 0.029          | 0.034         | 0.042            | 0.705                 |
| 1950   | 0.205      | 0.059         | 0.058          | 0.055         | 0.051            | 0.886                 |
| 1960   | 0.189      | 0.073         | 0.083          | 0.061         | 0.063            | 0.923                 |
| 1970   | 0.191      | 0.039         | 0.04           | 0.057         | 0.044            | 0.934                 |
| 1980   | 0.305      | 0.216         | 0.205          | 0.196         | 0.202            | 0.897                 |
| 1990   | 0.954      | 0.936         | 0.955          | 0.953         | 0.947            | 0.938                 |
| 2000   | 0.979      | 0.949         | 0.956          | 0.954         | 0.953            | 0.946                 |
| 2010   | 0.975      | 0.958         | 0.948          | 0.956         | 0.96             | 0.948                 |
| 2020   | NULL       | 0.91          | 0.979          | 0.93          | 0.946            | 0.942                 |

Note: the 2020 `trajectory` cell is NULL — no events in that decade have a Model A posterior for that dimension (likely a coverage gap in the published artifact, not literal zero probability). All other cells are non-null decade-level means.

Note (QA, confirmed cause): `prepare_event_observation_inputs` (`bc/python_models/statistical/models/_event_data.py`) drops any season whose rare-class count is < `MIN_RARE_CLASS_COUNT_PER_SEASON=100` or rare-class rate is < `MIN_RARE_CLASS_RATE_PER_SEASON=0.005`, and `build_observation_scoring_frame` scores only the surviving seasons — dropped seasons get no exported `p_observed_mean` row at all. For `trajectory`, every 2020s season fails that filter: unobserved-row counts (rare class) in `model_input_observation_batted_ball` are season 2020=6, 2021=31, 2022=2, 2023=2, 2024=1 (season is effectively 100% observed by scoring convention), so all six 2020-2025 seasons are dropped and `scorer_observation_propensities` has zero `trajectory` rows for the whole decade — hence the decade-average NULL. The four location dimensions are saturated too from 2021 on (rare rates 0.09%-0.02%), but season 2020 alone clears both thresholds (rare_count=1444, rare_rate=3.1%), so those dims keep exactly one 2020s season in the published artifact (confirmed: `scorer_observation_propensities` join shows `location_side/depth/edge/general_location` = 46,517 rows, all season 2020; `ball_handler_position` = all six seasons, since its rare class never saturates). This is intended behavior, explicitly documented in the `prepare_event_observation_inputs` docstring: a missing per-event `p_observed_mean` should be read by downstream consumers as "fully observed by construction" (p≈1), not a modeling gap. Recommendation: document-only fix here is sufficient for this table; if the paper wants the 2020 trajectory cell to show a number instead of NULL, the by-decade query (not the model) should `COALESCE` missing posterior means to 1.0 per that documented convention rather than treating the cell as a literal missing estimate.
