Posterior mean run expectancy (expected runs scored before the inning ends) for each of the 24 base-out states, 2015 NL, from `main_models.run_expectancy_summary` (`re-full-eraregime-v4`, `outcome = 'runs_to_end'`, the sole outcome value in the table), ordered by outs then base_state (bit-coded: 1=1B, 2=2B, 4=3B). The fit population is real regular-season events in innings 1 through 8 with a per-state negative-binomial dispersion.

| state | base_state | outs | re_value_mean | re_value_hdi_lower | re_value_hdi_upper |
|-------|-----------:|-----:|--------------:|-------------------:|-------------------:|
| 0_0   | 0          | 0    | 0.482         | 0.443              | 0.519              |
| 0_1   | 1          | 0    | 0.859         | 0.801              | 0.920              |
| 0_2   | 2          | 0    | 1.121         | 1.053              | 1.190              |
| 0_3   | 3          | 0    | 1.435         | 1.337              | 1.528              |
| 0_4   | 4          | 0    | 1.432         | 1.316              | 1.546              |
| 0_5   | 5          | 0    | 1.688         | 1.578              | 1.799              |
| 0_6   | 6          | 0    | 1.986         | 1.852              | 2.129              |
| 0_7   | 7          | 0    | 2.323         | 2.169              | 2.478              |
| 1_0   | 0          | 1    | 0.256         | 0.235              | 0.277              |
| 1_1   | 1          | 1    | 0.504         | 0.468              | 0.541              |
| 1_2   | 2          | 1    | 0.671         | 0.633              | 0.708              |
| 1_3   | 3          | 1    | 0.896         | 0.836              | 0.953              |
| 1_4   | 4          | 1    | 0.981         | 0.925              | 1.039              |
| 1_5   | 5          | 1    | 1.114         | 1.054              | 1.177              |
| 1_6   | 6          | 1    | 1.356         | 1.262              | 1.439              |
| 1_7   | 7          | 1    | 1.559         | 1.447              | 1.663              |
| 2_0   | 0          | 2    | 0.099         | 0.090              | 0.109              |
| 2_1   | 1          | 2    | 0.223         | 0.203              | 0.242              |
| 2_2   | 2          | 2    | 0.294         | 0.277              | 0.312              |
| 2_3   | 3          | 2    | 0.428         | 0.398              | 0.461              |
| 2_4   | 4          | 2    | 0.356         | 0.330              | 0.381              |
| 2_5   | 5          | 2    | 0.473         | 0.436              | 0.510              |
| 2_6   | 6          | 2    | 0.594         | 0.546              | 0.646              |
| 2_7   | 7          | 2    | 0.779         | 0.714              | 0.844              |

Note: against the deterministic `main_models.run_expectancy_matrix` (which carries two decimals), the 2015 NL cells sit within 0.035 runs except `0_4` (-0.078) and `0_7` (+0.123); over the 5,424 cells the two surfaces share across all seasons the median offset is -0.01% and the mean +0.34%, so the systematic 1.1 to 1.4% shortfall of the earlier fit (walk-off-censored innings and no-play rows in its population) is gone.
