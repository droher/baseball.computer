Count of games by primary source type (play-by-play, box score, or gamelog), grouped by decade, from `main_models.game_start_info`.

| decade | box_score_games | gamelog_games | play_by_play_games |
|-------:|-----------------:|---------------:|--------------------:|
| 1870   | 537               | 1489            | 0                    |
| 1880   | 0                 | 8759            | 0                    |
| 1890   | 1841              | 7627            | 0                    |
| 1900   | 11341             | 5               | 41                   |
| 1910   | 7                 | 1               | 13391                |
| 1920   | 131               | 0               | 12477                |
| 1930   | 220               | 1               | 13222                |
| 1940   | 1594              | 0               | 13353                |
| 1950   | 1                 | 1               | 12453                |
| 1960   | 0                 | 0               | 16041                |
| 1970   | 0                 | 1               | 19955                |
| 1980   | 0                 | 0               | 20523                |
| 1990   | 0                 | 0               | 21832                |
| 2000   | 0                 | 0               | 24623                |
| 2010   | 0                 | 0               | 24663                |
| 2020   | 0                 | 0               | 13312                |

Total events in the play-by-play corpus (`SELECT COUNT(*) FROM main_models.event_states_full`): **18,141,020**, matching the documented ~18.1M figure.
