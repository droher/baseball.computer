Share of batted-ball offense events with unknown (directly-recorded) trajectory and unknown batted-ball location, by decade, from the `offense_events` BSL table (`main_models.event_offense_stats` joined to `main_models.team_game_start_info`).

| decade | share_unknown_trajectory | share_unknown_location |
|-------:|--------------------------:|-------------------------:|
| 1900   | 0.497                     | 0.386                    |
| 1910   | 0.508                     | 0.428                    |
| 1920   | 0.479                     | 0.388                    |
| 1930   | 0.580                     | 0.467                    |
| 1940   | 0.655                     | 0.545                    |
| 1950   | 0.553                     | 0.408                    |
| 1960   | 0.526                     | 0.382                    |
| 1970   | 0.535                     | 0.416                    |
| 1980   | 0.460                     | 0.357                    |
| 1990   | 0.017                     | 0.012                    |
| 2000   | 0.013                     | 0.008                    |
| 2010   | 0.010                     | 0.006                    |
| 2020   | 0.00003                   | 0.00002                  |

No decade before 1900 appears because both `trajectory_known` and `trajectory_unknown` are 0 for those events (no batted-ball classification available in that era's sources). The share of unknown trajectory/location drops by roughly two orders of magnitude between the 1980s and 1990s, consistent with the shift to detailed batted-ball location strings in Retrosheet play-by-play files starting around 1990.
