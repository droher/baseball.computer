# Synthetic-lineup backtest, 1871-1910

Backtest of `synthetic_box_score.lineup_assignments` against real Retrosheet box-score lineups.
The optimizer is run on box-score-era games where real lineups exist, using only the gamelog-only feature set (Lahman/Databank season inputs + gamelog starting pitcher + DH flag). Results are compared to the real assignments.

## Methodology

- Seasons in scope: 1871, 1872, 1874, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910
- Games scored: 14963 (29926 game-sides, 269334 starter slots)
- Optimizer runtime: 38.1s
- 51 eligible games were excluded because the gamelog row is missing or has NULL starting pitchers (input-parity caveat).
- Inputs: `stg_databank_appearances`, `stg_databank_batting`, gamelog starting pitchers, DH flag. No box-score data on the input side.
- Modal lineups are recomputed from the candidate inputs via `compute_modal_lineups`, not read from a persisted table.
- Truth: `stg_box_score_batting_lines.nth_player_at_position = 1` joined to `stg_box_score_fielding_lines.nth_position_played_by_player = 1` on `(game_id, side, batter_id = fielder_id)`.

**Definitions.**

- *Wrong starters* per game-side = number of real starters the synthetic lineup didn't include (= 9 − |syn_set ∩ real_set|). Per game = sum across both sides, max 18.
- *Wrong positions* per game-side = number of correctly-included starters at the wrong fielding position. Per game = sum across both sides, max 18.
- *Per-player rates* are computed on (season, team, player) tuples: `miss_rate = missed / real_starts`; `add_rate = added / syn_starts`; `wrong_pos_rate = wrong_pos / correct_starter_games`.
- The starting pitcher is an input to the optimizer; pitcher-position errors are near-trivial except in orphan team-seasons.
- MILP tie-breaking is not strictly deterministic across runs; positional swaps among tied solutions shift wrong-position counts by a few hundredths per game between runs. Wrong-starter counts are stable.

## Coverage caveat: missing team-seasons

- All (season, team) pairs present in truth are also present in synthetic. No coverage gap.

## Headline error counts

Across all sides (orphans included):

- Wrong starters per game: 4.000 (per side: 2.000) out of 9
- Wrong positions per game (right starter, wrong defensive position): 1.450 (per side: 0.725)

## Errors per game by era × league

| era | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- |
| 1871-1880 | NA | 1074 | 2.279 | 3.119 |
| 1891-1900 | NL | 4818 | 4.128 | 1.417 |
| 1901-1910 | AL | 11994 | 4.042 | 1.488 |
| 1901-1910 | NL | 12040 | 4.061 | 1.276 |

## Wrong-defender rate by truth fielding position

For each defensive position, fraction of game-sides where the synthetic lineup did *not* put the same player at that position as the real lineup. Counts both 'wrong player' and 'right player at a different position' as errors.

| position_group | sides | wrong_sides | wrong_per_game |
| --- | --- | --- | --- |
| C | 29926 | 16023 | 1.071 |
| IF | 119704 | 35459 | 0.592 |
| OF | 89779 | 30068 | 0.67 |
| P | 29926 | 1 | 0.0 |

By individual fielding position:

| fielding_position | sides | wrong_sides | wrong_per_game |
| --- | --- | --- | --- |
| 1 | 29926 | 1 | 0.0 |
| 2 | 29926 | 16023 | 1.071 |
| 3 | 29926 | 9016 | 0.603 |
| 4 | 29926 | 9311 | 0.622 |
| 5 | 29926 | 9021 | 0.603 |
| 6 | 29926 | 8111 | 0.542 |
| 7 | 29927 | 9494 | 0.634 |
| 8 | 29926 | 9820 | 0.656 |
| 9 | 29926 | 10754 | 0.719 |

## Per-player rates by truth player's roster churn

Classification on each (season, team, real-player) tuple. multi-stint = the player had >1 stints in the league that season; single-stint full = stint=1 and games_total >= 80% of team games; single-stint partial = otherwise. Aggregated over all real-player game-sides for the bucket.

| churn | real_starts_total | missed_total | wrong_pos_total | miss_rate_pct | wrong_pos_rate_pct |
| --- | --- | --- | --- | --- | --- |
| multi-stint | 23836 | 6303 | 2407 | 26.44 | 13.73 |
| single-stint full | 129335 | 10887 | 11103 | 8.42 | 9.37 |
| single-stint partial | 116158 | 42666 | 8179 | 36.73 | 11.13 |

## Errors per game by team games-played bin

| team_games_bin | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- |
| 130+ | 28852 | 4.064 | 1.387 |
| 60-100 | 136 | 1.309 | 4.015 |
| <60 | 938 | 2.42 | 2.989 |

## Best 10 team-seasons (fewest wrong starters per game)

| season | team_id | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- | --- |
| 1874 | NY2 | NA | 65 | 0.554 | 4.369 |
| 1872 | BS1 | NA | 48 | 0.833 | 2.292 |
| 1871 | RC1 | NA | 25 | 0.96 | 3.52 |
| 1871 | TRO | NA | 29 | 1.103 | 3.655 |
| 1872 | PH1 | NA | 47 | 1.191 | 2.936 |
| 1904 | BOS | AL | 157 | 1.325 | 0.0 |
| 1871 | NY2 | NA | 33 | 1.333 | 3.697 |
| 1871 | PH1 | NA | 28 | 1.5 | 0.643 |
| 1901 | BOS | AL | 138 | 1.58 | 0.275 |
| 1872 | CL1 | NA | 22 | 1.636 | 3.091 |

## Worst 10 team-seasons (most wrong starters per game)

| season | team_id | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- | --- |
| 1910 | CHA | AL | 156 | 7.462 | 1.603 |
| 1872 | BR1 | NA | 26 | 6.538 | 2.769 |
| 1909 | CLE | AL | 155 | 6.529 | 1.587 |
| 1907 | WS1 | AL | 154 | 6.506 | 4.325 |
| 1910 | CLE | AL | 161 | 6.447 | 2.609 |
| 1909 | BSN | NL | 155 | 6.232 | 1.123 |
| 1899 | CIN | NL | 157 | 6.217 | 2.268 |
| 1909 | BRO | NL | 155 | 6.103 | 2.465 |
| 1909 | NYA | AL | 153 | 5.83 | 2.732 |
| 1902 | CHN | NL | 143 | 5.818 | 1.86 |

## Per-player error rates

- (season, team, player) tuples: 5556
- Synthetic count == real count AND every shared start at the right position: 2445 (44.01%)
- Synthetic count == real count, but some position errors: 417 (7.51%)
- Synthetic over-starts the player: 1295 (23.31%)
- Synthetic under-starts the player: 1399 (25.18%)

**Top 10 most-missed real starters** (real_starts ≥ 30, sorted by missed/real_starts):

| season | team_id | player_id | real_starts | syn_starts | missed | miss_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1903 | PHI | zimmc101 | 34 | 33 | 31 | 91.18 |
| 1905 | PIT | ganlb101 | 32 | 31 | 28 | 87.5 |
| 1908 | CIN | hobld101 | 32 | 31 | 28 | 87.5 |
| 1898 | WSN | myerb102 | 31 | 31 | 27 | 87.1 |
| 1906 | CLE | buelf101 | 31 | 31 | 27 | 87.1 |
| 1909 | CLE | bemih101 | 30 | 34 | 26 | 86.67 |
| 1906 | PHI | wardj102 | 30 | 31 | 26 | 86.67 |
| 1903 | CHA | sullb102 | 30 | 30 | 26 | 86.67 |
| 1906 | DET | loweb101 | 37 | 40 | 32 | 86.49 |
| 1903 | WS1 | delae101 | 41 | 40 | 35 | 85.37 |

**Top 10 most-wrongly-added players** (syn_starts ≥ 20, sorted by added/syn_starts):

| season | team_id | player_id | syn_starts | real_starts | added | add_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1904 | WS1 | mullj102 | 26 | 27 | 26 | 100.0 |
| 1905 | NY1 | clarb103 | 25 | 9 | 25 | 100.0 |
| 1910 | DET | lathc101 | 24 | 19 | 24 | 100.0 |
| 1902 | NY1 | ohagh101 | 24 | 24 | 24 | 100.0 |
| 1898 | BLN | brods101 | 22 | 23 | 22 | 100.0 |
| 1909 | NY1 | snodf101 | 22 | 16 | 22 | 100.0 |
| 1910 | CHN | kanej103 | 30 | 11 | 29 | 96.67 |
| 1909 | BRO | wheaz101 | 26 | 26 | 25 | 96.15 |
| 1903 | CHN | raubt101 | 26 | 15 | 25 | 96.15 |
| 1909 | BOS | spent101 | 24 | 22 | 23 | 95.83 |

**Top 10 worst position-misplacement** (correct_starter_games ≥ 30, sorted by wrong_pos/correct_starter_games):

| season | team_id | player_id | correct_starter_games | correct_starter_wrong_pos | wrong_pos_rate_pct |
| --- | --- | --- | --- | --- | --- |
| 1899 | NY1 | wilsp102 | 51 | 43 | 84.31 |
| 1904 | CHN | barrs101 | 63 | 53 | 84.13 |
| 1907 | BRO | hummj101 | 59 | 49 | 83.05 |
| 1904 | DET | robir101 | 48 | 39 | 81.25 |
| 1910 | SLA | griga101 | 73 | 59 | 80.82 |
| 1901 | DET | mcals101 | 42 | 33 | 78.57 |
| 1907 | BOS | paref101 | 68 | 53 | 77.94 |
| 1907 | CHN | hofms101 | 111 | 86 | 77.48 |
| 1898 | CIN | steih101 | 44 | 34 | 77.27 |
| 1902 | NY1 | dunnj102 | 57 | 44 | 77.19 |

## Artifacts

- Per-(game, side, lineup_position) comparison: `logs/synthetic_lineup_backtest/comparison.parquet`
- Per-(season, team, player) error rates: `logs/synthetic_lineup_backtest/per_player.parquet`
- Per-(game, side) error counts: `logs/synthetic_lineup_backtest/side_errors.parquet`