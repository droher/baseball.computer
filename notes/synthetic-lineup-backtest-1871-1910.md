# Synthetic-lineup backtest, 1871-1910

Backtest of `synthetic_box_score.lineup_assignments` against real Retrosheet box-score lineups.
The optimizer is run on box-score-era games where real lineups exist, using only the gamelog-only feature set (Lahman/Databank season inputs + gamelog starting pitcher + DH flag). Results are compared to the real assignments.

## Methodology

- Seasons in scope: 1871, 1872, 1874, 1898, 1899, 1900, 1901, 1902, 1903, 1904, 1905, 1906, 1907, 1908, 1909, 1910
- Games scored: 14963 (29926 game-sides, 269334 starter slots)
- Optimizer runtime: 26.1s
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

- Wrong starters per game: 3.999 (per side: 1.999) out of 9
- Wrong positions per game (right starter, wrong defensive position): 1.449 (per side: 0.725)

## Date-independent error rates

Set-based recall metrics, independent of which date the optimizer chose to start each player. The pitcher is excluded on the (syn_pos, real_pos) axis: rows where syn_pos == 1 or real_pos == 1 are dropped before aggregation.

- `set_miss_rate` = `1 − Σ_p min(syn_starts, real_starts) / Σ_p real_starts`, summed across all (season, team, player). Captures whether the optimizer picked the right people, regardless of dates or positions.
- `pos_set_miss_rate` = same construction but on (player, fielding_position) buckets. A player who started 50 at SS and 10 at 2B scores against the SS bucket and the 2B bucket independently. Captures position-conviction without date alignment.

Across all sides (orphans included):

- set_miss_rate: 1.43%
- pos_set_miss_rate: 1.91%

## Errors per game by era × league

| era | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- |
| 1871-1880 | NA | 1074 | 2.283 | 3.125 |
| 1891-1900 | NL | 4818 | 4.109 | 1.425 |
| 1901-1910 | AL | 11994 | 4.047 | 1.487 |
| 1901-1910 | NL | 12040 | 4.06 | 1.273 |

## Wrong-defender rate by truth fielding position

For each defensive position, fraction of game-sides where the synthetic lineup did *not* put the same player at that position as the real lineup. Counts both 'wrong player' and 'right player at a different position' as errors.

| position_group | sides | wrong_sides | wrong_per_game |
| --- | --- | --- | --- |
| C | 29926 | 16034 | 1.072 |
| IF | 119704 | 35452 | 0.592 |
| OF | 89779 | 30038 | 0.669 |
| P | 29926 | 1 | 0.0 |

By individual fielding position:

| fielding_position | sides | wrong_sides | wrong_per_game |
| --- | --- | --- | --- |
| 1 | 29926 | 1 | 0.0 |
| 2 | 29926 | 16034 | 1.072 |
| 3 | 29926 | 8996 | 0.601 |
| 4 | 29926 | 9326 | 0.623 |
| 5 | 29926 | 9021 | 0.603 |
| 6 | 29926 | 8109 | 0.542 |
| 7 | 29927 | 9502 | 0.635 |
| 8 | 29926 | 9804 | 0.655 |
| 9 | 29926 | 10732 | 0.717 |

## Per-player rates by truth player's roster churn

Classification on each (season, team, real-player) tuple. multi-stint = the player had >1 stints in the league that season; single-stint full = stint=1 and games_total >= 80% of team games; single-stint partial = otherwise. Aggregated over all real-player game-sides for the bucket.

| churn | real_starts_total | missed_total | wrong_pos_total | miss_rate_pct | wrong_pos_rate_pct |
| --- | --- | --- | --- | --- | --- |
| multi-stint | 23836 | 6305 | 2416 | 26.45 | 13.78 |
| single-stint full | 129335 | 10850 | 11115 | 8.39 | 9.38 |
| single-stint partial | 116158 | 42676 | 8157 | 36.74 | 11.1 |

## Date-independent decomposition by churn bucket

Per bucket: `wrong_starters_per_game` is the bucket's contribution to the headline (sum across rows = the headline `wrong_starters_per_game`). `set_miss_rate` and `pos_set_miss_rate` are computed over the bucket's (season, team, player) — and (..., fielding_position) — tuples using the same `1 − Σ_min / Σ_real` formula as the headline, so they are *not* additive across buckets.

| churn | wrong_starters_per_game | set_miss_rate_pct | pos_set_miss_rate_pct |
| --- | --- | --- | --- |
| multi-stint | 0.421 | 1.5 | 2.12 |
| single-stint full | 0.725 | 1.52 | 1.89 |
| single-stint partial | 2.852 | 1.29 | 1.9 |

## Errors per game by team games-played bin

| team_games_bin | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- |
| 130+ | 28852 | 4.063 | 1.387 |
| 60-100 | 136 | 1.338 | 4.147 |
| <60 | 938 | 2.42 | 2.977 |

## Best 10 team-seasons (fewest wrong starters per game)

| season | team_id | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- | --- |
| 1874 | NY2 | NA | 65 | 0.523 | 4.646 |
| 1872 | BS1 | NA | 48 | 0.833 | 2.333 |
| 1871 | RC1 | NA | 25 | 0.96 | 3.52 |
| 1871 | NY2 | NA | 33 | 1.091 | 3.515 |
| 1871 | TRO | NA | 29 | 1.172 | 3.517 |
| 1872 | PH1 | NA | 47 | 1.277 | 3.021 |
| 1904 | BOS | AL | 157 | 1.325 | 0.0 |
| 1901 | BOS | AL | 138 | 1.58 | 0.29 |
| 1872 | CL1 | NA | 22 | 1.636 | 3.091 |
| 1871 | PH1 | NA | 28 | 1.643 | 0.857 |

## Worst 10 team-seasons (most wrong starters per game)

| season | team_id | league | sides | wrong_starters_per_game | wrong_positions_per_game |
| --- | --- | --- | --- | --- | --- |
| 1910 | CHA | AL | 156 | 7.423 | 1.59 |
| 1872 | BR1 | NA | 26 | 6.692 | 2.615 |
| 1907 | WS1 | AL | 154 | 6.558 | 4.364 |
| 1909 | CLE | AL | 155 | 6.49 | 1.574 |
| 1910 | CLE | AL | 161 | 6.46 | 2.634 |
| 1899 | CIN | NL | 157 | 6.191 | 2.306 |
| 1909 | BSN | NL | 155 | 6.168 | 1.161 |
| 1909 | BRO | NL | 155 | 6.129 | 2.426 |
| 1909 | CHA | AL | 159 | 5.824 | 2.893 |
| 1902 | CHN | NL | 143 | 5.818 | 1.832 |

## Per-player error rates

- (season, team, player) tuples: 5556
- Synthetic count == real count AND every shared start at the right position: 2448 (44.06%)
- Synthetic count == real count, but some position errors: 413 (7.43%)
- Synthetic over-starts the player: 1296 (23.33%)
- Synthetic under-starts the player: 1399 (25.18%)

**Top 10 most-missed real starters** (real_starts ≥ 30, sorted by missed/real_starts):

| season | team_id | player_id | real_starts | syn_starts | missed | miss_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1903 | PHI | zimmc101 | 34 | 33 | 31 | 91.18 |
| 1908 | CIN | hobld101 | 32 | 31 | 28 | 87.5 |
| 1905 | PIT | ganlb101 | 32 | 31 | 28 | 87.5 |
| 1906 | CLE | buelf101 | 31 | 31 | 27 | 87.1 |
| 1898 | WSN | myerb102 | 31 | 31 | 27 | 87.1 |
| 1909 | CLE | bemih101 | 30 | 34 | 26 | 86.67 |
| 1899 | PIT | dillp103 | 30 | 30 | 26 | 86.67 |
| 1903 | CHA | sullb102 | 30 | 30 | 26 | 86.67 |
| 1908 | PHA | barrj104 | 36 | 36 | 31 | 86.11 |
| 1898 | NY1 | wilmw101 | 34 | 33 | 29 | 85.29 |

**Top 10 most-wrongly-added players** (syn_starts ≥ 20, sorted by added/syn_starts):

| season | team_id | player_id | syn_starts | real_starts | added | add_rate_pct |
| --- | --- | --- | --- | --- | --- | --- |
| 1903 | CHN | raubt101 | 26 | 15 | 26 | 100.0 |
| 1904 | WS1 | mullj102 | 26 | 27 | 26 | 100.0 |
| 1905 | NY1 | clarb103 | 25 | 9 | 25 | 100.0 |
| 1910 | DET | lathc101 | 24 | 19 | 24 | 100.0 |
| 1902 | NY1 | ohagh101 | 24 | 24 | 24 | 100.0 |
| 1898 | BLN | brods101 | 22 | 23 | 22 | 100.0 |
| 1904 | BSN | lautb101 | 20 | 20 | 20 | 100.0 |
| 1910 | CHN | kanej103 | 30 | 11 | 29 | 96.67 |
| 1910 | BOS | bradh101 | 24 | 16 | 23 | 95.83 |
| 1909 | BOS | spent101 | 24 | 22 | 23 | 95.83 |

**Top 10 worst position-misplacement** (correct_starter_games ≥ 30, sorted by wrong_pos/correct_starter_games):

| season | team_id | player_id | correct_starter_games | correct_starter_wrong_pos | wrong_pos_rate_pct |
| --- | --- | --- | --- | --- | --- |
| 1904 | CHN | barrs101 | 63 | 54 | 85.71 |
| 1907 | BRO | hummj101 | 58 | 49 | 84.48 |
| 1904 | DET | robir101 | 46 | 38 | 82.61 |
| 1899 | NY1 | wilsp102 | 51 | 42 | 82.35 |
| 1898 | CIN | steih101 | 43 | 35 | 81.4 |
| 1872 | BL1 | highd101 | 42 | 33 | 78.57 |
| 1902 | NY1 | dunnj102 | 59 | 46 | 77.97 |
| 1901 | DET | mcals101 | 44 | 34 | 77.27 |
| 1907 | BOS | paref101 | 70 | 54 | 77.14 |
| 1902 | PIT | wagnh101 | 127 | 97 | 76.38 |

## Artifacts

- Per-(game, side, lineup_position) comparison: `logs/synthetic_lineup_backtest/comparison.parquet`
- Per-(season, team, player) error rates: `logs/synthetic_lineup_backtest/per_player.parquet`
- Per-(game, side) error counts: `logs/synthetic_lineup_backtest/side_errors.parquet`