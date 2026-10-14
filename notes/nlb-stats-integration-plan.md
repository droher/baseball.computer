# Negro Leagues stats integration plan

Feed Retrosheet's `negroleagues.zip` player and team lines (`stg_nlb_*`) into the game, season, and career models, for the games no other source covers.

## What the data looks like

- `nlb_stats.gameinfo` has 8,215 games from 1903 to 1962. Of these:
  - 2,196 are already `PlayByPlay` in `stg_games`.
  - 2,452 are already `BoxScore` in `stg_games`.
  - 3,567 are NLB-only and reach `game_start_info` / `game_results` through `nlb_only_games` (`source_type = 'NegroLeaguesStats'`). By game type that is 2,226 regular season, 1,280 exhibition, and 61 other.
- 3,053 of the 3,567 NLB-only games have player lines. The other 514 have only a game row: 281 regular season, 228 exhibition, and 5 other.
- In games both sources cover, the NLB lines agree with the existing box lines. Of 97,188 player-game batting lines, 6 differ on hits and 25 on at-bats. NLB lines are therefore used only for NLB-only games, and the existing sources keep precedence everywhere else.
- Player ids match the rest of the warehouse: 3,480 of 3,485 are in `people`. The other five (oneib801, robis802, patkm801, cranj801, fitzj802) have an `8` in the sixth character, which the `people` regex `[-a-z]{5}[01][0-9]{2}` (`bc/models/intermediate/bio/people.sql:93`) turns into NULL.
- Two team ids are not in prod `seed_franchises`. `ALN` (American League All Stars, 34 games, 1926 to 1930) is already added on this branch. `NLN` (7 games, 1932) still needs a row.
- About 4% of batting stat rows are `lower`/`upper` bounds rather than exact values.
- The current Lahman release on R2 covers the Negro Leagues from 1891 to 1948. Prod `bc.db` was built from an older release with only AL and NL rows. [nlb-lahman-retrosheet-comparison.md](nlb-lahman-retrosheet-comparison.md) has the detailed comparison. In short: only Lahman covers 1891–1932, and only Retrosheet covers 1949. For 1933–48 both have data. Retrosheet is larger in most team-seasons once games without a box score are excluded and exhibitions between major clubs are counted, which matches Lahman's scope.
- 54 Lahman NeL team-seasons have no `team_id_retro`: mostly pre-1920 IND, EAS, and WES clubs, plus 1931 CC2 (Chicago Columbia Giants) and 1932 COT (Columbus Turf Club). `stg_databank_batting` and `stg_databank_pitching` have a `not_null(team_id)` audit, so the new release fails the build until these are mapped or filtered.
- With the current season-model logic, Lahman barely affects the NLB totals:
  - `retrosheet_supplemented` COALESCEs only where the whole Retrosheet season sum is NULL.
  - `databank_only` fires only for seasons absent from `stg_games`, and every season from 1871 on is present.
  - So every pre-1933 Lahman NeL season is dropped, and for 1933–48 the Retrosheet totals win wherever both exist.
  - The pre-1920 SB/CS override, by contrast, would replace Retrosheet SB/CS with Lahman's for 1903 to 1919 NeL rows.

## Decisions

- **Bounded stats are NULL in the main models.** The game and season models carry only exact values (`b_h`, not `b_h_lower`). A bounded stat contributes NULL, the same way a missing stat in a partial box score does now. The ranges get their own tables (step 10).
- **Precedence by source.** Event data comes first, then box score, then NLB. A game uses NLB lines only when `game_start_info.source_type = 'NegroLeaguesStats'`, which by construction excludes every game in `stg_games` or `stg_gamelog`.
- **Naming.** Abbreviate Negro Leagues as `nlb` everywhere: the `nlb_stats` source schema, `stg_nlb_*`, and `nlb_only_games`. Only the R2 path `data.baseball.computer/ngl_stats` keeps the old name, because the parser repo publishes it.

- **Lahman fills only the seasons Retrosheet hasn't covered yet.**
  - Lahman's NLB data is season-level only, so it never reaches game models.
  - The player season models use Lahman NLB rows only for seasons where Retrosheet's NLB coverage is incomplete. Today that means before 1933.
  - The cutoff is computed from the data, not hardcoded, so it moves back automatically as Retrosheet adds earlier seasons.
  - In Retrosheet-covered seasons, Lahman NLB rows do not feed the season models, not even through the COALESCE supplement or the pre-1920 SB/CS override. They appear only in the discrepancy tables.
- **Entity resolution is analysis first.**
  - Discrepancy and resolution tables propose matches for players and teams with evidence attached.
  - Only curated crosswalk seeds change what the season models use. Proposed matches are never applied automatically.

## Season coverage rule

`nlb_season_sources` has one row per season that has Negro Leagues data in either source.

- **Coverage measure:** `retrosheet_coverage = Retrosheet AB / Lahman AB`.
  - Retrosheet AB comes from box-score games only: regular-season games plus exhibitions between two clubs Lahman lists for that season.
  - Lahman AB is its Negro Leagues batting AB.
- **Rule:** a season uses Lahman when `retrosheet_coverage < 0.5`. Otherwise, or when Lahman has no rows, it uses Retrosheet.
- **Why 0.5:** today the ratio is at most 0.02 for 1920–32 and at least 0.96 for 1933–48 (the comparison note's Lahman ÷ Retrosheet ratio runs from 0.81 to 1.04). Any threshold between those works, and 0.5 sits in the gap.
- The threshold lives in one macro or variable.

In a Lahman-sourced season:
- Retrosheet NLB regular-season player-team-season rows (games in `stg_nlb_games`) are dropped from the season models, so they aren't double-counted.
- Retrosheet postseason, exhibition, and all-star rows stay, because Lahman is regular-season only.

## Changes

### 1. Prerequisites

- Keep the `people` player-id pattern `[-a-z]{5}[01][0-9]{2}`, and also accept any id that appears in an NLB batting, pitching, or fielding line.
  - Widening the pattern to `[-a-z]{5}[0-9]{3}` would admit 549 `8xx` ids, only 5 of them players. Most of the rest are managers or umpires.
  - It would also admit 1,826 `9xx` ids, which are all umpires.
  - Accepting the line ids admits only the five `8xx` players: oneib801, robis802, patkm801, cranj801, and fitzj802.
- Add an `NLN` row (National League All Stars, 1932) to `bc/seeds/misc/seed_franchises.csv`.

### 2. NLB lines in box-score shape

Add three intermediate models, filtered to NLB-only games, so that each downstream union is a single `UNION ALL BY NAME`:

- **`nlb_batting_lines`**: same columns as `stg_box_score_batting_lines`, plus `team_id`, with `side` derived from `is_home`.
  - Compute singles, total_bases, plate_appearances, on_base_opportunities, on_base_successes, and batting_outs with the same formulas as `stg_box_score_batting_lines.sql:118-128`.
  - Map lineup_position, nth_player_at_position (from sequence), and PH/PR from `b_lp`, `b_seq`, `is_pinch_hitter`, and `is_pinch_runner`.
- **`nlb_pitching_lines`**: same shape as the `box_agg` input in `player_game_pitching_stats`. `p_ipouts` becomes `outs_recorded`. `p_gs`, `p_gf`, and `p_cg` map to the starts, finishes, and complete games flags. Carry `is_winning_pitcher`, `is_losing_pitcher`, and `is_save` through.
- **`nlb_fielding_lines`**: same shape as `stg_box_score_fielding_lines`. `d_pos` maps to the fielding position enum, and `d_ifouts` becomes outs_played.

### 3. Player-game models

- **`player_game_offense_stats`**: union `nlb_batting_lines` next to the box branch (`:170-225`).
- **`player_game_pitching_stats`**: union `nlb_pitching_lines` next to `box_agg` (`:241-281`).
  - W/L/SV currently come from a LEFT JOIN to `stg_games` (`:289-291`). Source them from `game_start_info`/`game_results`, or from the NLB line flags, so that NLB-only games don't get zeros.
  - The window-derived CG, SHO, QS, no-hitter, and perfect game logic needs a check against NLB-only games, where some game-level inputs may be NULL.
- **`player_position_game_fielding_stats`**: add `nlb_fielding_lines` to the box side of the FULL OUTER JOIN (`:92-108`). There are no events for these games, so the box-side value is the one used.
- **`player_game_appearances`**: union NLB batting and fielding lines in the non-PlayByPlay branch (`:42-111`).

### 4. Season models: stop dropping non-`stg_games` games

`player_team_season_offense_stats` (`:213-224`), `player_team_season_pitching_stats` (`:219-231`), and `player_position_team_season_fielding_stats` (`:138-139`) join `stg_games` to read season and game type, which drops every NLB-only game. Switch those joins to `game_start_info`.

Gamelog-only games have no player lines, so the switch adds nothing for them. Verify that with a before/after row-count diff on a pre-1947 and a post-1947 season.

### 5. Team-game models

- `team_game_offense_stats` and `team_game_pitching_stats` sum the player-game models, so they pick up NLB-only games once step 3 lands.
- **`team_game_fielding_stats`**: add the team-level fields from `stg_nlb_teams` (`d_*`, `lob`) where box team lines are used today (`:95-203`).
- **`team_game_results`**: hits and errors for NLB-only games come through the models above. The 514 NLB-only games without player lines still get W/L and runs from `game_results`, plus team totals from `stg_nlb_teams`, if any.

### 6. Completeness and coverage

Update `game_data_completeness` and `season_team_coverage` so that NLB-only games with player lines count as having a box score.

### 7. Lahman NLB staging and crosswalks

- **NLB rows:** a row is NLB when its Lahman `lg_id` is not an MLB league (AL, NL, AA, UA, PL, FL, NA). Expose this as `is_negro_leagues` on `stg_databank_batting`, `stg_databank_pitching`, and `stg_databank_fielding`.
- **Team crosswalk:** `team_id` comes from `seed_nlb_lahman_team_crosswalk` (season, databank_team_id, team_id) when a row exists there, otherwise from `teams.team_id_retro`.
  - Start the seed with the four `team_id_retro` values that disagree with the NLB file: 1935 CEG→NSH, 1942 CCB→CCB, 1946–47 CIC→IN9.
  - Verify each one by roster overlap in `nlb_lahman_team_resolution`.
- **Teams Retrosheet does not have:** superseded on 2026-10-14 (see "Lahman-only teams, players, and leagues" below). These team-seasons keep their Lahman team id.
- **Player crosswalk:** `seed_lahman_people_id_corrections` (databank_player_id, retrosheet_player_id, baseball_reference_player_id) replaces `people.retro_id` and `people.bbref_id` for the listed people.
  - It starts empty and is filled from reviewed matches in `nlb_lahman_player_resolution`.
  - Lahman NLB rows whose person has no Retrosheet id keep the Databank player id (2026-10-14, below).

### 8. Season models: Lahman for incomplete seasons

`player_team_season_offense_stats`, `player_team_season_pitching_stats`, and `player_position_team_season_fielding_stats`:

- Add Lahman NLB rows for Lahman-sourced seasons, alongside the existing `databank_only` rows.
- Drop Retrosheet NLB regular-season rows in those seasons.
- Exclude NLB rows from `retrosheet_supplemented` and from the pre-1920 SB/CS override. NLB status comes from the game being in `stg_nlb_games` on the Retrosheet side and from `is_negro_leagues` on the Lahman side.
- Add a column recording each row's source (`Retrosheet` / `Databank`), if one doesn't exist already, so consumers can tell season-only totals from game-backed ones.

### 9. Lahman standings for Lahman-sourced seasons

Lahman has standings for every NLB team-season it covers: G, W, L, rank, and the pennant flag. Before 1933 that is 316 team-seasons, from 1891 to 1932.

- **New model `team_season_standings`:** final regular-season standings, one row per (season, team_id). Columns:
  - season, team_id, league, games, wins, losses, ties (`games - wins - losses`), win_percentage, league_rank, won_pennant.
  - `standings_source`: `Retrosheet` or `Databank`.
- **Retrosheet seasons:** each team's last row in the daily `standings` model, with ranks computed by league.
- **Lahman-sourced NLB seasons:**
  - Rows come from Lahman `teams`, with `team_id` resolved through the same crosswalk as the player rows.
  - Team-seasons with no Retrosheet team id are left out and listed in `nlb_lahman_team_resolution`.
  - Lahman's team runs are left out. They are sums over the batting lines Lahman has, not complete season totals: 1944 Birmingham shows 155 runs in 74 games.
- **Pennants and ranks:** `won_pennant` comes straight from Lahman's `lg_win` and can differ from first place. In 1926, Lahman ranks Hilldale first in the ECL but gives the pennant to Atlantic City, and ranks Kansas City first in the NNL but gives it to Chicago. Rank and pennant are kept as separate columns, and neither is derived from the other.
- **Daily `standings`:** stays derived from games and unchanged. In Lahman-sourced seasons it still shows the few NLB games Retrosheet has, so `team_season_standings` is the authoritative season-level table for those years. Its model description says so.

### 10. Stat-range tables

**What the bounds are:**
- Bounds sit almost entirely in games without a box score: 1,624 of the 1,633 batting games with bounds, and all 389 pitching games with bounds.
- They read like partial accounts, such as "at least 2 hits" or innings pitched within a range:
  - Batting bounds are mostly lower bounds, on AB (4,426 rows), H (3,898), PA, R, and RBI.
  - Pitching bounds are mostly upper and lower bounds on outs recorded and runs.
- In those games most player stats are simply unknown: 14,073 of 18,894 player lines have neither AB nor H.
- Team totals are far more complete. Across 6,096 team-games without a box score, runs and innings pitched are known in 100%, hits in 78%, and errors in 80%.

**Range of a stat on one player-game line:**

| Case | Lower | Upper |
|---|---|---|
| Exact value | the value | the value |
| Reported bound | Retrosheet's lower, or 0 if only an upper is given | Retrosheet's upper, or unbounded |
| Unknown | 0 | unbounded |

**Models.** All are built from `stg_nlb_*`, which hold the complete Retrosheet NLB record; games that also have play-by-play or box files are exact there. Only Retrosheet-sourced seasons (per `nlb_season_sources`) are included. Retrosheet's few games in Lahman-sourced seasons are left out.

| Model | Grain | Contents |
|---|---|---|
| `nlb_player_game_stat_ranges` | game, player, team, line type, position, stat | Long format. Value, lower and upper, and `range_basis` (`Exact`, `ReportedBound`, `Unknown`). |
| `nlb_team_game_stat_ranges` | game, team, stat | The team line's range, the sums of the players' lower and upper bounds, and a flag when the team value falls outside them. |
| `nlb_player_season_stat_ranges` | season, team, player, game type, stat | Sum of lowers, sum of uppers (unbounded if any game is unbounded), and the count of exact, bounded, and unknown games. |
| `nlb_player_career_stat_ranges` | player, game type, stat | The same, rolled up to the career. |
| `nlb_team_season_stat_ranges` | season, team, game type, stat | Team lines summed, with the same counts. |
| `nlb_player_season_batting_ranges` and `nlb_player_season_pitching_ranges` | season, team, player, game type | Wide pivots of the common counting stats (`h_lower`, `h_upper`, and so on). No rate stats. |

**Audits:**
- Every range has lower ≤ upper.
- A team value outside its players' summed bounds is flagged in a non-blocking audit, since it indicates a source inconsistency.

### 11. Discrepancy and entity-resolution tables

New models under `bc/models/analyses/nlb/`, built from `stg_nlb_*` and the Lahman staging models:

- **`nlb_season_sources`**: the coverage rule above. Per season: Lahman AB, Retrosheet AB (box-score games, regular season and major-club exhibitions), the coverage ratio, and the chosen source.
- **`nlb_team_season_discrepancies`**: one row per team-season in either source. Columns:
  - Lahman: standings G/W/L and batting AB/H/R.
  - Retrosheet: games by tier (box score vs appearances only) and by type (regular, major-club exhibition, other exhibition, postseason), W/L, and box-score AB/H/R.
  - Comparison: AB ratio and a coverage category.
- **`nlb_player_season_batting_discrepancies`** and **`nlb_player_season_pitching_discrepancies`**: one row per (season, team, player) in either source, in matched scope.
  - Coverage category: both, Lahman only, Retrosheet only.
  - Paired stats with differences.
  - Flags for equal AB (or outs) with differing stats. These point to disagreements about values, not coverage.
- **`nlb_lahman_team_resolution`**: one row per Lahman NLB team-season.
  - Lahman id, name, league, `team_id_retro`, crosswalk override, resolved `team_id`, whether it exists in `seed_franchises`.
  - The Retrosheet team with the most shared linked players that season, and the overlap share.
  - Status: `confirmed`, `override`, `crosswalk_disagrees_with_rosters`, `databank_team_id`, or `no_retrosheet_games`.
- **`nlb_lahman_player_resolution`**: one row per candidate pair (Lahman person, Retrosheet person), for Lahman NLB people whose `retro_id` is missing or doesn't appear in any Retrosheet NLB line or bio record.
  - Evidence: same last name (accent-folded), same first initial, birth year agreement, team-seasons shared with the candidate, and AB similarity on those team-seasons.
  - A rank, and a confidence tier derived from the evidence. Shared team-seasons dominate the tier, because names and birth years are too sparse to decide alone.
  - Only 241 of the 822 unlinked people have a birth year. In 1933–48, a shared team-season plus a last-name match links 546 of 697 Lahman-only player-seasons.

## Tests

Each test goes through the model's public inputs and outputs.

- **`player_game_offense_stats`**: an NLB-only game's line appears with the derived columns correct. A game that is also `BoxScore` is not double-counted. A bounded stat comes out NULL.
- **`player_game_pitching_stats`**: W/L/SV are populated for an NLB-only game, and `outs_recorded` comes from `p_ipouts`.
- **`player_team_season_offense_stats`**: an NLB-only regular-season game reaches the season total, and an exhibition does not.
- **`people`**: an `8xx` id that appears in an NLB line survives, and an `8xx` or `9xx` id that doesn't is still excluded.
- **`nlb_season_sources`**: a season below the threshold picks Lahman, one at or above picks Retrosheet, and a season with no Lahman rows picks Retrosheet.
- **`player_team_season_offense_stats`**: in a Lahman-sourced season, a Lahman NLB row appears and the Retrosheet regular-season row for the same player is dropped, while a Retrosheet exhibition row stays. In a Retrosheet-sourced season, the Lahman NLB row is absent and doesn't fill NULLs.
- **`stg_databank_batting`**: a crosswalk-seed row overrides `team_id_retro`.
- **`nlb_lahman_team_resolution`**: a crosswalk that disagrees with the roster overlap is flagged.
- **`nlb_lahman_player_resolution`**: a shared team-season plus a last-name match outranks a name-only match.
- **`nlb_player_game_stat_ranges`**: an exact value gives an equal lower and upper bound; a reported lower bound with no upper gives an unbounded upper; an unknown stat gives 0 to unbounded.
- **`nlb_player_season_stat_ranges`**: one unbounded game makes the season upper bound unbounded, and the exact, bounded, and unknown game counts add up to the games played.
- **`nlb_team_game_stat_ranges`**: a team value outside the players' summed bounds is flagged.
- **`team_season_standings`**: a Lahman-sourced NLB season takes its W/L/rank/pennant from Lahman, including a pennant winner that isn't ranked first. A Retrosheet season matches the last daily `standings` row. A Lahman team-season with no resolved `team_id` is absent.

## Validation on real data

1. Run `just preload` (it loads the `nlb_stats` schema and the new Lahman release), then run `just plan` on the branch env.
2. Run all audits. Expect the `relationships(player_id → people)` and `team_id → seed_franchises` audits to pass after step 1.
3. Spot-check 1937 Homestead Grays and 1942 Kansas City Monarchs season batting lines against the Retrosheet NeL season pages and Seamheads totals.
4. Count player-team-season rows by league for 1920 to 1948, before and after.
5. Confirm that no 1920 to 1948 AL or NL season total changed.

## Outcome (2026-10-13, branch env `claude_nlb_data_integration_b2c2b9`)

Implemented as planned, with these differences:

- `nlb_season_sources` lives in `intermediate/nlb`, not `analyses/nlb`, because the season models depend on it.
- Team-game totals for NLB-only games come from Retrosheet's team lines (`nlb_team_lines`), not from summed player lines, because the player lines often leave stats unknown that the team line knows.
- `has_box_score` counts an NLB-only game when `gameinfo.box = 'y'`, not when it has player lines.
- NLB pitching starts and finishes use `p_gs`/`p_gf` for every pitcher on a team-game when any pitcher there has one, and fall back to pitching order only when all are blank. Mixing the two produced two starters in 7 team-games.
- The new Lahman release gives 21 Retrosheet ids and 12 Baseball-Reference ids to two people each. `seed_lahman_people_id_corrections` (which replaces the planned `seed_nlb_lahman_player_crosswalk`) fixes the 24 affected people from the Retrosheet biofile, and the uniqueness audits for both ids moved from the raw `baseballdatabank.people` table to `stg_people`. Without this, every model joining `people` duplicated rows, and the raw-table audits stopped the prod rebuild.
- `stg_databank_appearances` joins `games_total` and `outs_played` on the Databank team, since the 1939 Toledo Crawfords are two Lahman teams (NAL and NN2) and one Retrosheet team.
- The two NLB-only games with no Retrosheet game type (CAG193305290, DI1193305290) stay in the game-level models but are left out of the season models (`@is_retrosheet_season_game`).
- `nlb_team_game_stat_ranges` does not compare fielding double and triple plays with player sums, since every fielder in the play is credited.
- `nlb_lahman_team_resolution` marks a team-season `no_retrosheet_games` when the resolved team has no Retrosheet roster that season. Before 1933 a single shared player otherwise flagged 86 team-seasons as disagreeing.
- Source-data guards: Lahman batting singles are NULL when hits are fewer than extra-base hits (tayloci99, 1912), and a negative Retrosheet NLB left-on-base count is NULL (MEM194107041).
- Out-of-scope fix found on the way: team `earned_runs` in `team_game_pitching_stats` was zeroed whenever `team_unearned_runs` was NULL (`GREATEST` ignores NULLs). In prod this affected 14,203 of 28,990 box-score team-games.

Validation on the branch env:

- The Lahman/Retrosheet cutoff falls at 1933: 1891–1932 are Lahman-sourced, 1933 on are Retrosheet-sourced (coverage 0.95–1.13 for 1941–48).
- AL and NL 1920–48 offense and pitching season totals match prod exactly. Every game already in prod has the same player-game and team-game totals, except team earned runs (the fix above).
- 1937 Homestead Grays and 1942 Kansas City Monarchs Retrosheet at-bats are within 10% of Lahman.
- Rows added: about 19,000 player-game batting lines, 8,100 pitching lines, 14,600 fielding lines, 7,100 team-games, 5,600 player-team-season batting rows.
- Remaining non-blocking audit warnings: `nlb_team_game_stat_ranges` team values outside summed player bounds (3,257 rows, a real disagreement in the source); 14 NLB team-games whose starter's line is missing; 3 NLB-only player lines with impossible counts (for example 10 strikeouts in 4 plate appearances); 3 exact duplicate rows in Retrosheet's NLB player file.

## Lahman-only teams, players, and leagues (2026-10-14)

Lahman has Negro Leagues teams, players, and leagues that Retrosheet does not. They now stay in the warehouse under Lahman's ids instead of being dropped.

- **Teams:** `stg_databank_teams.team_id` is the crosswalk row, otherwise `team_id_retro` when it is in `seed_franchises`, otherwise Lahman's team id (`is_databank_team_id`). `nlb_lahman_team_resolution` reports these as `databank_team_id`.
  - Crosswalk rows added from roster overlap and Lahman `franch_id`: Philadelphia Giants 1903–11 (PG→PHG), Cuban Stars of Havana 1906–15 (CSH→CUW), 1931 Chicago Columbia Giants (CC2→CAG).
  - Two Lahman ids are already different Retrosheet teams, so they get new ids: New York Lincoln Stars 1914–16 (NLS→NYS) and West Baden Sprudels 1910–13 (WBS→WBD).
  - `seed_franchises` gains a row for each Lahman-only team and span rows for Lahman seasons outside a Retrosheet team's span (for example the Brooklyn Royal Giants 1905–22). The `team_season_in_franchises` audit requires a franchise row covering every Negro Leagues team-season.
- **Players:** `stg_people.player_id` is the Retrosheet id, or the Databank id when there is none. The season models and `stg_databank_appearances` use it, and `people` has a row for every such person. `people.player_id` is also set for anyone with a Lahman batting, pitching, or fielding line, which covers Walter Goines (Retrosheet non-player id `goinw901`). Frank Warfield's Lahman `retro_id` `warf101` is corrected to `warff101`.
- **Leagues:** `seed_leagues` maps Lahman `lg_id` to Retrosheet's code (NNL→NN1, EWL→EW), keeps Lahman's code for leagues Retrosheet lacks (EAS, WES, NAC, INT), and leaves independents (IND) NULL. It records major-league seasons per MLB's recognition: the 1969 committee for the 19th- and 20th-century leagues, without the National Association, and 1920–48 for the seven Negro Leagues. `team_season_standings` gains `is_major_league`, and a Retrosheet row with no league takes Lahman's league for that team-season when Lahman gives exactly one.

## Out of scope

- Fielding discrepancy tables. Batting and pitching come first.
- Lahman team batting, pitching, and fielding totals (`teams` R, AB, H, ERA, and so on). Standings are in scope (step 9). These totals are partial sums and stay out.
- Tightening ranges with team totals or within-line consistency, for example capping a player's runs at the team total minus teammates' known runs. It went to the retrosheet-mirror session as a candidate Retrosheet finding: 1,134 unknown player runs and 247 unknown hits are forced to 0 by the team totals, and 71 team-games have player minimums above the team total.
- The Chadwick register as resolution evidence. It links 228 of the 822 unlinked Lahman people and exposes 23 conflicts, but it is not on R2 yet.
- `stg_nlb_plays`. The 2,196 PBP games already come through the event pipeline, so check whether `plays.csv` covers anything beyond them before wiring it in.
- Era-stale league codes on spanning NeL franchise rows. This is already in `notes/followups.md`.
- Promotion to prod. That needs `just promote-prod` on the changed models, or a `rebuild-prod`, since several upstream models change.
