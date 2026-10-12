# Data-quality audit standards

Negro League accounts and box-score accounts before 1875 can have incomplete or contradictory recorded detail. Retain the recorded rows and report those limitations instead of inventing player statistics or pitching order. A game qualifies for the lenient detail rules when its canonical `game_start_info` record has `is_negro_leagues IS TRUE`, or has both `source_type = 'BoxScore'` and `season < 1875`. Unknown metadata does not grant an exception unless the other eligibility condition is established.

These exceptions supplement existing audit rules; they do not remove the existing checks for other source families or legitimate recorded play-by-play artifacts.

| Check | Other records | NLB or pre-1875 box-score records |
| --- | --- | --- |
| Required game, player and team identities; unique grains; reference integrity | Blocking | Blocking |
| Unknown pitcher on a recorded home run | Blocking | Retain the home-run row and warn |
| Known extra-base-hit components exceed hits | Blocking | Retain recorded counters, use NULL derived singles/total bases, and warn |
| Batting or pitching home runs exceed hits | Blocking | Tolerate a matching recorded box-score issue and warn |
| Ambiguous box-score starting-pitcher order | Blocking | Record the team-game issue and warn; preserve the recorded flags without choosing a starter |
| Park-factor upper plausibility limits | Blocking | Warn for positive, finite estimates in confirmed NLB park/season/league populations |
| Negative or nonfinite park factors | Blocking | Blocking |

The eligibility rule lives in one place, the `@is_less_complete_source(<game table alias>)` macro. Each scoped audit (`box_score_data_issue_source_scope`, `box_score_home_run_pitcher_missing`, `team_game_has_one_starter`, `park_factor_bounds`) takes an `excused_population` argument and is attached twice: `excused_population := FALSE` checks everything outside the exception and blocks, and `excused_population := TRUE, blocking := FALSE` reports the excused rows as warnings. `bc/tests/test_less_complete_source_audits.py` fails if any attachment pairs those two arguments differently. Extra-base-hit and home-run contradictions are scoped where they are recorded: `box_score_data_issues` blocks on any such issue outside the exception, so the player-game `bounded_excluding_data_issues` carve-outs only ever excuse eligible games.

Missing hit components remain unknown when deriving singles and total bases. A warning is evidence of a data limitation, not evidence that the record was corrected. Audit output should identify the affected game and player or team so consumers can inspect the source.

The starter check covers observed pitching groups and expected play-by-play or box-score team-games, including a box-score side with no recorded pitching rows. A game-log-only account without pitching detail is outside that expected population. The existing recorded play-by-play starter-with-no-appearance exception still applies.

Park-factor comparisons must stay within the same league. A park with no qualifying same-league comparison park has no estimate from this method; other leagues must not supply substitute peers.

The registered park-factor views use play-by-play observations and classify their park/season/league population from canonical game records. Their upper-limit warning remains specific to confirmed NLB populations; the pre-1875 box-score exception concerns game-level recorded detail. Missing, unknown or mixed NLB classification stays strict for park factors. Upper-limit warnings preserve the computed estimate rather than clipping it. For example, KAN05's 1949 NAL home-run factor is 11.01: matched batter/pitcher pairs contribute four home runs over 75 outs there versus zero over 81 outs at its only peer, before the league prior. This is sparse-sample uncertainty rather than invalid arithmetic.

For example, PH2187405040's 1874 box-score line for Dan Eggler records zero hits and one double. It is retained with NULL derived singles and total bases, and reported as a warning. The same contradiction in a non-NLB box-score account from 1875 onward remains blocking. Earlier non-NLB play-by-play and game-log accounts do not gain this exception.

## Production acceptance

The installed SQLMesh DuckDB evaluator forces audits on nondeployable dev snapshots to be nonblocking, so a completed `just plan` does not establish production acceptance. `scripts/audit_report.py` (`just audit-report --db <warehouse> --output <report.json>`) runs every audit read-only with its configured blocking status and records blocking failures, permitted warnings and errors separately, with query text, row counts and sample rows. The candidate builder runs it on each isolated build. Missing tables or unresolved eligibility are errors, not a passing audit.

Source inconsistencies and authorized handling decisions remain visible in the existing player/game data-issue tables and native audit logs. Production publication requires a successful production build; a dev warning must not silently become a blanket production exemption.
