# Geometry Statcast Development Protocol

This protocol creates a development bridge between scorer-recorded Retrosheet trajectory classes and a four-class trajectory derived from Statcast launch angle. The two targets remain separate. Historical outputs retain scorer-specific classifications; the standardized target uses the angle bands below.

The frozen selection is `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-selection-v1`. It was constructed without reading play, trajectory, observed-status, or Statcast angle labels. Eligibility used only PRIMARY TRAIN membership and game metadata:

- Seasons 2015, 2019, 2023, and 2025
- Regular-season, MLB Retrosheet play-by-play games
- `SingleGame` only
- The accepted 6,105-game confirmation reserve excluded by its bound file and sorted-ID digests
- `HOU202504190`, `KCA202504070`, and `NYN202508130` conservatively excluded because they formed the earlier bridge inventory; Statcast labels were inspected only for `HOU202504190`, while the other two games were metadata-only

Within each season and Retrosheet park, eligible games are ordered lexicographically by `SHA256('geometry-statcast-selection-v1:' + game_id)`, with `game_id` as the collision tie-breaker. Ranks 1–3 are fitting games and rank 4 is `modern_angle_evaluation`. Seven park-season strata have fewer than four eligible games and are listed in `unsupported_strata.json`; they contribute their available first three fitting games and no evaluation game when rank 4 is absent.

The selection contains 6,632 eligible games across 128 park-season strata, 373 fitting games, and 121 modern-angle evaluation games. The first acquisition stage contains 12 fitting games: three supported park strata per season, selected from rank-1 games by `SHA256('geometry-statcast-mechanics-v1:' + season + ':' + park_id)`. The remaining 361 fitting games may be acquired only after the mechanics checks below pass. The 121 modern-angle evaluation games must remain unacquired and uninspected until the feature set, model, fitting procedure, angle transformation, and evaluation outputs are frozen.

For each selected game, resolve exactly one MLB `game_pk` using game date, explicit Retrosheet-to-MLB team mappings for both clubs, and game number. A zero-match or multiple-match game is a failed game and remains in the denominator. For each Retrosheet terminal plate appearance, derive the within-game cumulative terminal-PA ordinal. Match it to Statcast `at_bat_number`, then require exact inning, frame, batter MLBAM ID, and pitcher MLBAM ID agreement. Person resolution must use the Chadwick Register pinned at commit `7640314a83d788c63fa7d26fa5ce9a9871053e27`. PA-key duplicates, personnel disagreements, unmatched PAs, and missing launch angles remain explicit failures or missing values in denominators; they are never silently dropped.

The standardized four-class angle target is:

| Class | Launch angle |
| --- | --- |
| GroundBall | `< 10` |
| LineDrive | `>= 10` and `<= 25` |
| Fly | `> 25` and `<= 50` |
| PopUp | `> 50` |

An angle exactly equal to 25 degrees is LineDrive; values greater than 25 degrees enter Fly. `FlyBall` is an input alias only and must normalize to the canonical project class `Fly`. Bunt is a separate attribute rather than an exclusion from the four-class angle target: a bunt at 22 degrees remains angle-defined LineDrive with its bunt flag preserved. Every batted ball remains in the denominator. Retain the source numeric angle, its raw representation, missingness, bunt flag, and any provider flags. Public Statcast CSV numeric values may be estimated or transformed rather than direct instrument measurements, so this target is a standardized operational definition rather than measured-only gold truth.

Mechanics acceptance requires 12 of 12 unique game resolutions, complete one-to-one batted-ball matches for the declared Retrosheet batted-ball denominator, unique PA-key sets on both sides, exact inning/frame and both-player agreement, and at least 95% launch-angle availability overall. The accounting identity must cover matched rows, unmatched Retrosheet rows, unmatched Statcast rows, duplicate keys, player mismatches, and missing angles. All missing angles and failures remain in the predeclared selected-game and batted-ball denominators. If any gate fails, diagnose it and freeze a new declared selection or bridge artifact before full fitting acquisition; do not lower the threshold or silently subset the data.

All selected games are PRIMARY TRAIN games. Their existing scorer-recorded labels have already been exposed during development, including prior weak-clue work. The future `modern_angle_evaluation` labels are only prospectively uninspected if acquisition is deferred until model freeze; they are not globally virgin games. The separate 6,105-game confirmation reserve remains sealed.

The selection is bound to the corrected geometry materialization report and source contract, but full upstream relation content hashes and a full database digest are unavailable. Its provenance claim is therefore partial materialization and selected-contract binding, not full source-content certification.
