# Geometry Statcast Bridge Acquisition Protocol

This protocol acquires Statcast launch angles for every eligible 2016, 2017, and 2018 regular-season game in the development partition. It extends the development bridge defined in `geometry-statcast-development-protocol.md` and changes nothing about the standardized angle target, the matching rule, or the sealed evaluation sets. The purpose is development evidence only:

- Measure each hitter's true launch-angle band mix inside the 2009 to 2019 recording pipeline, so that the same hitters' recorded labels in 2012 to 2014 can test whether the early-regime translation transports to seasons without references.
- Give the 2009 to 2019 pipeline a full-season reference instead of a park-balanced sample, so per-scorer and per-park translation cells have enough support to be estimated rather than pooled.

The frozen selection is `artifacts/statistical/backtests/geometry_reliability/20260911-statcast-bridge-selection-v1`. It was constructed without reading play, trajectory, observed-status, or Statcast angle labels. Eligibility used only PRIMARY TRAIN membership and game metadata:

- Seasons 2016, 2017, and 2018
- Regular-season, MLB Retrosheet play-by-play games
- `SingleGame` only
- The accepted 6,105-game confirmation reserve excluded by its bound file and sorted-ID digests
- The three games of the earlier bridge inventory excluded; none falls in these seasons

Every eligible game is a fitting game. There is no evaluation tier in this selection: the 121 `modern_angle_evaluation` games of the development selection remain unacquired and uninspected, and the confirmation reserve remains sealed. The selection contains 4,962 games (1,700 in 2016, 1,651 in 2017, 1,611 in 2018).

Acquisition runs in two stages. The smoke stage acquires six games, two per season, chosen as the lowest `SHA256('geometry-statcast-bridge-smoke-v1:' + game_id)` within each season: `CIN201705040`, `KCA201808140`, `LAN201608050`, `NYA201605080`, `SDN201709200`, `SEA201805020`. The full stage may run only if all six smoke games resolve to exactly one `game_pk`, pair completely under the matching rule below, and reach at least 95% launch-angle availability overall. If any smoke gate fails, diagnose it and freeze a new declared selection or bridge artifact; do not lower the threshold or subset the games.

Game resolution, plate-appearance matching, person resolution through the Chadwick Register pinned at commit `7640314a83d788c63fa7d26fa5ce9a9871053e27`, the plate-appearance counter of the September 11, 2026 matching amendment, and the four-class angle target are unchanged from the development protocol and are bound by hash in `docs/geometry-statcast-bridge-inputs.json`. The accepted twelve-game mechanics artifact `20260911-statcast-mechanics-v2` is bound as the mechanics evidence and is re-verified before the full stage. A game that fails resolution or pairs incompletely stays in the denominator with its failure recorded; failures are never repaired by matching on trajectory agreement.

The public Statcast CSV does not distinguish tracked launch angles from provider estimates, and MLBAM's stated practice fills untracked batted balls from the stringer's batted-ball type and the outcome. Rows acquired here therefore carry `measurement_origin = tracking_or_estimate_not_distinguished`, and any analysis of the mapping must state how it treats angles that may be functions of the recorded label.

All selected games are PRIMARY TRAIN games whose scorer-recorded labels are already development-exposed. Angles acquired under this protocol are development evidence and may not be used to score the 121 evaluation games or any reserve game.
