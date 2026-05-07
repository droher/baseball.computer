# Synthetic Lineup Optimizer Handoff

This note captures the current state of the synthetic lineup optimizer work so another agent can continue without relying on chat history.

## Current Branch And Scope

Branch: `codex/synthetic-lineup-optimizer`

Goal: replace repeated modal lineups for gamelog-only synthetic box scores with deterministic, seeded starting lineup assignments. Pitchers remain fixed from `main_models.stg_gamelog`; only non-pitcher starters are optimized.

The latest user direction changes the stint handling plan:

> To further simplify, why don't we just translate stints to cutoff dates. take the calendar range of the season and divide it according to games on each stint.

Stop and implement that next. Do not continue the current multi-team chronological MILP constraint approach without revisiting it.

Also important: the user explicitly said not to revert doc changes they are making. `AGENTS.md`, `GEMINI.md`, and `notes/followups.md` have doc-only changes that may be user-authored. Leave them alone unless asked.

## Files Changed

Core Python:

- `bc/python_models/synthetic_box_scores/game_lineups.py`
- `bc/python_models/synthetic_box_scores/__init__.py`

SQLMesh models:

- `bc/models/staging/baseballdatabank/stg_databank_appearances.sql`
- `bc/models/synthetic_box_score/team_season_modal_lineups.py`
- `bc/models/synthetic_box_score/box_score_batting_lines.py`
- `bc/models/synthetic_box_score/box_score_fielding_lines.py`
- `bc/models/synthetic_box_score/lineup_assignments.py` (new)
- `bc/models/synthetic_box_score/lineup_optimization_report.py` (new)

Tests/deps:

- `bc/tests/test_synthetic_game_lineups.py`
- `pyproject.toml`
- `uv.lock`

Docs with existing local modifications:

- `AGENTS.md`
- `GEMINI.md`
- `notes/followups.md`

## Implemented Behavior

`game_lineups.py` now has a SciPy HiGHS MILP based optimizer:

- Public assignment function: `build_synthetic_lineup_assignments(games, lineups, candidates)`
- Public report functions:
  - `build_synthetic_lineup_report(games, lineups, candidates)`
  - `build_synthetic_lineup_report_from_assignments(assignments, candidates)`
- Output assignment rows include `stint`.
- Pitcher rows are emitted with `stint = 0`; report calculations ignore pitcher rows by filtering to fielding positions `2..9`.
- Candidate input now includes `stint`, `games_total`, `games_at_position`, `plate_appearances`, and `games_played`.
- Fielding targets are scaled down when a player’s summed fielding games exceed batting games:
  - scale factor is `games_played / sum(games_at_position)`
  - each position target is multiplied by that scale
  - total target becomes batting games when scaling applies

Hard constraints currently encoded:

- positions `2..9` filled once per game side
- each non-pitcher batting slot filled once
- each player appears at most once per game side outside pitcher
- starting pitcher excluded from non-pitcher slots for that game side
- candidates are only eligible where they have `games_at_position > 0`
- default players can only keep their own batting slot, even if changing fielding positions
- replacements inherit the vacated slot

Objective:

- primary MILP minimizes summed relative absolute error for player-stint total games and player-stint-position games
- secondary MILP preserves the exact primary optimum and uses stable seeded random costs to choose among ties

## Staging Model State

`main_models.stg_databank_appearances` was changed to include:

- `stint SMALLINT`
- `games_total USMALLINT`
- grain `(player_id, season, team_id, stint, fielding_position)`

Important discovery: `baseballdatabank.appearances` does not have `stint`. `baseballdatabank.batting`, `fielding`, and `pitching` do.

Current staging approach:

- Uses `baseballdatabank.fielding` for stinted P/C/IF position games.
- Uses `baseballdatabank.fielding` joined to `baseballdatabank.fielding_of` for stinted LF/CF/RF games.
- Uses `baseballdatabank.appearances.g_all` only as the full team-season `games_total`, because appearances has the full total but no stint.

Potential issue to revisit: `games_total` from appearances is team-season total, not stinted. With the user’s new cutoff-date simplification, this may be acceptable if the optimizer/report uses batting games by stint as the effective total target. The current helper already uses batting games when fielding totals exceed batting games, but otherwise it can still keep `games_total`.

## SQLMesh Model Design

The optimizer was split into a shared model to avoid running the MILP three times:

- `synthetic_box_score.lineup_assignments`
  - Runs the optimizer once.
  - One row per game side starter assignment.
  - Includes pitcher and non-pitcher rows.

- `synthetic_box_score.box_score_batting_lines`
  - Reads `lineup_assignments`.
  - Emits batting skeleton rows with stat columns NULL.

- `synthetic_box_score.box_score_fielding_lines`
  - Reads `lineup_assignments`.
  - Emits fielding skeleton rows with stat columns NULL.

- `synthetic_box_score.lineup_optimization_report`
  - Reads `lineup_assignments` and candidate targets.
  - Emits one row per player-stint total target and player-stint-position target.
  - Columns:
    - `season`
    - `team_id`
    - `player_id`
    - `stint`
    - `metric_type` (`Total` or `Position`)
    - `fielding_position` (`0` for total rows)
    - `actual_games`
    - `realized_games`
    - `signed_error`
    - `abs_error`
    - `pct_error`
    - `signed_pct_error`
    - `error_rank`

## Latest User-Requested Simplification

Replace the current chronological stint-order constraints with cutoff date eligibility:

1. For each `(season, player_id)` with multiple stints, collect stints in order.
2. For each stint, use its game share to divide the season calendar range.
   - The user said “take the calendar range of the season and divide it according to games on each stint.”
   - Likely input for stint share should be batting games by `(season, player, team, stint)` where available, because batting is stinted and this optimizer is for non-pitcher starters.
   - Fielding stinted game totals could be used as fallback when batting games are absent.
3. Convert the shares into date windows.
   - Example: season game-side dates run April 1 through September 30.
   - Stints with games `[20, 80]` get roughly first 20% of date range for stint 1 and last 80% for stint 2.
4. During variable generation, only create variables for candidates whose stint window contains the game side date.
5. Remove `_add_stint_order_constraints`.
6. The optimizer can then solve by team-season, or small team components, because chronological ordering is handled by candidate eligibility rather than cross-team pairwise constraints.

Implementation sketch:

- Add a helper that receives `game_sides` and `candidate_rows`.
- Produce `dict[(season, team_id, player_id, stint), (start_key, end_key)]`.
- For single-stint players, no restriction is needed.
- In `_build_milp_problem`, before creating a variable, skip the candidate when `game_side.date_key` is outside the candidate’s date window.
- Update tests:
  - Replace `test_multi_stint_player_assignments_follow_team_order` with one that proves the cutoff windows make earlier games eligible only for stint 1 and later games only for stint 2.
  - Keep deterministic output test.

## Current Runtime Problem

The first MILP version solved by whole season, which was too large. I added `_season_game_side_components` to split a season into connected team components, where teams are connected only if the same player has candidates for multiple teams in that season.

That component split passed focused tests, but it should be reconsidered after implementing cutoff date windows. It may still be useful, but it is no longer the main way to enforce stint chronology.

## Current SQLMesh Failure

The most recent scoped SQLMesh plan failed on `synthetic_box_score.lineup_assignments`:

```text
Cannot construct source query from an empty DataFrame. This error is commonly related to Python models that produce no data. For such models, consider yielding from an empty generator if the resulting set is empty, i.e. use `yield from ()`.
```

This means `lineup_assignments.execute` returned an empty pandas DataFrame for the actual dev input set. Possible causes:

- The optimizer/fallback produced no rows after the staging change.
- Candidate inputs for selected team-seasons are empty or no longer line up after switching appearances to fielding/fielding_of.
- The `lineup_assignments` SQLMesh Python model needs to yield an empty generator when output is empty, or otherwise construct an empty relation in the pattern SQLMesh expects for Python models.

Before rerunning a full plan, inspect dev data:

- Row count for `main_models__dev.stg_databank_appearances`
- Row count for `synthetic_box_score__dev.team_season_modal_lineups`
- Candidate query inside `lineup_assignments.py`
- Games query inside `lineup_assignments.py`

## DuckDB WAL Issue From Interrupted Plans

Interrupted SQLMesh plans repeatedly left `bc.db.wal` in a state where DuckDB could not replay it:

```text
Failure while replaying WAL file ".../bc.db.wal": Type with name PLAYER_ID does not exist
```

I moved bad WAL files aside twice:

- `/private/tmp/bc.db.wal.synthetic-lineup-optimizer.20260506-0800`
- `/private/tmp/bc.db.wal.synthetic-lineup-optimizer.20260506-0815`

After moving the WAL, this succeeded:

```bash
cd bc
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh info
```

If the next agent sees the same WAL replay failure, move the WAL aside only after understanding that it discards uncheckpointed changes from failed/interrupted SQLMesh runs.

## Verification Already Done

These passed after the optimizer and report changes:

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --group build ruff check bc/python_models/synthetic_box_scores/game_lineups.py bc/python_models/synthetic_box_scores/__init__.py bc/models/synthetic_box_score/lineup_assignments.py bc/models/synthetic_box_score/lineup_optimization_report.py bc/models/synthetic_box_score/box_score_batting_lines.py bc/models/synthetic_box_score/box_score_fielding_lines.py bc/tests/test_synthetic_game_lineups.py
```

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --group build ruff format --check bc/python_models/synthetic_box_scores/game_lineups.py bc/python_models/synthetic_box_scores/__init__.py bc/models/synthetic_box_score/lineup_assignments.py bc/models/synthetic_box_score/lineup_optimization_report.py bc/models/synthetic_box_score/box_score_batting_lines.py bc/models/synthetic_box_score/box_score_fielding_lines.py bc/tests/test_synthetic_game_lineups.py
```

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --with pyright pyright bc/python_models/synthetic_box_scores bc/models/synthetic_box_score/lineup_assignments.py bc/models/synthetic_box_score/lineup_optimization_report.py bc/models/synthetic_box_score/box_score_batting_lines.py bc/models/synthetic_box_score/box_score_fielding_lines.py bc/tests/test_synthetic_game_lineups.py
```

```bash
env UV_CACHE_DIR=/private/tmp/uv-cache uv run --group build pytest bc/tests/test_synthetic_game_lineups.py bc/tests/test_modal_lineups.py
```

Focused tests reported `28 passed`.

These SQLMesh renders passed sequentially:

```bash
cd bc
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render main_models.stg_databank_appearances
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render synthetic_box_score.team_season_modal_lineups
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render synthetic_box_score.lineup_assignments
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render synthetic_box_score.box_score_batting_lines
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render synthetic_box_score.box_score_fielding_lines
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh render synthetic_box_score.lineup_optimization_report
```

Do not run SQLMesh renders in parallel; they can collide on `bc/bc_state.db`.

## Next Steps

1. Implement stint cutoff windows in `game_lineups.py`.
2. Remove or bypass `_add_stint_order_constraints`.
3. Rework the multi-stint test around date-window eligibility.
4. Debug why `lineup_assignments` returns empty on dev inputs.
5. Make SQLMesh Python models handle empty output in the form SQLMesh accepts.
6. Rerun focused tests and renders.
7. Rerun scoped dev plan.
8. Query `synthetic_box_score__dev.lineup_optimization_report` for the largest errors once materialized.

Suggested scoped plan command:

```bash
cd bc
env UV_CACHE_DIR=/private/tmp/uv-cache BC_CONCURRENT_TASKS=1 MAX_FORK_WORKERS=1 uv run --group build sqlmesh plan dev --auto-apply --skip-linter \
  --select-model main_models.stg_databank_appearances \
  --select-model synthetic_box_score.team_season_modal_lineups \
  --select-model synthetic_box_score.box_score_games \
  --select-model synthetic_box_score.lineup_assignments \
  --select-model synthetic_box_score.box_score_batting_lines \
  --select-model synthetic_box_score.box_score_fielding_lines \
  --select-model synthetic_box_score.lineup_optimization_report \
  --select-model synthetic_box_score.box_score_pitching_lines \
  --select-model synthetic_box_score.box_score_line_scores
```

Network access is restricted in the sandbox. If SQLMesh fails with hostname resolution for `https://data.baseball.computer/...`, rerun with escalation.

