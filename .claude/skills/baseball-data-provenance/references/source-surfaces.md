# Source Surfaces

Use this reference to orient provenance work before reading individual models.

## Core Source Families

| Family | Typical grain | Role |
| --- | --- | --- |
| Play-by-play events | Event and event-player | Canonical spine for event counters, states, personnel, batted-ball clues, fielding plays, pitch sequences, and baserunning events. |
| Box scores | Game-player and game-team | Official aggregate anchors for batting, pitching, fielding, line scores, decisions, earned runs, and reconciliation. |
| Gamelogs or schedule rows | Game and team-game | Coarse game existence, final results, start context, and coverage inclusion when event/box detail is absent. |
| Databank-like supplements | Season-player and season-team | Aggregate fill for season counters when project source coverage is coarser or known weaker. |
| Seed taxonomies | Code or category maps | Deterministic collapse of raw event codes into stable baseball categories. |
| Derived analysis models | Analysis or intermediate grain | Existing heuristics and candidate priors for missingness, fielding, advancement, park factors, and scorer tendencies. |
| ML artifacts | Prediction grain | Predictive signals that can propose priors or diagnostics but should not become source facts. |

## Primary Repo Surfaces

| Area | Files or models to inspect |
| --- | --- |
| Coverage taxonomy | `notes/data-coverage-taxonomy-1910-2025.md` |
| Statistical design | `notes/statistical-modeling-coverage-design.md` |
| Source metadata | `bc/external_models.yaml` |
| Game spine | `bc/models/intermediate/game_level/game_start_info.sql`, `game_results.sql`, `game_scorekeeping.sql`, `game_starting_lineups.sql`, `game_forfeits.sql` |
| Event spine and state | `bc/models/intermediate/states/event_states_full.sql`, `event_personnel_lookup.sql`, `personnel_fielding_states.sql` |
| Coverage flags | `bc/models/analyses/game_data_completeness.sql`, `event_completeness_batted_balls.sql`, `event_completeness_fielding_credit.sql` |
| Batted-ball derivation | `bc/models/intermediate/event_level/calc_batted_ball_type.sql` |
| Fielding aggregation | `bc/models/intermediate/event_level/calc_fielding_play_agg.sql`, `bc/models/intermediate/player_game_level/player_position_game_fielding_stats.sql` |
| Unknown fielding shares | `bc/models/intermediate/expectancy/unknown_fielding_play_shares.sql` |
| Data quality issues | `bc/models/intermediate/data_quality/box_score_data_issues.sql`, `team_game_data_issues.sql` |
| Park factors | `bc/python_models/park_factors/`, `bc/models/intermediate/park_factors/park_factors.sql` |
| ML features and outcomes | `bc/models/intermediate/machine_learning/ml_features.sql`, `ml_event_outcomes.sql` |

## Authority Heuristics To Preserve

- Event rows dominate event counters when the event grain is complete and audited.
- Box scores can anchor player-game and team-game official aggregates.
- Databank-like supplements can fill season aggregates but should remain visible as aggregate fills.
- Official scoring fields such as earned runs, pitcher decisions, errors, assists, and putouts are not always recoverable from physical event logic.
- Seed taxonomy collapse is derivation, not imputation, unless the raw field was missing.

## First Questions

- Is the target row supposed to exist at this grain?
- Was the source acquired for this game, team, season, and dimension?
- Is the value absent, unknown-coded, default-coded, structurally not applicable, or contradicted by another source?
- Is the source known to have row-level, field-level, or era-specific artifacts?
- Which downstream model treats this value as a fact, a mask, a weight, or an uncertain measurement?
