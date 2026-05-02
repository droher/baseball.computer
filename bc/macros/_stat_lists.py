"""Stat-name lists for event/game-level counting columns.

Each macro returns a list of column-name strings; callers loop with
SQLMesh's built-in `@EACH` operator. The pitching list is derived from
the offense list (extras prepended, non-pitching items removed) at module
load — no runtime set logic.
"""

from __future__ import annotations

from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

EVENT_LEVEL_OFFENSE_STATS: list[str] = [
    "plate_appearances",
    "at_bats",
    "hits",
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "total_bases",
    "strikeouts",
    "walks",
    "intentional_walks",
    "hit_by_pitches",
    "sacrifice_hits",
    "sacrifice_flies",
    "reached_on_errors",
    "reached_on_interferences",
    "inside_the_park_home_runs",
    "ground_rule_doubles",
    "infield_hits",
    "on_base_opportunities",
    "on_base_successes",
    "runs_batted_in",
    "grounded_into_double_plays",
    "double_plays",
    "triple_plays",
    "batting_outs",
    "balls_in_play",
    "balls_batted",
    "trajectory_fly_ball",
    "trajectory_ground_ball",
    "trajectory_line_drive",
    "trajectory_pop_up",
    "trajectory_unknown",
    "trajectory_known",
    "trajectory_broad_air_ball",
    "trajectory_broad_ground_ball",
    "trajectory_broad_unknown",
    "trajectory_broad_known",
    "bunts",
    "batted_distance_plate",
    "batted_distance_infield",
    "batted_distance_outfield",
    "batted_distance_unknown",
    "batted_distance_known",
    "fielded_by_battery",
    "fielded_by_infielder",
    "fielded_by_outfielder",
    "fielded_by_known",
    "fielded_by_unknown",
    "batted_angle_left",
    "batted_angle_right",
    "batted_angle_middle",
    "batted_angle_unknown",
    "batted_angle_known",
    "batted_location_plate",
    "batted_location_right_infield",
    "batted_location_middle_infield",
    "batted_location_left_infield",
    "batted_location_left_field",
    "batted_location_center_field",
    "batted_location_right_field",
    "batted_location_unknown",
    "batted_location_known",
    "batted_balls_pulled",
    "batted_balls_opposite_field",
    "runs",
    "times_reached_base",
    "times_lead_runner",
    "times_force_on_runner",
    "times_next_base_empty",
    "stolen_base_opportunities",
    "stolen_base_opportunities_second",
    "stolen_base_opportunities_third",
    "stolen_base_opportunities_home",
    "stolen_bases",
    "stolen_bases_second",
    "stolen_bases_third",
    "stolen_bases_home",
    "caught_stealing",
    "caught_stealing_second",
    "caught_stealing_third",
    "caught_stealing_home",
    "picked_off",
    "picked_off_first",
    "picked_off_second",
    "picked_off_third",
    "picked_off_caught_stealing",
    "outs_on_basepaths",
    "unforced_outs_on_basepaths",
    "outs_avoided_on_errors",
    "advances_on_wild_pitches",
    "advances_on_passed_balls",
    "advances_on_balks",
    "advances_on_unspecified_plays",
    "advances_on_defensive_indifference",
    "advances_on_errors",
    "plate_appearances_while_on_base",
    "balls_in_play_while_running",
    "balls_in_play_while_on_base",
    "batter_total_bases_while_running",
    "batter_total_bases_while_on_base",
    "extra_base_chances",
    "extra_base_advance_attempts",
    "extra_bases_taken",
    "bases_advanced",
    "bases_advanced_on_balls_in_play",
    "surplus_bases_advanced_on_balls_in_play",
    "outs_on_extra_base_advance_attempts",
    "pitches",
    "swings",
    "swings_with_contact",
    "strikes",
    "strikes_called",
    "strikes_swinging",
    "strikes_foul",
    "strikes_foul_tip",
    "strikes_in_play",
    "strikes_unknown",
    "balls",
    "balls_called",
    "balls_intentional",
    "balls_automatic",
    "unknown_pitches",
    "pitchouts",
    "pitcher_pickoff_attempts",
    "catcher_pickoff_attempts",
    "pitches_blocked_by_catcher",
    "pitches_with_runners_going",
    "passed_balls",
    "wild_pitches",
    "balks",
    "left_on_base",
    "left_on_base_with_two_outs",
]

_NON_PITCHING_STATS: frozenset[str] = frozenset(
    {
        "runs_batted_in",
        "plate_appearances_while_on_base",
        "balls_in_play_while_running",
        "balls_in_play_while_on_base",
        "batter_total_bases_while_running",
        "batter_total_bases_while_on_base",
        "times_lead_runner",
        "times_force_on_runner",
        "times_next_base_empty",
    }
)

_EXTRA_PITCHING_STATS: list[str] = [
    "batters_faced",
    "outs_recorded",
    "inherited_runners_scored",
    "bequeathed_runners_scored",
    "team_unearned_runs",
]

EVENT_LEVEL_PITCHING_STATS: list[str] = _EXTRA_PITCHING_STATS + [
    s for s in EVENT_LEVEL_OFFENSE_STATS if s not in _NON_PITCHING_STATS
]

GAME_LEVEL_PITCHING_STATS: list[str] = [
    "games_started",
    "innings_pitched",
    "inherited_runners",
    "bequeathed_runners",
    "games_relieved",
    "games_finished",
    "save_situations_entered",
    "holds",
    "blown_saves",
    "saves_by_rule",
    "save_opportunities",
    "wins",
    "losses",
    "saves",
    "earned_runs",
    "complete_games",
    "shutouts",
    "quality_starts",
    "cheap_wins",
    "tough_losses",
    "no_decisions",
    "no_hitters",
    "perfect_games",
]

FIELDING_STATS: list[str] = [
    "outs_played",
    "plate_apperances_in_field",
    "plate_appearances_in_field_with_ball_in_play",
    "putouts",
    "assists",
    "errors",
    "fielders_choices",
    "reaching_errors",
    "double_plays",
    "triple_plays",
    "ground_ball_double_plays",
    "passed_balls",
    "balls_hit_to",
    "stolen_bases",
    "caught_stealing",
    "games_left_field",
    "games_center_field",
    "games_right_field",
    "unknown_putouts_while_fielding",
    "assisted_putouts",
    "in_play_putouts",
    "in_play_assists",
    "pickoffs",
    "double_plays_started",
    "ground_ball_double_plays_started",
]

# Output cols of main_models.event_baserunning_stats. Intersected below
# with the pitching-stats list so event_pitching_stats can SUM them
# directly without a runtime filter.
_BASERUNNING_STATS_COLS: list[str] = [
    "runs",
    "outs_on_basepaths",
    "times_reached_base",
    "stolen_base_opportunities",
    "stolen_base_opportunities_second",
    "stolen_base_opportunities_third",
    "stolen_base_opportunities_home",
    "stolen_bases",
    "stolen_bases_second",
    "stolen_bases_third",
    "stolen_bases_home",
    "caught_stealing",
    "caught_stealing_second",
    "caught_stealing_third",
    "caught_stealing_home",
    "picked_off",
    "picked_off_first",
    "picked_off_second",
    "picked_off_third",
    "picked_off_caught_stealing",
    "advances_on_wild_pitches",
    "advances_on_passed_balls",
    "advances_on_balks",
    "advances_on_unspecified_plays",
    "advances_on_defensive_indifference",
    "advances_on_errors",
    "extra_base_advance_attempts",
    "bases_advanced",
    "bases_advanced_on_balls_in_play",
    "surplus_bases_advanced_on_balls_in_play",
    "outs_on_extra_base_advance_attempts",
    "outs_avoided_on_errors",
    "unforced_outs_on_basepaths",
    "extra_base_chances",
    "extra_bases_taken",
    "times_lead_runner",
    "times_force_on_runner",
    "times_next_base_empty",
]

PITCHING_BASERUNNING_COLS: list[str] = [
    s for s in _BASERUNNING_STATS_COLS if s in EVENT_LEVEL_PITCHING_STATS
]


@macro()
def event_level_offense_stats(_evaluator: MacroEvaluator) -> list[str]:
    return EVENT_LEVEL_OFFENSE_STATS


@macro()
def event_level_pitching_stats(_evaluator: MacroEvaluator) -> list[str]:
    return EVENT_LEVEL_PITCHING_STATS


@macro()
def game_level_pitching_stats(_evaluator: MacroEvaluator) -> list[str]:
    return GAME_LEVEL_PITCHING_STATS


@macro()
def fielding_stats(_evaluator: MacroEvaluator) -> list[str]:
    return FIELDING_STATS


@macro()
def pitching_baserunning_cols(_evaluator: MacroEvaluator) -> list[str]:
    return PITCHING_BASERUNNING_COLS


_COMBINED_PITCHING_STATS: list[str] = (
    EVENT_LEVEL_PITCHING_STATS + GAME_LEVEL_PITCHING_STATS
)


@macro()
def combined_pitching_stats(_evaluator: MacroEvaluator) -> list[str]:
    return _COMBINED_PITCHING_STATS


def _sum_cast_block(stats: list[str], default_cast: str) -> list[str]:
    """One SUM(stat)::TYPE AS stat per stat, with INT1 for surplus_*."""
    return [
        f"SUM({s})::{('INT1' if s.startswith('surplus') else default_cast)} AS {s}"
        for s in stats
    ]


@macro()
def offense_sum_utinyint(_evaluator: MacroEvaluator) -> list[str]:
    return _sum_cast_block(EVENT_LEVEL_OFFENSE_STATS, "UTINYINT")


@macro()
def offense_sum_usmallint(_evaluator: MacroEvaluator) -> list[str]:
    return _sum_cast_block(EVENT_LEVEL_OFFENSE_STATS, "USMALLINT")


@macro()
def pitching_combined_sum_usmallint(_evaluator: MacroEvaluator) -> list[str]:
    return _sum_cast_block(_COMBINED_PITCHING_STATS, "USMALLINT")


def _player_pitching_dtype(s: str) -> str:
    int2_cols = (
        "bases_advanced",
        "bases_advanced_on_balls_in_play",
        "surplus_bases_advanced_on_balls_in_play",
    )
    if s in int2_cols:
        return "INT2"
    if s.startswith("pitches"):
        return "USMALLINT"
    return "UTINYINT"


@macro()
def player_pitching_sum_block(_evaluator: MacroEvaluator) -> list[str]:
    return [
        f"SUM({s})::{_player_pitching_dtype(s)} AS {s}"
        for s in EVENT_LEVEL_PITCHING_STATS
    ]
