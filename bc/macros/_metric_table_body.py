"""Build the body of a metrics_* model.

`@metric_table_body(kind, *keys)` returns the full CTE-tree SQL for one of
the 9 metric tables. Per-kind column lists, basic-rate formulas, and
event-based metric formulas live as module-level constants.

If the upstream player_*_game_*_stats models add a new int counter, append
it to the matching list below or it'll be silently dropped from the metric
output. (We don't introspect at parse time because SQLMesh's data-type
parser rejects DuckDB ENUM literals.)
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_OFFENSE_INT_COLS: list[str] = [
    "plate_appearances", "at_bats", "hits", "singles", "doubles", "triples",
    "home_runs", "total_bases", "strikeouts", "walks", "intentional_walks",
    "hit_by_pitches", "sacrifice_hits", "sacrifice_flies", "reached_on_errors",
    "reached_on_interferences", "inside_the_park_home_runs",
    "ground_rule_doubles", "infield_hits", "on_base_opportunities",
    "on_base_successes", "runs_batted_in", "grounded_into_double_plays",
    "double_plays", "triple_plays", "batting_outs", "balls_in_play",
    "balls_batted", "trajectory_fly_ball", "trajectory_ground_ball",
    "trajectory_line_drive", "trajectory_pop_up", "trajectory_unknown",
    "trajectory_known", "trajectory_broad_air_ball",
    "trajectory_broad_ground_ball", "trajectory_broad_unknown",
    "trajectory_broad_known", "bunts", "batted_distance_plate",
    "batted_distance_infield", "batted_distance_outfield",
    "batted_distance_unknown", "batted_distance_known", "fielded_by_battery",
    "fielded_by_infielder", "fielded_by_outfielder", "fielded_by_known",
    "fielded_by_unknown", "batted_angle_left", "batted_angle_right",
    "batted_angle_middle", "batted_angle_unknown", "batted_angle_known",
    "batted_location_plate", "batted_location_right_infield",
    "batted_location_middle_infield", "batted_location_left_infield",
    "batted_location_left_field", "batted_location_center_field",
    "batted_location_right_field", "batted_location_unknown",
    "batted_location_known", "batted_balls_pulled",
    "batted_balls_opposite_field", "runs", "times_reached_base",
    "stolen_bases", "caught_stealing", "picked_off",
    "picked_off_caught_stealing", "outs_on_basepaths",
    "unforced_outs_on_basepaths", "outs_avoided_on_errors",
    "advances_on_wild_pitches", "advances_on_passed_balls",
    "advances_on_balks", "advances_on_unspecified_plays",
    "advances_on_defensive_indifference", "advances_on_errors",
    "plate_appearances_while_on_base", "balls_in_play_while_running",
    "balls_in_play_while_on_base", "batter_total_bases_while_running",
    "batter_total_bases_while_on_base", "extra_base_advance_attempts",
    "bases_advanced", "bases_advanced_on_balls_in_play",
    "surplus_bases_advanced_on_balls_in_play",
    "outs_on_extra_base_advance_attempts", "pitches", "swings",
    "swings_with_contact", "strikes", "strikes_called", "strikes_swinging",
    "strikes_foul", "strikes_foul_tip", "strikes_in_play", "strikes_unknown",
    "balls", "balls_called", "balls_intentional", "balls_automatic",
    "unknown_pitches", "pitchouts", "pitcher_pickoff_attempts",
    "catcher_pickoff_attempts", "pitches_blocked_by_catcher",
    "pitches_with_runners_going", "passed_balls", "wild_pitches", "balks",
    "left_on_base", "left_on_base_with_two_outs", "stolen_bases_second",
    "stolen_bases_third", "stolen_bases_home", "caught_stealing_second",
    "caught_stealing_third", "caught_stealing_home",
    "stolen_base_opportunities", "stolen_base_opportunities_second",
    "stolen_base_opportunities_third", "stolen_base_opportunities_home",
    "picked_off_first", "picked_off_second", "picked_off_third",
    "times_force_on_runner", "times_lead_runner", "times_next_base_empty",
    "extra_base_chances", "extra_bases_taken",
]

_PITCHING_INT_COLS: list[str] = [
    "wins", "losses", "saves", "earned_runs", "batters_faced",
    "outs_recorded", "inherited_runners_scored", "bequeathed_runners_scored",
    "team_unearned_runs", "at_bats", "hits", "singles", "doubles", "triples",
    "home_runs", "total_bases", "strikeouts", "walks", "intentional_walks",
    "hit_by_pitches", "sacrifice_hits", "sacrifice_flies",
    "reached_on_errors", "reached_on_interferences",
    "inside_the_park_home_runs", "ground_rule_doubles", "infield_hits",
    "on_base_opportunities", "on_base_successes",
    "grounded_into_double_plays", "double_plays", "triple_plays",
    "batting_outs", "balls_in_play", "balls_batted", "trajectory_fly_ball",
    "trajectory_ground_ball", "trajectory_line_drive", "trajectory_pop_up",
    "trajectory_unknown", "trajectory_known", "trajectory_broad_air_ball",
    "trajectory_broad_ground_ball", "trajectory_broad_unknown",
    "trajectory_broad_known", "bunts", "batted_distance_plate",
    "batted_distance_infield", "batted_distance_outfield",
    "batted_distance_unknown", "batted_distance_known", "fielded_by_battery",
    "fielded_by_infielder", "fielded_by_outfielder", "fielded_by_known",
    "fielded_by_unknown", "batted_angle_left", "batted_angle_right",
    "batted_angle_middle", "batted_angle_unknown", "batted_angle_known",
    "batted_location_plate", "batted_location_right_infield",
    "batted_location_middle_infield", "batted_location_left_infield",
    "batted_location_left_field", "batted_location_center_field",
    "batted_location_right_field", "batted_location_unknown",
    "batted_location_known", "batted_balls_pulled",
    "batted_balls_opposite_field", "runs", "times_reached_base",
    "stolen_bases", "caught_stealing", "picked_off",
    "picked_off_caught_stealing", "outs_on_basepaths",
    "unforced_outs_on_basepaths", "outs_avoided_on_errors",
    "advances_on_wild_pitches", "advances_on_passed_balls",
    "advances_on_balks", "advances_on_unspecified_plays",
    "advances_on_defensive_indifference", "advances_on_errors",
    "extra_base_advance_attempts", "bases_advanced",
    "bases_advanced_on_balls_in_play",
    "surplus_bases_advanced_on_balls_in_play",
    "outs_on_extra_base_advance_attempts", "stolen_bases_second",
    "stolen_bases_third", "stolen_bases_home", "caught_stealing_second",
    "caught_stealing_third", "caught_stealing_home",
    "stolen_base_opportunities", "stolen_base_opportunities_second",
    "stolen_base_opportunities_third", "stolen_base_opportunities_home",
    "picked_off_first", "picked_off_second", "picked_off_third", "pitches",
    "swings", "swings_with_contact", "strikes", "strikes_called",
    "strikes_swinging", "strikes_foul", "strikes_foul_tip", "strikes_in_play",
    "strikes_unknown", "balls", "balls_called", "balls_intentional",
    "balls_automatic", "unknown_pitches", "pitchouts",
    "pitcher_pickoff_attempts", "catcher_pickoff_attempts",
    "pitches_blocked_by_catcher", "pitches_with_runners_going",
    "passed_balls", "wild_pitches", "balks", "left_on_base",
    "left_on_base_with_two_outs", "games_started", "inherited_runners",
    "bequeathed_runners", "games_relieved", "games_finished",
    "save_situations_entered", "holds", "blown_saves", "saves_by_rule",
    "save_opportunities", "complete_games", "shutouts", "quality_starts",
    "cheap_wins", "tough_losses", "no_decisions", "no_hitters",
    "perfect_games", "extra_base_chances", "extra_bases_taken",
    "plate_appearances",
]

_FIELDING_INT_COLS: list[str] = [
    "games_started", "outs_played", "putouts", "assists", "errors",
    "double_plays", "triple_plays", "plate_appearances_in_field",
    "plate_appearances_in_field_with_ball_in_play", "reaching_errors",
    "fielders_choices", "assisted_putouts", "in_play_putouts",
    "in_play_assists", "balls_hit_to", "ground_ball_double_plays",
    "passed_balls", "stolen_bases", "caught_stealing",
    "unknown_putouts_while_fielding", "pickoffs", "double_plays_started",
    "ground_ball_double_plays_started",
]

_INT_COLS: dict[str, list[str]] = {
    "offense": _OFFENSE_INT_COLS,
    "pitching": _PITCHING_INT_COLS,
    "fielding": _FIELDING_INT_COLS,
}

# team_game_start_info columns NOT in any of the player_*_game_*_stats
# upstream models. Same list across all 3 kinds: only shared columns are
# the join keys (team_id, game_id), handled by USING.
_GAME_COLS: list[str] = [
    "opponent_id", "league", "opponent_league", "division",
    "opponent_division", "team_name", "opponent_name", "starting_pitcher_id",
    "opponent_starting_pitcher_id", "team_side", "date", "start_time",
    "season", "doubleheader_status", "time_of_day", "game_type",
    "bat_first_side", "sky", "field_condition", "precipitation",
    "wind_direction", "park_id", "temperature_fahrenheit", "attendance",
    "wind_speed_mph", "use_dh", "scorer", "scoring_method", "source_type",
    "umpire_home_id", "umpire_first_id", "umpire_second_id",
    "umpire_third_id", "umpire_left_id", "umpire_right_id", "filename",
    "is_regular_season", "is_postseason", "is_integrated",
    "is_negro_leagues", "is_segregated_white", "away_franchise_id",
    "home_franchise_id", "is_interleague", "lineup_map_away",
    "lineup_map_home", "fielding_map_away", "fielding_map_home", "series_id",
    "season_game_number", "series_game_number", "days_since_last_game",
]

# ---------------------------------------------------------------------------
# Basic rate stats — applied to per-season counts.

_BASIC_RATE_OFFENSE: dict[str, str] = {
    "batting_average": "SUM(hits) / SUM(at_bats)",
    "on_base_percentage": "SUM(on_base_successes) / SUM(on_base_opportunities)",
    "slugging_percentage": "SUM(total_bases) / SUM(at_bats)",
    "on_base_plus_slugging": "SUM(on_base_successes) / SUM(on_base_opportunities) + SUM(total_bases) / SUM(at_bats)",
    "isolated_power": "SUM(total_bases) / SUM(at_bats) - SUM(hits) / SUM(at_bats)",
    "secondary_average": "SUM(total_bases - hits + walks + stolen_bases - caught_stealing) / SUM(at_bats)",
    "batting_average_on_balls_in_play": "SUM(hits - home_runs) / (SUM(at_bats) - SUM(home_runs) - SUM(strikeouts) + SUM(COALESCE(sacrifice_flies, 0)))",
    "home_run_rate": "SUM(home_runs) / SUM(plate_appearances)",
    "walk_rate": "SUM(walks) / SUM(plate_appearances)",
    "strikeout_rate": "SUM(strikeouts) / SUM(plate_appearances)",
    "stolen_base_percentage": "SUM(stolen_bases) / SUM(stolen_bases + caught_stealing)",
}

_BASIC_RATE_PITCHING: dict[str, str] = {
    "earned_run_average": "SUM(earned_runs) * 9 / SUM(outs_recorded / 3)",
    "run_average": "SUM(runs) * 9 / SUM(outs_recorded / 3)",
    "walks_per_9_innings": "SUM(walks) * 9 / SUM(outs_recorded / 3)",
    "strikeouts_per_9_innings": "SUM(strikeouts) * 9 / SUM(outs_recorded / 3)",
    "home_runs_per_9_innings": "SUM(home_runs) * 9 / SUM(outs_recorded / 3)",
    "hits_per_9_innings": "SUM(hits) * 9 / SUM(outs_recorded / 3)",
    "walks_and_hits_per_innings_pitched": "(SUM(walks) + SUM(hits)) / SUM(outs_recorded / 3)",
    "strikeout_to_walk_ratio": "SUM(strikeouts) / SUM(walks)",
    "walk_rate": "SUM(walks) / SUM(batters_faced)",
    "strikeout_rate": "SUM(strikeouts) / SUM(batters_faced)",
    "home_run_rate": "SUM(home_runs) / SUM(batters_faced)",
    "batting_average_against": "SUM(hits) / SUM(at_bats)",
    "on_base_percentage_against": "SUM(on_base_successes) / SUM(on_base_opportunities)",
    "slugging_percentage_against": "SUM(total_bases) / SUM(at_bats)",
    "on_base_plus_slugging_against": "SUM(on_base_successes) / SUM(on_base_opportunities) + SUM(total_bases) / SUM(at_bats)",
    "batting_average_on_balls_in_play": "SUM(hits - home_runs) / (SUM(at_bats) - SUM(home_runs) - SUM(strikeouts) + SUM(COALESCE(sacrifice_flies, 0)))",
}

_BASIC_RATE_FIELDING: dict[str, str] = {
    "fielding_percentage": "SUM(putouts + assists) / SUM(putouts + assists + errors)",
    "range_factor": "(SUM(putouts) + SUM(assists)) * 9 / SUM(outs_played / 3)",
    "innings_played": "ROUND(SUM(outs_played) / 3, 2)",
}

_BASIC_RATE: dict[str, dict[str, str]] = {
    "offense": _BASIC_RATE_OFFENSE,
    "pitching": _BASIC_RATE_PITCHING,
    "fielding": _BASIC_RATE_FIELDING,
}

# ---------------------------------------------------------------------------
# Event-based metrics — applied to per-event rows.
# Batted-ball trajectory + angle (3 sides) + direction (2 sides).


def _build_batted_ball_stats() -> dict[str, str]:
    stats: dict[str, str] = {
        "known_trajectory_rate_outs": "SUM(trajectory_known * balls_batted * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_trajectory_rate_hits": "SUM(trajectory_known * balls_batted * hits) / SUM(balls_batted * hits)",
        "known_trajectory_rate": "SUM(trajectory_known * balls_batted) / SUM(balls_batted)",
        "known_trajectory_broad_rate_outs": "SUM(trajectory_broad_known * balls_batted * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_trajectory_broad_rate_hits": "SUM(trajectory_broad_known * balls_batted * hits) / SUM(balls_batted * hits)",
        "known_trajectory_broad_rate": "SUM(trajectory_broad_known * balls_batted) / SUM(balls_batted)",
        "known_trajectory_out_hit_ratio": "known_trajectory_rate_outs / known_trajectory_rate_hits",
        "known_trajectory_broad_out_hit_ratio": "known_trajectory_broad_rate_outs / known_trajectory_broad_rate_hits",
        "air_ball_rate_outs": "SUM(trajectory_broad_air_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_ball_rate_outs": "SUM(trajectory_broad_ground_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_air_out_ratio": "ground_ball_rate_outs / air_ball_rate_outs",
        "air_ball_hit_rate": "SUM(trajectory_broad_air_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_ball_hit_rate": "SUM(trajectory_broad_ground_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_air_hit_ratio": "ground_ball_hit_rate / air_ball_hit_rate",
        "fly_ball_rate": "SUM(trajectory_fly_ball) / SUM(trajectory_known)",
        "line_drive_rate": "SUM(trajectory_line_drive) / SUM(trajectory_known)",
        "pop_up_rate": "SUM(trajectory_pop_up) / SUM(trajectory_known)",
        "ground_ball_rate": "SUM(trajectory_ground_ball) / SUM(trajectory_broad_known)",
        "coverage_weighted_air_ball_batting_average": "SUM(trajectory_broad_air_ball * hits) * known_trajectory_broad_out_hit_ratio / (SUM(trajectory_broad_air_ball * hits) * known_trajectory_broad_out_hit_ratio + SUM(trajectory_broad_air_ball * (at_bats - hits)))",
        "coverage_weighted_ground_ball_batting_average": "SUM(trajectory_ground_ball * hits) * known_trajectory_broad_out_hit_ratio / (SUM(trajectory_ground_ball * hits) * known_trajectory_broad_out_hit_ratio + SUM(trajectory_ground_ball * (at_bats - hits)))",
        "coverage_weighted_fly_ball_batting_average": "SUM(trajectory_fly_ball * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_fly_ball * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_fly_ball * (at_bats - hits)))",
        "coverage_weighted_line_drive_batting_average": "SUM(trajectory_line_drive * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_line_drive * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_line_drive * (at_bats - hits)))",
        "coverage_weighted_pop_up_batting_average": "SUM(trajectory_pop_up * hits) * known_trajectory_out_hit_ratio / (SUM(trajectory_pop_up * hits) * known_trajectory_out_hit_ratio + SUM(trajectory_pop_up * (at_bats - hits)))",
        "known_angle_rate_outs": "SUM(batted_angle_known * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))",
        "known_angle_rate_hits": "SUM(batted_angle_known * hits) / SUM(hits)",
        "known_angle_rate": "SUM(batted_angle_known) / SUM(balls_batted)",
        "known_angle_out_hit_ratio": "known_angle_rate_outs / known_angle_rate_hits",
    }
    for a in ("left", "right", "middle"):
        stats[f"angle_{a}_rate_outs"] = f"SUM(batted_angle_{a} * (at_bats - hits)) / SUM(batted_angle_known * (at_bats - hits))"
        stats[f"angle_{a}_rate_hits"] = f"SUM(batted_angle_{a} * hits) / SUM(batted_angle_known * hits)"
        stats[f"angle_{a}_rate"] = f"SUM(batted_angle_{a}) / SUM(batted_angle_known)"
        stats[f"coverage_weighted_angle_{a}_batting_average"] = f"SUM(batted_angle_{a} * hits) * known_angle_out_hit_ratio / (SUM(batted_angle_{a} * hits) * known_angle_out_hit_ratio + SUM(batted_angle_{a} * (at_bats - hits)))"
    for d in ("pulled", "opposite_field"):
        stats[f"{d}_rate_outs"] = f"SUM(batted_balls_{d} * (at_bats - hits)) / SUM(batted_angle_known * (at_bats - hits))"
        stats[f"{d}_rate_hits"] = f"SUM(batted_balls_{d} * hits) / SUM(batted_angle_known * hits)"
        stats[f"{d}_rate"] = f"SUM(batted_balls_{d}) / SUM(batted_angle_known)"
        stats[f"coverage_weighted_{d}_batting_average"] = f"SUM(batted_balls_{d} * hits) * known_angle_out_hit_ratio / (SUM(batted_balls_{d} * hits) * known_angle_out_hit_ratio + SUM(batted_balls_{d} * (at_bats - hits)))"
    return stats


_BATTED_BALL_STATS: dict[str, str] = _build_batted_ball_stats()

_BASERUNNING_STATS: dict[str, str] = {
    "stolen_base_attempt_rate_second": "SUM(stolen_bases_second + caught_stealing_second) / SUM(stolen_base_opportunities_second)",
    "stolen_base_attempt_rate_third": "SUM(stolen_bases_third + caught_stealing_third) / SUM(stolen_base_opportunities_third)",
    "stolen_base_attempt_rate_home": "SUM(stolen_bases_home + caught_stealing_home) / SUM(stolen_base_opportunities_home)",
    "unforced_out_rate": "SUM(unforced_outs_on_basepaths) / SUM(times_reached_base)",
}

_PITCH_SEQUENCE_STATS: dict[str, str] = {
    "pitch_strike_rate": "SUM(strikes) / SUM(pitches)",
    "pitch_contact_rate": "SUM(swings_with_contact) / SUM(pitches)",
    "pitch_swing_rate": "SUM(swings_with_contact + strikes_swinging) / SUM(pitches)",
    "pitch_ball_rate": "SUM(balls) / SUM(pitches)",
    "pitch_swing_and_miss_rate": "SUM(strikes_swinging) / SUM(pitches)",
    "pitch_foul_rate": "SUM(strikes_foul) / SUM(pitches)",
    "pitched_called_strike_rate": "SUM(strikes_called) / SUM(pitches)",
    "pitch_data_coverage_rate": "COUNT_IF(pitches > 0) / SUM(plate_appearances)",
}

_EVENT_BASED_METRICS: dict[str, dict[str, str]] = {
    "offense": {**_BATTED_BALL_STATS, **_BASERUNNING_STATS, **_PITCH_SEQUENCE_STATS},
    "pitching": {**_BATTED_BALL_STATS, **_BASERUNNING_STATS, **_PITCH_SEQUENCE_STATS},
    "fielding": {},
}

_EVENT_MODELS: dict[str, str] = {
    "offense": "main_models.event_offense_stats",
    "pitching": "main_models.event_pitching_stats",
    "fielding": "main_models.event_player_fielding_stats",
}

_SEASON_MODELS: dict[str, str] = {
    "offense": "main_models.player_team_season_offense_stats",
    "pitching": "main_models.player_team_season_pitching_stats",
    "fielding": "main_models.player_position_team_season_fielding_stats",
}

# ---------------------------------------------------------------------------


def _arg_str(arg: Expression) -> str:
    """Pull the underlying string from a Literal or Column expression."""
    if isinstance(arg, (exp.Literal, exp.Column)):
        return arg.name
    return str(arg)


@macro()
def metric_table_body(
    _evaluator: MacroEvaluator,
    kind: Expression,
    *grouping_keys: Expression,
) -> str:
    kind_s = _arg_str(kind)
    if kind_s not in _INT_COLS:
        raise ValueError(
            f"Invalid kind '{kind_s}' — must be one of offense/pitching/fielding"
        )
    keys = [_arg_str(k) for k in grouping_keys]
    if not keys:
        raise ValueError("metric_table_body requires at least one grouping key")

    int_cols = _INT_COLS[kind_s]
    basic_metrics = _BASIC_RATE[kind_s]
    event_metrics = _EVENT_BASED_METRICS[kind_s]
    event_model = _EVENT_MODELS[kind_s]
    season_model = _SEASON_MODELS[kind_s]
    keys_csv = ", ".join(keys)

    keys_block = "\n            ".join(f"{k}," for k in keys)
    int_block = "\n            ".join(f"SUM({c}) AS {c}," for c in int_cols)
    basic_block = ",\n            ".join(
        f"{formula} AS {col}" for col, formula in basic_metrics.items()
    )
    game_block = ",\n                ".join(f"g.{c}" for c in _GAME_COLS)

    if event_metrics:
        event_block = ",\n            ".join(
            f"{formula} AS {col}" for col, formula in event_metrics.items()
        )
    else:
        event_block = ""

    int_final = "".join(f"\n            basic_stats.{c}::INT AS {c}," for c in int_cols)
    basic_final = "".join(f"\n            basic_stats.{c}," for c in basic_metrics)
    event_final = "".join(f",\n            event_agg.{c}" for c in event_metrics)

    return f"""
    WITH season AS (
        SELECT
            s.*,
            COALESCE(f.league, 'N/A') AS league
        FROM {season_model} AS s
        LEFT JOIN main_seeds.seed_franchises AS f
            ON s.team_id = f.team_id
            AND s.season BETWEEN EXTRACT(YEAR FROM f.date_start) AND COALESCE(EXTRACT(YEAR FROM f.date_end), 9999)
    ),
    event AS (
        SELECT
            e.*,
            {game_block}
        FROM {event_model} AS e
        LEFT JOIN main_models.team_game_start_info AS g USING (team_id, game_id)
    ),
    basic_stats AS (
        SELECT
            {keys_block}
            SUM(games) AS games,
            {int_block}
            {basic_block}
        FROM season
        WHERE game_type IN (SELECT game_type FROM main_seeds.seed_game_types WHERE is_regular_season)
        GROUP BY {keys_csv}
    ),
    event_agg AS (
        SELECT
            {keys_block}
            COUNT(DISTINCT game_id) AS games,
            {event_block}
        FROM event
        WHERE game_id IN (SELECT game_id FROM main_models.game_start_info WHERE is_regular_season)
        GROUP BY {keys_csv}
    ),
    final AS (
        SELECT
            {keys_block}{int_final}{basic_final}
            COALESCE(event_agg.games / basic_stats.games, 0) AS event_coverage_rate{event_final}
        FROM basic_stats
        LEFT JOIN event_agg USING ({keys_csv})
    )
    SELECT * FROM final
"""
