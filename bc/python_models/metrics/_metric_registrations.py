"""Register every metric used by the 9 metrics_* tables.

One Metric per (basic-rate or event-based) formula, mirrored from
bc/macros/_metric_table_body.py. The macro will be retired in Phase 2
stage G; for now both produce identical row values.

Imported for side effects from bc/python_models/metrics/__init__.py.
"""

from __future__ import annotations

from ._ibis_helpers import (
    coverage_weighted_ba,
    known_angle_out_hit_ratio,
    known_trajectory_broad_out_hit_ratio,
    known_trajectory_out_hit_ratio,
)
from .registry import Metric, register

# ---------------------------------------------------------------------------
# Basic-rate offense (and pitching uses many of the same formulas).


def _register_basic_offense() -> None:
    register(
        Metric(
            name="batting_average",
            kind="offense",
            source="season",
            numerator=lambda t: t.hits.sum(),
            denominator=lambda t: t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="on_base_percentage",
            kind="offense",
            source="season",
            numerator=lambda t: t.on_base_successes.sum(),
            denominator=lambda t: t.on_base_opportunities.sum(),
        )
    )
    register(
        Metric(
            name="slugging_percentage",
            kind="offense",
            source="season",
            numerator=lambda t: t.total_bases.sum(),
            denominator=lambda t: t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="on_base_plus_slugging",
            kind="offense",
            source="season",
            formula=lambda t: t.on_base_successes.sum() / t.on_base_opportunities.sum()
            + t.total_bases.sum() / t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="isolated_power",
            kind="offense",
            source="season",
            formula=lambda t: t.total_bases.sum() / t.at_bats.sum()
            - t.hits.sum() / t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="secondary_average",
            kind="offense",
            source="season",
            numerator=lambda t: (
                t.total_bases - t.hits + t.walks + t.stolen_bases - t.caught_stealing
            ).sum(),
            denominator=lambda t: t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="batting_average_on_balls_in_play",
            kind="offense",
            source="season",
            numerator=lambda t: (t.hits - t.home_runs).sum(),
            denominator=lambda t: t.at_bats.sum()
            - t.home_runs.sum()
            - t.strikeouts.sum()
            + t.sacrifice_flies.fill_null(0).sum(),
        )
    )
    register(
        Metric(
            name="home_run_rate",
            kind="offense",
            source="season",
            numerator=lambda t: t.home_runs.sum(),
            denominator=lambda t: t.plate_appearances.sum(),
        )
    )
    register(
        Metric(
            name="walk_rate",
            kind="offense",
            source="season",
            numerator=lambda t: t.walks.sum(),
            denominator=lambda t: t.plate_appearances.sum(),
        )
    )
    register(
        Metric(
            name="strikeout_rate",
            kind="offense",
            source="season",
            numerator=lambda t: t.strikeouts.sum(),
            denominator=lambda t: t.plate_appearances.sum(),
        )
    )
    register(
        Metric(
            name="stolen_base_percentage",
            kind="offense",
            source="season",
            numerator=lambda t: t.stolen_bases.sum(),
            denominator=lambda t: (t.stolen_bases + t.caught_stealing).sum(),
        )
    )


# ---------------------------------------------------------------------------
# Basic-rate pitching.


def _register_basic_pitching() -> None:
    # SUM(outs_recorded / 3) — DuckDB's INTEGER / 3 produces a DOUBLE.
    def innings(t):
        return (t.outs_recorded / 3).sum()

    register(
        Metric(
            name="earned_run_average",
            kind="pitching",
            source="season",
            numerator=lambda t: t.earned_runs.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="run_average",
            kind="pitching",
            source="season",
            numerator=lambda t: t.runs.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="walks_per_9_innings",
            kind="pitching",
            source="season",
            numerator=lambda t: t.walks.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="strikeouts_per_9_innings",
            kind="pitching",
            source="season",
            numerator=lambda t: t.strikeouts.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="home_runs_per_9_innings",
            kind="pitching",
            source="season",
            numerator=lambda t: t.home_runs.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="hits_per_9_innings",
            kind="pitching",
            source="season",
            numerator=lambda t: t.hits.sum() * 9,
            denominator=innings,
        )
    )
    register(
        Metric(
            name="walks_and_hits_per_innings_pitched",
            kind="pitching",
            source="season",
            numerator=lambda t: t.walks.sum() + t.hits.sum(),
            denominator=innings,
        )
    )
    register(
        Metric(
            name="strikeout_to_walk_ratio",
            kind="pitching",
            source="season",
            numerator=lambda t: t.strikeouts.sum(),
            denominator=lambda t: t.walks.sum(),
        )
    )
    register(
        Metric(
            name="walk_rate",
            kind="pitching",
            source="season",
            numerator=lambda t: t.walks.sum(),
            denominator=lambda t: t.batters_faced.sum(),
        )
    )
    register(
        Metric(
            name="strikeout_rate",
            kind="pitching",
            source="season",
            numerator=lambda t: t.strikeouts.sum(),
            denominator=lambda t: t.batters_faced.sum(),
        )
    )
    register(
        Metric(
            name="home_run_rate",
            kind="pitching",
            source="season",
            numerator=lambda t: t.home_runs.sum(),
            denominator=lambda t: t.batters_faced.sum(),
        )
    )
    register(
        Metric(
            name="batting_average_against",
            kind="pitching",
            source="season",
            numerator=lambda t: t.hits.sum(),
            denominator=lambda t: t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="on_base_percentage_against",
            kind="pitching",
            source="season",
            numerator=lambda t: t.on_base_successes.sum(),
            denominator=lambda t: t.on_base_opportunities.sum(),
        )
    )
    register(
        Metric(
            name="slugging_percentage_against",
            kind="pitching",
            source="season",
            numerator=lambda t: t.total_bases.sum(),
            denominator=lambda t: t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="on_base_plus_slugging_against",
            kind="pitching",
            source="season",
            formula=lambda t: t.on_base_successes.sum() / t.on_base_opportunities.sum()
            + t.total_bases.sum() / t.at_bats.sum(),
        )
    )
    register(
        Metric(
            name="batting_average_on_balls_in_play",
            kind="pitching",
            source="season",
            numerator=lambda t: (t.hits - t.home_runs).sum(),
            denominator=lambda t: t.at_bats.sum()
            - t.home_runs.sum()
            - t.strikeouts.sum()
            + t.sacrifice_flies.fill_null(0).sum(),
        )
    )


# ---------------------------------------------------------------------------
# Basic-rate fielding.


def _register_basic_fielding() -> None:
    register(
        Metric(
            name="fielding_percentage",
            kind="fielding",
            source="season",
            numerator=lambda t: (t.putouts + t.assists).sum(),
            denominator=lambda t: (t.putouts + t.assists + t.errors).sum(),
        )
    )
    register(
        Metric(
            name="range_factor",
            kind="fielding",
            source="season",
            numerator=lambda t: (t.putouts.sum() + t.assists.sum()) * 9,
            denominator=lambda t: (t.outs_played / 3).sum(),
        )
    )
    register(
        Metric(
            name="innings_played",
            kind="fielding",
            source="season",
            formula=lambda t: (t.outs_played.sum() / 3).round(2),
        )
    )


# ---------------------------------------------------------------------------
# Event-based: batted-ball trajectory + angle (3 sides) + direction (2 sides).
#
# Inlined dependencies: the macro defined `known_trajectory_out_hit_ratio` etc.
# as references to other named metrics, but in a single GROUP BY all aliases
# resolve at the same level — algebraically equivalent to inlining the SUMs.


def _batted_ball_metrics(kind: str) -> None:
    # Trajectory.
    register(
        Metric(
            name="known_trajectory_rate_outs",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.trajectory_known * t.balls_batted * (t.at_bats - t.hits)
            ).sum(),
            denominator=lambda t: (t.balls_batted * (t.at_bats - t.hits)).sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_rate_hits",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.trajectory_known * t.balls_batted * t.hits).sum(),
            denominator=lambda t: (t.balls_batted * t.hits).sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.trajectory_known * t.balls_batted).sum(),
            denominator=lambda t: t.balls_batted.sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_broad_rate_outs",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.trajectory_broad_known * t.balls_batted * (t.at_bats - t.hits)
            ).sum(),
            denominator=lambda t: (t.balls_batted * (t.at_bats - t.hits)).sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_broad_rate_hits",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.trajectory_broad_known * t.balls_batted * t.hits
            ).sum(),
            denominator=lambda t: (t.balls_batted * t.hits).sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_broad_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.trajectory_broad_known * t.balls_batted).sum(),
            denominator=lambda t: t.balls_batted.sum(),
        )
    )
    register(
        Metric(
            name="known_trajectory_out_hit_ratio",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            formula=known_trajectory_out_hit_ratio,
        )
    )
    register(
        Metric(
            name="known_trajectory_broad_out_hit_ratio",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            formula=known_trajectory_broad_out_hit_ratio,
        )
    )
    register(
        Metric(
            name="air_ball_rate_outs",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.trajectory_broad_air_ball * (t.at_bats - t.hits)
            ).sum(),
            denominator=lambda t: (
                t.trajectory_broad_known * (t.at_bats - t.hits)
            ).sum(),
        )
    )
    register(
        Metric(
            name="ground_ball_rate_outs",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.trajectory_broad_ground_ball * (t.at_bats - t.hits)
            ).sum(),
            denominator=lambda t: (
                t.trajectory_broad_known * (t.at_bats - t.hits)
            ).sum(),
        )
    )
    register(
        Metric(
            name="ground_air_out_ratio",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            formula=lambda t: (
                (t.trajectory_broad_ground_ball * (t.at_bats - t.hits)).sum()
                / (t.trajectory_broad_known * (t.at_bats - t.hits)).sum()
            )
            / (
                (t.trajectory_broad_air_ball * (t.at_bats - t.hits)).sum()
                / (t.trajectory_broad_known * (t.at_bats - t.hits)).sum()
            ),
        )
    )
    register(
        Metric(
            name="air_ball_hit_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.trajectory_broad_air_ball * t.hits).sum(),
            denominator=lambda t: (t.trajectory_broad_known * t.hits).sum(),
        )
    )
    register(
        Metric(
            name="ground_ball_hit_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.trajectory_broad_ground_ball * t.hits).sum(),
            denominator=lambda t: (t.trajectory_broad_known * t.hits).sum(),
        )
    )
    register(
        Metric(
            name="ground_air_hit_ratio",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            formula=lambda t: (
                (t.trajectory_broad_ground_ball * t.hits).sum()
                / (t.trajectory_broad_known * t.hits).sum()
            )
            / (
                (t.trajectory_broad_air_ball * t.hits).sum()
                / (t.trajectory_broad_known * t.hits).sum()
            ),
        )
    )
    register(
        Metric(
            name="fly_ball_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.trajectory_fly_ball.sum(),
            denominator=lambda t: t.trajectory_known.sum(),
        )
    )
    register(
        Metric(
            name="line_drive_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.trajectory_line_drive.sum(),
            denominator=lambda t: t.trajectory_known.sum(),
        )
    )
    register(
        Metric(
            name="pop_up_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.trajectory_pop_up.sum(),
            denominator=lambda t: t.trajectory_known.sum(),
        )
    )
    register(
        Metric(
            name="ground_ball_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.trajectory_ground_ball.sum(),
            denominator=lambda t: t.trajectory_broad_known.sum(),
        )
    )
    # Coverage-weighted batting averages — broad trajectory (air, ground).
    for col in ("trajectory_broad_air_ball", "trajectory_ground_ball"):
        out_name = (
            "coverage_weighted_air_ball_batting_average"
            if col == "trajectory_broad_air_ball"
            else "coverage_weighted_ground_ball_batting_average"
        )
        register(
            Metric(
                name=out_name,
                kind=kind,  # type: ignore[arg-type]
                source="event",
                formula=(
                    lambda t, _c=col: coverage_weighted_ba(
                        t, _c, known_trajectory_broad_out_hit_ratio
                    )
                ),
            )
        )
    # Coverage-weighted batting averages — fine trajectory (fly, line, pop).
    for col, out_name in (
        ("trajectory_fly_ball", "coverage_weighted_fly_ball_batting_average"),
        ("trajectory_line_drive", "coverage_weighted_line_drive_batting_average"),
        ("trajectory_pop_up", "coverage_weighted_pop_up_batting_average"),
    ):
        register(
            Metric(
                name=out_name,
                kind=kind,  # type: ignore[arg-type]
                source="event",
                formula=(
                    lambda t, _c=col: coverage_weighted_ba(
                        t, _c, known_trajectory_out_hit_ratio
                    )
                ),
            )
        )
    # Angle.
    register(
        Metric(
            name="known_angle_rate_outs",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.batted_angle_known * (t.at_bats - t.hits)).sum(),
            denominator=lambda t: (t.balls_batted * (t.at_bats - t.hits)).sum(),
        )
    )
    register(
        Metric(
            name="known_angle_rate_hits",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.batted_angle_known * t.hits).sum(),
            denominator=lambda t: t.hits.sum(),
        )
    )
    register(
        Metric(
            name="known_angle_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.batted_angle_known.sum(),
            denominator=lambda t: t.balls_batted.sum(),
        )
    )
    register(
        Metric(
            name="known_angle_out_hit_ratio",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            formula=known_angle_out_hit_ratio,
        )
    )
    for a in ("left", "right", "middle"):
        register(
            Metric(
                name=f"angle_{a}_rate_outs",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(
                    lambda t, _a=a: (
                        t[f"batted_angle_{_a}"] * (t.at_bats - t.hits)
                    ).sum()
                ),
                denominator=lambda t: (
                    t.batted_angle_known * (t.at_bats - t.hits)
                ).sum(),
            )
        )
        register(
            Metric(
                name=f"angle_{a}_rate_hits",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(lambda t, _a=a: (t[f"batted_angle_{_a}"] * t.hits).sum()),
                denominator=lambda t: (t.batted_angle_known * t.hits).sum(),
            )
        )
        register(
            Metric(
                name=f"angle_{a}_rate",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(lambda t, _a=a: t[f"batted_angle_{_a}"].sum()),
                denominator=lambda t: t.batted_angle_known.sum(),
            )
        )
        register(
            Metric(
                name=f"coverage_weighted_angle_{a}_batting_average",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                formula=(
                    lambda t, _a=a: coverage_weighted_ba(
                        t, f"batted_angle_{_a}", known_angle_out_hit_ratio
                    )
                ),
            )
        )
    # Direction.
    for d in ("pulled", "opposite_field"):
        col = f"batted_balls_{d}"
        register(
            Metric(
                name=f"{d}_rate_outs",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(lambda t, _c=col: (t[_c] * (t.at_bats - t.hits)).sum()),
                denominator=lambda t: (
                    t.batted_angle_known * (t.at_bats - t.hits)
                ).sum(),
            )
        )
        register(
            Metric(
                name=f"{d}_rate_hits",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(lambda t, _c=col: (t[_c] * t.hits).sum()),
                denominator=lambda t: (t.batted_angle_known * t.hits).sum(),
            )
        )
        register(
            Metric(
                name=f"{d}_rate",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                numerator=(lambda t, _c=col: t[_c].sum()),
                denominator=lambda t: t.batted_angle_known.sum(),
            )
        )
        register(
            Metric(
                name=f"coverage_weighted_{d}_batting_average",
                kind=kind,  # type: ignore[arg-type]
                source="event",
                formula=(
                    lambda t, _c=col: coverage_weighted_ba(
                        t, _c, known_angle_out_hit_ratio
                    )
                ),
            )
        )


def _baserunning_metrics(kind: str) -> None:
    register(
        Metric(
            name="stolen_base_attempt_rate_second",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (
                t.stolen_bases_second + t.caught_stealing_second
            ).sum(),
            denominator=lambda t: t.stolen_base_opportunities_second.sum(),
        )
    )
    register(
        Metric(
            name="stolen_base_attempt_rate_third",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.stolen_bases_third + t.caught_stealing_third).sum(),
            denominator=lambda t: t.stolen_base_opportunities_third.sum(),
        )
    )
    register(
        Metric(
            name="stolen_base_attempt_rate_home",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.stolen_bases_home + t.caught_stealing_home).sum(),
            denominator=lambda t: t.stolen_base_opportunities_home.sum(),
        )
    )
    register(
        Metric(
            name="unforced_out_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.unforced_outs_on_basepaths.sum(),
            denominator=lambda t: t.times_reached_base.sum(),
        )
    )


def _pitch_sequence_metrics(kind: str) -> None:
    register(
        Metric(
            name="pitch_strike_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.strikes.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_contact_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.swings_with_contact.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_swing_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.swings_with_contact + t.strikes_swinging).sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_ball_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.balls.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_swing_and_miss_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.strikes_swinging.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_foul_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.strikes_foul.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitched_called_strike_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: t.strikes_called.sum(),
            denominator=lambda t: t.pitches.sum(),
        )
    )
    register(
        Metric(
            name="pitch_data_coverage_rate",
            kind=kind,  # type: ignore[arg-type]
            source="event",
            numerator=lambda t: (t.pitches > 0).sum(),
            denominator=lambda t: t.plate_appearances.sum(),
        )
    )


_register_basic_offense()
_register_basic_pitching()
_register_basic_fielding()
for _kind in ("offense", "pitching"):
    _batted_ball_metrics(_kind)
    _baserunning_metrics(_kind)
    _pitch_sequence_metrics(_kind)
