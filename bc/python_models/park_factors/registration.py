"""Programmatic @model registration for the calc_park_factor_* views.

One spec per variant — collapses 6 hand-written .py models with
identical structure (only rate_stats / filter_exp / denominator_stat
differ) into a single registration loop.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models._doc_lookup import doc
from python_models._enum_types import HAND, PARK_ID
from python_models.park_factors.builder import batter_pitcher_park_factor


def register_park_factor_model(
    *,
    variant: str,
    rate_stats: list[str],
    denominator_stat: str,
    filter_exp: str | None = None,
    use_odds: bool = True,
    batter_hand_split: bool = False,
    bounded_max: float = 10.0,
) -> None:
    """Register one calc_park_factor_<variant> view via @model."""
    name = f"main_models.calc_park_factor_{variant}"
    grain: list[str] = ["park_id", "season", "league"]
    columns: dict[str, Any] = {
        "park_id": PARK_ID,
        "season": "SMALLINT",
        "league": "VARCHAR",
    }
    column_descriptions: dict[str, str] = {
        "park_id": doc("park_id"),
        "season": doc("season"),
        "league": doc("league"),
    }
    if batter_hand_split:
        grain.append("batter_hand")
        columns["batter_hand"] = HAND
    columns["sqrt_sample_size"] = "DOUBLE"
    for s in rate_stats:
        columns[f"{s}_park_factor"] = "DOUBLE"

    _rate_stats = list(rate_stats)
    _denominator_stat = denominator_stat
    _filter_exp = filter_exp
    _use_odds = use_odds
    _batter_hand_split = batter_hand_split

    grain_tuple = exp.Tuple(expressions=[exp.column(c) for c in grain])
    audits: list[tuple[str, dict[str, Any]]] = [
        ("not_null", {"columns": grain_tuple}),
        ("valid_baseball_season", {"column": exp.column("season")}),
        (
            "bounded_range",
            {
                "column": exp.column("sqrt_sample_size"),
                "min_v": exp.convert(0),
                "max_v": exp.convert(1_000_000),
            },
        ),
    ]
    for s in rate_stats:
        audits.append(
            (
                "bounded_range",
                {
                    "column": exp.column(f"{s}_park_factor"),
                    "min_v": exp.convert(0.0),
                    "max_v": exp.convert(bounded_max),
                },
            )
        )

    @model(
        name,
        is_sql=True,
        kind="VIEW",
        columns=columns,
        column_descriptions=column_descriptions,
        grain=grain,
        audits=audits,
    )
    def entrypoint(evaluator: MacroEvaluator) -> str:
        del evaluator
        kwargs: dict[str, Any] = {
            "rate_stats": _rate_stats,
            "denominator_stat": _denominator_stat,
        }
        if _filter_exp is not None:
            kwargs["filter_exp"] = _filter_exp
        if not _use_odds:
            kwargs["use_odds"] = False
        if _batter_hand_split:
            kwargs["batter_hand_split"] = True
        return batter_pitcher_park_factor(**kwargs)


_VARIANTS: list[dict[str, Any]] = [
    {
        "variant": "in_play",
        "rate_stats": [
            "hits", "singles", "doubles", "triples",
            "reached_on_errors", "batting_outs",
        ],
        "denominator_stat": "balls_in_play",
        "filter_exp": "balls_in_play = 1",
    },
    {
        "variant": "outs",
        "rate_stats": [
            "singles", "doubles", "triples", "home_runs", "strikeouts",
            "reached_on_errors", "walks", "plate_appearances", "runs",
            "balls_in_play",
        ],
        "denominator_stat": "batting_outs",
        "use_odds": False,
    },
    {
        "variant": "plate_appearances",
        "rate_stats": [
            "singles", "doubles", "triples", "home_runs", "strikeouts",
            "reached_on_errors", "walks", "batting_outs", "runs",
            "balls_in_play",
        ],
        "denominator_stat": "plate_appearances",
    },
    {
        "variant": "trajectory_outs",
        "rate_stats": [
            "trajectory_broad_air_ball", "trajectory_ground_ball",
            "trajectory_fly_ball", "trajectory_line_drive", "trajectory_pop_up",
        ],
        "denominator_stat": "plate_appearances",
        "filter_exp": "trajectory_known = 1 AND batting_outs > 0",
    },
    {
        "variant": "hit_location",
        "rate_stats": [
            "batted_distance_plate", "batted_distance_infield",
            "batted_distance_outfield", "fielded_by_battery",
            "fielded_by_infielder", "fielded_by_outfielder",
            "batted_angle_left", "batted_angle_right", "batted_angle_middle",
            "batted_location_left_field", "batted_location_center_field",
            "batted_location_right_field",
        ],
        "denominator_stat": "plate_appearances",
        "filter_exp": "batted_location_known = 1 AND hits = 1",
        "batter_hand_split": True,
        "bounded_max": 20.0,
    },
    {
        "variant": "out_location",
        "rate_stats": [
            "batted_distance_plate", "batted_distance_infield",
            "batted_distance_outfield", "fielded_by_battery",
            "fielded_by_infielder", "fielded_by_outfielder",
            "batted_angle_left", "batted_angle_right", "batted_angle_middle",
            "batted_location_plate", "batted_location_right_infield",
            "batted_location_middle_infield", "batted_location_left_infield",
            "batted_location_left_field", "batted_location_center_field",
            "batted_location_right_field",
        ],
        "denominator_stat": "plate_appearances",
        "filter_exp": "batted_location_known = 1 AND batting_outs > 0",
        "batter_hand_split": True,
        "bounded_max": 20.0,
    },
]


def register_all() -> None:
    for spec in _VARIANTS:
        register_park_factor_model(**spec)
