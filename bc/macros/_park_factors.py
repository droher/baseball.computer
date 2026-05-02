"""Park-factor stat lists + the batter_pitcher_park_factor macro.

`@batter_pitcher_park_factor(rate_stats, denominator_stat, ...)` returns a
CTE-tree SQL string used by the calc_park_factor_* analysis views.
"""

from __future__ import annotations

from sqlglot import exp
from sqlglot.expressions.core import Expression
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

ADVANCED_PARK_FACTOR_STATS: list[str] = [
    "plate_appearances",
    "singles",
    "doubles",
    "triples",
    "home_runs",
    "strikeouts",
    "walks",
    "batting_outs",
    "runs",
    "balls_in_play",
    "trajectory_fly_ball",
    "trajectory_ground_ball",
    "trajectory_line_drive",
    "trajectory_pop_up",
    "trajectory_unknown",
    "batted_distance_infield",
    "batted_distance_outfield",
    "batted_distance_unknown",
    "batted_angle_left",
    "batted_angle_right",
    "batted_angle_middle",
]

# plate_appearances is the denominator for every rate; the rest are the
# numerators a "_park_factor" gets emitted for.
ADVANCED_PARK_FACTOR_RATE_STATS: list[str] = ADVANCED_PARK_FACTOR_STATS[1:]


@macro()
def advanced_park_factor_stats(_evaluator: MacroEvaluator) -> list[str]:
    return ADVANCED_PARK_FACTOR_STATS


@macro()
def advanced_park_factor_rate_stats(_evaluator: MacroEvaluator) -> list[str]:
    return ADVANCED_PARK_FACTOR_RATE_STATS


# ---------------------------------------------------------------------------


def _str_list(arg: Expression) -> list[str]:
    """Unpack a SQL array/tuple of string literals into a Python list."""
    if isinstance(arg, (exp.Array, exp.Tuple)):
        out: list[str] = []
        for e in arg.expressions:
            if not isinstance(e, Expression):
                raise TypeError(f"unexpected element {e!r}")
            out.append(_str(e))
        return out
    raise TypeError(f"expected an array/tuple of literals, got {type(arg).__name__}")


def _str(arg: Expression) -> str:
    if isinstance(arg, (exp.Literal, exp.Column)):
        return arg.name
    return str(arg)


def _bool(arg: Expression | bool) -> bool:
    if isinstance(arg, bool):
        return arg
    if isinstance(arg, exp.Boolean):
        return bool(arg.this)
    if isinstance(arg, exp.Literal):
        return arg.name.lower() in ("true", "t", "1")
    return bool(arg)


def _int(arg: Expression | int) -> int:
    if isinstance(arg, int):
        return arg
    if isinstance(arg, exp.Literal):
        return int(arg.name)
    return int(str(arg))


@macro()
def batter_pitcher_park_factor(
    _evaluator: MacroEvaluator,
    rate_stats: Expression,
    denominator_stat: Expression,
    prior_sample_size: Expression | int = 1000,
    prev_years: Expression | int = 2,
    filter_exp: Expression | str = "1=1",
    batter_hand_split: Expression | bool = False,
    use_odds: Expression | bool = True,
) -> str:
    rates = _str_list(rate_stats)
    denom = _str(denominator_stat)
    prior = _int(prior_sample_size)
    yrs = _int(prev_years)
    filt = _str(filter_exp) if isinstance(filter_exp, Expression) else str(filter_exp)
    hand_split = _bool(batter_hand_split)
    odds = _bool(use_odds)

    stats = [denom, *rates]
    hand_select = "batter_hand,\n            " if hand_split else ""
    hand_filter = "AND states.batter_hand IN ('L', 'R')" if hand_split else ""
    hand_join = "AND this.batter_hand = other.batter_hand" if hand_split else ""
    hand_groupby_extra = ", 6" if hand_split else ""
    hand_groupby_4 = ", 4" if hand_split else ""
    hand_groupby_3 = ", 3" if hand_split else ""

    stat_sums = ",\n                ".join(
        f"SUM(lines.{s})::INT AS {s}" for s in stats
    )
    multi_year_sums = ",\n                ".join(
        f"SUM(la.{s})::INT AS {s}" for s in stats
    )
    avg_rates = ",\n                ".join(
        f"SUM({s}) / SUM({denom}) AS {s}_rate" for s in rates
    )
    prior_rate_cols = ",\n                ".join(
        f"averages.{s}_rate * {prior} AS {s}" for s in rates
    )
    self_pairs = ",\n                ".join(
        f"this.{s} AS this_{s},\n                other.{s} AS other_{s}"
        for s in stats
    )
    rate_calc = ",\n                ".join(
        (
            f"this_{s} / this_{denom} AS this_{s}_rate,\n                "
            f"other_{s} / other_{denom} AS other_{s}_rate"
        )
        for s in rates
    )
    weighted_rows = ",\n                ".join(
        (
            f"SUM(this_{s}_rate * sample_weight) / SUM(sample_weight) AS avg_this_{s}_rate,\n                "
            f"SUM(other_{s}_rate * sample_weight) / SUM(sample_weight) AS avg_other_{s}_rate,\n                "
            f"avg_this_{s}_rate / (1 - avg_this_{s}_rate) AS this_{s}_odds,\n                "
            f"avg_other_{s}_rate / (1 - avg_other_{s}_rate) AS other_{s}_odds,\n                "
            f"this_{s}_odds / other_{s}_odds AS {s}_odds_park_factor,\n                "
            f"avg_this_{s}_rate / avg_other_{s}_rate AS {s}_rate_park_factor"
        )
        for s in rates
    )
    final_rounds = ",\n                ".join(
        f"ROUND({s}_{('odds' if odds else 'rate')}_park_factor, 2) AS {s}_park_factor"
        for s in rates
    )

    return f"""
    WITH unique_park_seasons AS (
        SELECT
            park_id,
            season,
            home_league AS league
        FROM main_models.game_start_info
        WHERE game_type = 'RegularSeason'
        GROUP BY 1, 2, 3
        HAVING COUNT(*) > 25
    ),

    lines AS (
        SELECT *
        FROM main_models.event_offense_stats
        WHERE {filt}
    ),

    lines_agg AS (
        SELECT
            states.park_id,
            states.season,
            states.league,
            states.batter_id,
            states.pitcher_id,
            ANY_VALUE(states.batter_hand) AS batter_hand,
            {stat_sums}
        FROM main_models.event_states_full AS states
        INNER JOIN lines USING (event_key)
        INNER JOIN unique_park_seasons USING (season, league, park_id)
        WHERE states.game_type = 'RegularSeason'
            AND NOT states.is_interleague
            {hand_filter}
        GROUP BY 1, 2, 3, 4, 5
    ),

    multi_year_range AS MATERIALIZED (
        SELECT
            la.park_id,
            ups.season,
            la.league,
            la.batter_id,
            la.pitcher_id,
            {hand_select}{multi_year_sums}
        FROM lines_agg AS la
        INNER JOIN unique_park_seasons AS ups
            ON la.park_id = ups.park_id
                AND la.league = ups.league
                AND la.season BETWEEN ups.season - {yrs} AND ups.season
        GROUP BY 1, 2, 3, 4, 5{hand_groupby_extra}
    ),

    averages AS MATERIALIZED (
        SELECT
            season,
            league,
            {hand_select}{avg_rates}
        FROM multi_year_range
        GROUP BY 1, 2{hand_groupby_3}
    ),

    with_priors AS (
        SELECT *
        FROM multi_year_range
        UNION ALL BY NAME
        SELECT
            unique_park_seasons.park_id,
            season,
            league,
            'MARK' AS batter_id,
            'PRIOR' AS pitcher_id,
            {hand_select}{prior} AS {denom},
            {prior_rate_cols}
        FROM averages
        INNER JOIN unique_park_seasons USING (season, league)
    ),

    self_joined AS (
        SELECT
            this.park_id AS this_park_id,
            other.park_id AS other_park_id,
            this.season,
            this.league,
            this.batter_id,
            this.pitcher_id,
            {("this.batter_hand," + chr(10) + "            ") if hand_split else ""}{self_pairs},
            SQRT(LEAST(this_{denom}, other_{denom})) AS sample_size,
            SUM(sample_size) OVER (PARTITION BY this.park_id, other.park_id, this.season, this.league) AS sum_sample_size
        FROM with_priors AS this
        INNER JOIN with_priors AS other
            ON this.park_id != other.park_id
                AND this.season = other.season
                AND this.batter_id = other.batter_id
                AND this.pitcher_id = other.pitcher_id
                {hand_join}
    ),

    rate_calculation AS (
        SELECT
            *,
            {rate_calc},
            MAX(sum_sample_size) OVER (PARTITION BY this_park_id, season, league) AS scaling_factor,
            sample_size * (scaling_factor / sum_sample_size) AS sample_weight
        FROM self_joined
    ),

    weighted_average AS (
        SELECT
            this_park_id AS park_id,
            season,
            league,
            {hand_select}SUM(sample_size) AS sqrt_sample_size,
            {weighted_rows}
        FROM rate_calculation
        GROUP BY 1, 2, 3{hand_groupby_4}
    ),

    final AS (
        SELECT
            park_id,
            season,
            league,
            {hand_select}ROUND(sqrt_sample_size, 0) AS sqrt_sample_size,
            {final_rounds}
        FROM weighted_average
    )

    SELECT * FROM final
"""
