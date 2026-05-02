"""SQL builder for the 6 ``calc_park_factor_*`` analysis views.

Mirrors the legacy ``@batter_pitcher_park_factor`` jinja-style macro
(``bc/macros/_park_factors.py``) one CTE at a time. The macro is still
in the tree for the .sql callers; this Python port runs the same shape
for the new Python ``@model`` decorators.
"""

from __future__ import annotations


def batter_pitcher_park_factor(
    rate_stats: list[str],
    denominator_stat: str,
    *,
    prior_sample_size: int = 1000,
    prev_years: int = 2,
    filter_exp: str = "1=1",
    batter_hand_split: bool = False,
    use_odds: bool = True,
) -> str:
    """Build the 7-CTE park-factor SQL for one numerator-set / denominator pair.

    Args mirror the legacy macro positionally:

    - ``rate_stats``: list of numerator columns (one ``*_park_factor`` output each).
    - ``denominator_stat``: shared denominator (e.g. ``plate_appearances``).
    - ``prior_sample_size``: synthetic Bayesian prior weight per (park, season, league).
    - ``prev_years``: trailing-window length on ``unique_park_seasons`` join.
    - ``filter_exp``: raw SQL predicate applied inside the ``lines`` CTE.
    - ``batter_hand_split``: when True, partition by ``batter_hand`` end-to-end.
    - ``use_odds``: pick ``odds_park_factor`` (default) vs ``rate_park_factor``
      for the final ROUND.
    """
    stats = [denominator_stat, *rate_stats]

    indent = "                "
    hand_select = "batter_hand,\n            " if batter_hand_split else ""
    hand_filter = "AND states.batter_hand IN ('L', 'R')" if batter_hand_split else ""
    hand_join = "AND this.batter_hand = other.batter_hand" if batter_hand_split else ""
    hand_groupby_extra = ", 6" if batter_hand_split else ""
    hand_groupby_3 = ", 3" if batter_hand_split else ""
    hand_groupby_4 = ", 4" if batter_hand_split else ""

    stat_sums = (",\n" + indent).join(f"SUM(lines.{s})::INT AS {s}" for s in stats)
    multi_year_sums = (",\n" + indent).join(f"SUM(la.{s})::INT AS {s}" for s in stats)
    avg_rates = (",\n" + indent).join(
        f"SUM({s}) / SUM({denominator_stat}) AS {s}_rate" for s in rate_stats
    )
    prior_rate_cols = (",\n" + indent).join(
        f"averages.{s}_rate * {prior_sample_size} AS {s}" for s in rate_stats
    )
    self_pairs = (",\n" + indent).join(
        f"this.{s} AS this_{s},\n{indent}other.{s} AS other_{s}" for s in stats
    )
    rate_calc = (",\n" + indent).join(
        (
            f"this_{s} / this_{denominator_stat} AS this_{s}_rate,\n{indent}"
            f"other_{s} / other_{denominator_stat} AS other_{s}_rate"
        )
        for s in rate_stats
    )
    weighted_rows = (",\n" + indent).join(
        (
            f"SUM(this_{s}_rate * sample_weight) / SUM(sample_weight) AS avg_this_{s}_rate,\n{indent}"
            f"SUM(other_{s}_rate * sample_weight) / SUM(sample_weight) AS avg_other_{s}_rate,\n{indent}"
            f"avg_this_{s}_rate / NULLIF(1 - avg_this_{s}_rate, 0) AS this_{s}_odds,\n{indent}"
            f"avg_other_{s}_rate / NULLIF(1 - avg_other_{s}_rate, 0) AS other_{s}_odds,\n{indent}"
            f"this_{s}_odds / NULLIF(other_{s}_odds, 0) AS {s}_odds_park_factor,\n{indent}"
            f"avg_this_{s}_rate / NULLIF(avg_other_{s}_rate, 0) AS {s}_rate_park_factor"
        )
        for s in rate_stats
    )
    odds_or_rate = "odds" if use_odds else "rate"
    final_rounds = (",\n" + indent).join(
        f"ROUND({s}_{odds_or_rate}_park_factor, 2) AS {s}_park_factor"
        for s in rate_stats
    )

    self_joined_hand = "this.batter_hand,\n            " if batter_hand_split else ""

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
        WHERE {filter_exp}
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
        HAVING SUM(lines.{denominator_stat}) > 0
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
                AND la.season BETWEEN ups.season - {prev_years} AND ups.season
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
            {hand_select}{prior_sample_size} AS {denominator_stat},
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
            {self_joined_hand}{self_pairs},
            SQRT(LEAST(this_{denominator_stat}, other_{denominator_stat})) AS sample_size,
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
