from __future__ import annotations

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_MLB_LEAGUE_IDS = ("AL", "NL", "AA", "UA", "PL", "FL", "NA")
_LAHMAN_SOURCED_COVERAGE_THRESHOLD = 0.5


@macro()
def is_negro_leagues_league(
    evaluator: MacroEvaluator, league_id: exp.Expression
) -> exp.Expression:
    del evaluator
    return exp.Paren(
        this=exp.Not(
            this=exp.In(
                this=league_id,
                expressions=[exp.Literal.string(lg) for lg in _MLB_LEAGUE_IDS],
            )
        )
    )


@macro()
def nlb_lahman_coverage_threshold(evaluator: MacroEvaluator) -> exp.Expression:
    del evaluator
    return exp.Literal.number(_LAHMAN_SOURCED_COVERAGE_THRESHOLD)


@macro()
def is_retrosheet_season_game(
    evaluator: MacroEvaluator, games: exp.Expression
) -> exp.Expression:
    del evaluator
    alias = games.name
    return exp.Paren(
        this=exp.condition(
            f"{alias}.game_type IS NOT NULL AND NOT ("
            f"{alias}.is_negro_leagues AND {alias}.game_type = 'RegularSeason' "
            f"AND {alias}.season IN (SELECT season FROM main_models.nlb_season_sources "
            "WHERE season_source = 'Databank'))",
            dialect="duckdb",
        )
    )


_RANGE_STATS: dict[str, dict[str, str]] = {
    "Batting": {
        "b_pa": "plate_appearances",
        "b_ab": "at_bats",
        "b_r": "runs",
        "b_h": "hits",
        "b_d": "doubles",
        "b_t": "triples",
        "b_hr": "home_runs",
        "b_rbi": "runs_batted_in",
        "b_sh": "sacrifice_hits",
        "b_sf": "sacrifice_flies",
        "b_hbp": "hit_by_pitches",
        "b_w": "walks",
        "b_iw": "intentional_walks",
        "b_k": "strikeouts",
        "b_sb": "stolen_bases",
        "b_cs": "caught_stealing",
        "b_gdp": "grounded_into_double_plays",
        "b_xi": "reached_on_interferences",
        "b_roe": "reached_on_errors",
    },
    "Pitching": {
        "p_ipouts": "outs_recorded",
        "p_bfp": "batters_faced",
        "p_h": "hits",
        "p_d": "doubles",
        "p_t": "triples",
        "p_hr": "home_runs",
        "p_r": "runs",
        "p_er": "earned_runs",
        "p_w": "walks",
        "p_iw": "intentional_walks",
        "p_k": "strikeouts",
        "p_hbp": "hit_by_pitches",
        "p_wp": "wild_pitches",
        "p_bk": "balks",
        "p_sh": "sacrifice_hits",
        "p_sf": "sacrifice_flies",
    },
    "Fielding": {
        "d_ifouts": "outs_played",
        "d_po": "putouts",
        "d_a": "assists",
        "d_e": "errors",
        "d_dp": "double_plays",
        "d_tp": "triple_plays",
        "d_pb": "passed_balls",
        "d_sb": "stolen_bases",
        "d_cs": "caught_stealing",
    },
}

_PLAYER_ONLY_RANGE_STATS: dict[str, set[str]] = {"Fielding": {"d_ifouts"}}


@macro()
def nlb_stat_range_structs(
    evaluator: MacroEvaluator, line_type: exp.Expression, line_owner: exp.Expression
) -> str:
    del evaluator
    kind = line_type.name
    is_team_line = line_owner.name == "team"
    stats = _RANGE_STATS[kind]
    skipped = _PLAYER_ONLY_RANGE_STATS.get(kind, set()) if is_team_line else set()
    structs = [
        f"{{'stat': '{name}', 'value': {col}, 'lower_bound': {col}_lower, 'upper_bound': {col}_upper}}"
        for col, name in stats.items()
        if col not in skipped
    ]
    return "[" + ", ".join(structs) + "]"
