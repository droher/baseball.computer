from pathlib import Path
from typing import Any, cast

import duckdb
import pytest
from sqlglot import exp
from sqlmesh import Context
from sqlmesh.core.audit.definition import Audit
from sqlmesh.core.model import Model
from sqlmesh.core.renderer import QueryRenderer

SCOPED_AUDITS = {
    "box_score_data_issue_source_scope",
    "box_score_home_run_pitcher_missing",
    "team_game_has_one_starter",
    "park_factor_bounds",
}

GAME_CLASSES = [
    ("NLB_BOX", True, "BoxScore", 1933),
    ("NLB_PBP", True, "PlayByPlay", 1934),
    ("BOX_1874", False, "BoxScore", 1874),
    ("BOX_1874_NULL_CLASS", None, "BoxScore", 1874),
    ("BOX_1875", False, "BoxScore", 1875),
    ("BOX_1900", False, "BoxScore", 1900),
    ("PBP_1874", False, "PlayByPlay", 1874),
    ("GAMELOG_1874", False, "GameLog", 1874),
    ("NULL_CLASS_PBP_1874", None, "PlayByPlay", 1874),
]
LESS_COMPLETE = {"NLB_BOX", "NLB_PBP", "BOX_1874", "BOX_1874_NULL_CLASS"}
COMPLETE = {game[0] for game in GAME_CLASSES} - LESS_COMPLETE | {"NO_INFO"}


@pytest.fixture(scope="module")
def context() -> Context:
    return Context(paths=Path(__file__).resolve().parents[2] / "bc", load_state=False)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute("ATTACH ':memory:' AS bc")
    connection.execute("CREATE SCHEMA bc.main_models")
    connection.execute("CREATE SCHEMA bc.sqlmesh__main_models")
    return connection


def _add_game_classes(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        "CREATE TABLE bc.main_models.game_start_info "
        "(game_id VARCHAR, is_negro_leagues BOOLEAN, source_type VARCHAR, season USMALLINT)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.game_start_info VALUES (?, ?, ?, ?)", GAME_CLASSES
    )


def _is_excused(args: dict[str, exp.Expr]) -> bool:
    return bool(args["excused_population"].to_py())


def _scoped(
    model: Model, name: str, excused: bool
) -> tuple[Audit, dict[str, exp.Expr]]:
    return next(
        (audit, args)
        for audit, args in model.audits_with_args
        if audit.name == name and _is_excused(args) == excused
    )


def _bounded(
    model: Model, issue_type: str, column_sql: str
) -> tuple[Audit, dict[str, exp.Expr]]:
    return next(
        (audit, args)
        for audit, args in model.audits_with_args
        if audit.name == "bounded_excluding_data_issues"
        and args["issue_type"].this == issue_type
        and args["column"].sql() == column_sql
    )


def _render(
    model: Model, audit: Audit, args: dict[str, exp.Expr], this_model: str
) -> exp.Query:
    return model.render_audit_query(
        audit, this_model=this_model, **cast(dict[str, Any], args)
    )


def _render_physical(
    model: Model,
    audit: Audit,
    args: dict[str, exp.Expr],
    table_mapping: dict[str, str],
    this_model: str,
) -> exp.Query:
    renderer = QueryRenderer(
        audit.query,
        audit.dialect or model.dialect,
        audit.macro_definitions,
        jinja_macro_registry=audit.jinja_macros,
        python_env=model.python_env,
        default_catalog=model.default_catalog,
        model=model,
    )
    query = renderer.render(
        table_mapping=table_mapping,
        this_model=exp.to_table(this_model, dialect=model.dialect),
        **cast(dict[str, Any], {**audit.defaults, **args}),
    )
    assert query is not None
    return query


def _results(
    connection: duckdb.DuckDBPyConnection, query: exp.Query
) -> list[tuple[object, ...]]:
    return connection.execute(query.sql(dialect="duckdb")).fetchall()


def test_scoped_audits_block_only_outside_the_excused_population(
    context: Context,
) -> None:
    attachments = [
        (model.name, _is_excused(args), args.get("blocking"), audit.blocking)
        for model in context.models.values()
        for audit, args in model.audits_with_args
        if audit.name in SCOPED_AUDITS
    ]
    assert {name for name, *_ in attachments} >= {
        "main_models.box_score_data_issues",
        "main_models.stg_box_score_home_runs",
        "main_models.player_game_pitching_stats",
        "main_models.calc_park_factor_outs",
    }
    for name, excused, blocking_arg, default_blocking in attachments:
        blocking = default_blocking if blocking_arg is None else blocking_arg.to_py()
        assert blocking is not excused, name


def test_component_issues_block_outside_less_complete_sources(
    context: Context,
) -> None:
    model = context.get_model("main_models.box_score_data_issues")
    assert '"bc"."main_models"."game_start_info"' in model.depends_on
    connection = _connection()
    _add_game_classes(connection)
    connection.execute(
        "CREATE TABLE bc.main_models.box_score_data_issues "
        "(source_table VARCHAR, game_id VARCHAR, player_id VARCHAR, issue_type VARCHAR)"
    )
    games = [game[0] for game in GAME_CLASSES] + ["NO_INFO"]
    connection.executemany(
        "INSERT INTO bc.main_models.box_score_data_issues VALUES ('lines', ?, ?, ?)",
        [
            (game_id, f"{issue_type}_{game_id}", issue_type)
            for game_id in games
            for issue_type in (
                "extra_base_hits_gt_hits",
                "home_runs_gt_hits",
                "hits_gt_at_bats",
            )
        ],
    )
    rows = {
        excused: {
            (row[1], row[3])
            for row in _results(
                connection,
                _render(
                    model,
                    *_scoped(model, "box_score_data_issue_source_scope", excused),
                    "bc.main_models.box_score_data_issues",
                ),
            )
        }
        for excused in (False, True)
    }

    for excused, games_in_scope in ((False, COMPLETE), (True, LESS_COMPLETE)):
        assert rows[excused] == {
            (game_id, issue_type)
            for game_id in games_in_scope
            for issue_type in ("extra_base_hits_gt_hits", "home_runs_gt_hits")
        }


@pytest.mark.parametrize(
    ("model_name", "issue_type", "column_sql"),
    [
        (
            "main_models.player_game_offense_stats",
            "extra_base_hits_gt_hits",
            "(COALESCE(doubles, 0) + COALESCE(triples, 0) + COALESCE(home_runs, 0))",
        ),
        ("main_models.player_game_pitching_stats", "home_runs_gt_hits", "home_runs"),
    ],
)
def test_component_bounds_exempt_only_recorded_issues(
    context: Context, model_name: str, issue_type: str, column_sql: str
) -> None:
    model = context.get_model(model_name)
    assert '"bc"."main_models"."box_score_data_issues"' in model.depends_on
    table = f"bc.main_models.{model_name.split('.')[1]}"
    connection = _connection()
    connection.execute(
        f"CREATE TABLE {table} (game_id VARCHAR, team_id VARCHAR, player_id VARCHAR, "
        "hits UTINYINT, doubles UTINYINT, triples UTINYINT, home_runs UTINYINT)"
    )
    connection.executemany(
        f"INSERT INTO {table} VALUES (?, 'TEAM', ?, ?, 0, 0, ?)",
        [
            ("LOGGED", "logged", 0, 1),
            ("WRONG_TYPE", "wrong_type", 0, 2),
            ("UNLOGGED", "unlogged", 0, 3),
            ("VALID", "valid", 1, 1),
        ],
    )
    connection.execute(
        "CREATE TABLE bc.main_models.box_score_data_issues "
        "(game_id VARCHAR, player_id VARCHAR, issue_type VARCHAR)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.box_score_data_issues VALUES (?, ?, ?)",
        [
            ("LOGGED", "logged", issue_type),
            ("WRONG_TYPE", "wrong_type", "hits_gt_at_bats"),
        ],
    )
    query = _render(model, *_bounded(model, issue_type, column_sql), table)

    assert {row[0] for row in _results(connection, query)} == {2, 3}


def test_missing_issue_relation_cannot_exempt_invalid_rows(context: Context) -> None:
    model = context.get_model("main_models.player_game_pitching_stats")
    connection = _connection()
    connection.execute(
        "CREATE TABLE bc.main_models.player_game_pitching_stats "
        "(game_id VARCHAR, player_id VARCHAR, hits UTINYINT, home_runs UTINYINT)"
    )
    connection.execute(
        "INSERT INTO bc.main_models.player_game_pitching_stats VALUES ('BAD', 'p', 0, 1)"
    )
    query = _render(
        model,
        *_bounded(model, "home_runs_gt_hits", "home_runs"),
        "bc.main_models.player_game_pitching_stats",
    )

    with pytest.raises(duckdb.CatalogException):
        _results(connection, query)


@pytest.mark.parametrize("suffix", ["4190534890", "4190534890__dev"])
def test_box_score_issue_match_uses_physical_table_mapping(
    context: Context, suffix: str
) -> None:
    model = context.get_model("main_models.player_game_pitching_stats")
    pitching = (
        f"bc.sqlmesh__main_models.main_models__player_game_pitching_stats__{suffix}"
    )
    issues = f"bc.sqlmesh__main_models.main_models__box_score_data_issues__571117782{'__dev' if suffix.endswith('__dev') else ''}"
    connection = _connection()
    connection.execute(
        f"CREATE TABLE {pitching} (game_id VARCHAR, player_id VARCHAR, hits UTINYINT, home_runs UTINYINT)"
    )
    connection.executemany(
        f"INSERT INTO {pitching} VALUES (?, ?, 0, 1)",
        [("LOGGED", "logged"), ("UNLOGGED", "unlogged")],
    )
    connection.execute(
        f"CREATE TABLE {issues} (game_id VARCHAR, player_id VARCHAR, issue_type VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {issues} VALUES ('LOGGED', 'logged', 'home_runs_gt_hits')"
    )
    query = _render_physical(
        model,
        *_bounded(model, "home_runs_gt_hits", "home_runs"),
        {"bc.main_models.box_score_data_issues": issues},
        pitching,
    )

    assert exp.to_table(issues).sql(dialect="duckdb", identify=True) in query.sql(
        dialect="duckdb"
    )
    assert _results(connection, query) == [(1,)]


def test_team_game_issue_match_uses_physical_table_mapping(context: Context) -> None:
    model = context.get_model("main_models.player_game_pitching_stats")
    assert {
        '"bc"."main_models"."team_game_data_issues"',
        '"bc"."main_models"."team_game_start_info"',
    } <= model.depends_on
    pitching = (
        "bc.sqlmesh__main_models.main_models__player_game_pitching_stats__5923487210"
    )
    issues = "bc.sqlmesh__main_models.main_models__team_game_data_issues__3468090172"
    team_games = "bc.sqlmesh__main_models.main_models__team_game_start_info__1357924680"
    connection = _connection()
    connection.execute(
        f"CREATE TABLE {pitching} (game_id VARCHAR, team_id VARCHAR, games_started UTINYINT)"
    )
    connection.executemany(
        f"INSERT INTO {pitching} VALUES (?, ?, 0)",
        [("LOGGED", "T1"), ("UNLOGGED", "T2")],
    )
    connection.execute(
        f"CREATE TABLE {issues} (game_id VARCHAR, team_id VARCHAR, issue_type VARCHAR)"
    )
    connection.execute(
        f"INSERT INTO {issues} VALUES ('LOGGED', 'T1', 'starting_pitcher_no_appearance')"
    )
    connection.execute(
        f"CREATE TABLE {team_games} (game_id VARCHAR, team_id VARCHAR, "
        "source_type VARCHAR, is_negro_leagues BOOLEAN, season USMALLINT)"
    )
    connection.executemany(
        f"INSERT INTO {team_games} VALUES (?, ?, 'PlayByPlay', FALSE, 1900)",
        [("LOGGED", "T1"), ("UNLOGGED", "T2")],
    )
    query = _render_physical(
        model,
        *_scoped(model, "team_game_has_one_starter", excused=False),
        {
            "bc.main_models.team_game_data_issues": issues,
            "bc.main_models.team_game_start_info": team_games,
        },
        pitching,
    )
    rendered = query.sql(dialect="duckdb")

    assert exp.to_table(issues).sql(dialect="duckdb", identify=True) in rendered
    assert exp.to_table(team_games).sql(dialect="duckdb", identify=True) in rendered
    assert _results(connection, query) == [("UNLOGGED", "T2", 0)]


def test_missing_home_run_pitcher_is_excused_only_for_less_complete_sources(
    context: Context,
) -> None:
    model = context.get_model("main_models.stg_box_score_home_runs")
    connection = _connection()
    _add_game_classes(connection)
    connection.execute(
        "CREATE TABLE bc.main_models.stg_box_score_home_runs "
        "(game_id VARCHAR, batting_side VARCHAR, batter_id VARCHAR, pitcher_id VARCHAR)"
    )
    games = [game[0] for game in GAME_CLASSES] + ["NO_INFO"]
    connection.executemany(
        "INSERT INTO bc.main_models.stg_box_score_home_runs VALUES (?, 'Home', ?, ?)",
        [(game_id, game_id.lower(), None) for game_id in games]
        + [(game_id, "known", "pitcher") for game_id in games],
    )
    for excused, expected in ((False, COMPLETE), (True, LESS_COMPLETE)):
        query = _render(
            model,
            *_scoped(model, "box_score_home_run_pitcher_missing", excused),
            "bc.main_models.stg_box_score_home_runs",
        )
        assert {row[0] for row in _results(connection, query)} == expected


def test_ambiguous_box_score_pitcher_order_is_recorded_and_excused(
    context: Context,
) -> None:
    issues_model = context.get_model("main_models.team_game_data_issues")
    pitching_model = context.get_model("main_models.player_game_pitching_stats")
    connection = _connection()
    connection.execute(
        "CREATE TABLE bc.main_models.game_start_info (game_id VARCHAR, home_team_id VARCHAR, "
        "away_team_id VARCHAR, home_starting_pitcher_id VARCHAR, "
        "away_starting_pitcher_id VARCHAR, is_negro_leagues BOOLEAN, source_type VARCHAR, "
        "season USMALLINT)"
    )
    games = [
        ("NW1193407222", "NW1", "CV8", None, None, True, "BoxScore", 1934),
        ("PTC193605171", "PTC", "NW1", None, None, True, "BoxScore", 1936),
        ("PTC193307240", "PTC", "CAG", None, None, True, "BoxScore", 1933),
        ("NLB_PBP_ORDER", "NP1", "NP2", None, None, True, "PlayByPlay", 1934),
        ("NLB_GAMELOG_ORDER", "NG1", "NG2", None, None, True, "GameLog", 1934),
        ("STRICT_BAD", "A", "B", None, None, False, "PlayByPlay", 1900),
        ("UNKNOWN_BAD", "U", "V", None, None, None, "PlayByPlay", 1900),
        ("PBP_SCRATCH", "BX1", "BX2", "scratch", None, False, "PlayByPlay", 1900),
        ("NLB_NO_PITCHERS", "LOW", "NO", None, None, True, "BoxScore", 1934),
        ("GAMELOG_OBSERVED", "GL2", "GL3", None, None, False, "GameLog", 1900),
        ("BOX_1874_AMBIG", "H74", "A74", None, None, False, "BoxScore", 1874),
        ("BOX_1874_NULL_CLASS", "HN74", "AN74", None, None, None, "BoxScore", 1874),
        ("BOX_1875_AMBIG", "H75", "A75", None, None, False, "BoxScore", 1875),
        ("PBP_1874_BAD", "P74", "PA74", None, None, False, "PlayByPlay", 1874),
        ("GAMELOG_1874_BAD", "GL74", "GLA74", None, None, False, "GameLog", 1874),
    ]
    connection.executemany(
        "INSERT INTO bc.main_models.game_start_info VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        games,
    )
    connection.execute(
        "CREATE TABLE bc.main_models.stg_games (game_id VARCHAR, source_type VARCHAR)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.stg_games VALUES (?, ?)",
        [(game[0], game[6]) for game in games],
    )
    connection.execute(
        "CREATE TABLE bc.main_models.event_pitching_stats "
        "(game_id VARCHAR, team_id VARCHAR, player_id VARCHAR)"
    )
    connection.execute(
        "CREATE TABLE bc.main_models.stg_box_score_pitching_lines "
        "(game_id VARCHAR, side VARCHAR, pitcher_id VARCHAR, nth_pitcher UTINYINT)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.stg_box_score_pitching_lines VALUES (?, ?, ?, ?)",
        [
            ("NW1193407222", "Home", "valid_nw", 1),
            ("NW1193407222", "Away", "rober105", 1),
            ("NW1193407222", "Away", "byrdb101", 1),
            ("PTC193605171", "Away", "valid_nw", 1),
            ("PTC193605171", "Home", "paigs101", 1),
            ("PTC193605171", "Home", "stres101", 1),
            ("PTC193307240", "Away", "powem104", 1),
            ("PTC193307240", "Away", "fostw101", 1),
            ("PTC193307240", "Home", "matll101", 2),
            ("NLB_PBP_ORDER", "Home", "npbp_a", 1),
            ("NLB_PBP_ORDER", "Home", "npbp_b", 1),
            ("NLB_GAMELOG_ORDER", "Away", "ngl_a", 1),
            ("NLB_GAMELOG_ORDER", "Away", "ngl_b", 1),
            ("BOX_1874_AMBIG", "Home", "h74a", 1),
            ("BOX_1874_AMBIG", "Home", "h74b", 1),
            ("BOX_1874_AMBIG", "Away", "a74", 1),
            ("BOX_1874_NULL_CLASS", "Home", "hn74a", 1),
            ("BOX_1874_NULL_CLASS", "Home", "hn74b", 1),
            ("BOX_1874_NULL_CLASS", "Away", "an74", 1),
            ("BOX_1875_AMBIG", "Home", "h75a", 1),
            ("BOX_1875_AMBIG", "Home", "h75b", 1),
            ("BOX_1875_AMBIG", "Away", "a75", 1),
            ("GAMELOG_1874_BAD", "Home", "gl74a", 1),
            ("GAMELOG_1874_BAD", "Home", "gl74b", 1),
            ("GAMELOG_1874_BAD", "Away", "gla74", 1),
        ],
    )
    issue_query = issues_model.render_query()
    assert issue_query is not None
    issue_rows = _results(connection, issue_query)
    expected_excused = {
        ("NW1193407222", "CV8"),
        ("PTC193605171", "PTC"),
        ("PTC193307240", "CAG"),
        ("PTC193307240", "PTC"),
        ("NLB_NO_PITCHERS", "LOW"),
        ("NLB_NO_PITCHERS", "NO"),
        ("BOX_1874_AMBIG", "H74"),
        ("BOX_1874_NULL_CLASS", "HN74"),
    }
    assert {
        (row[0], row[1])
        for row in issue_rows
        if row[2] == "nlb_box_score_starting_pitcher_order_ambiguous"
    } == expected_excused
    connection.execute(
        "CREATE TABLE bc.main_models.team_game_data_issues "
        "(game_id VARCHAR, team_id VARCHAR, issue_type VARCHAR, notes VARCHAR)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.team_game_data_issues VALUES (?, ?, ?, ?)",
        issue_rows,
    )
    connection.execute(
        "CREATE TABLE bc.main_models.team_game_start_info (game_id VARCHAR, "
        "team_id VARCHAR, is_negro_leagues BOOLEAN, source_type VARCHAR, season USMALLINT)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.team_game_start_info VALUES (?, ?, ?, ?, ?)",
        [
            (game_id, team_id, nlb, source, season)
            for game_id, home, away, _, _, nlb, source, season in games
            if game_id not in {"NLB_PBP_ORDER", "NLB_GAMELOG_ORDER"}
            for team_id in (home, away)
        ]
        + [("GAMELOG_ZERO", "GL1", False, "GameLog", 1900)],
    )
    connection.execute(
        "CREATE TABLE bc.main_models.player_game_pitching_stats "
        "(game_id VARCHAR, team_id VARCHAR, games_started UTINYINT)"
    )
    connection.executemany(
        "INSERT INTO bc.main_models.player_game_pitching_stats VALUES (?, ?, ?)",
        [
            ("NW1193407222", "NW1", 1),
            ("NW1193407222", "CV8", 2),
            ("PTC193605171", "PTC", 2),
            ("PTC193605171", "NW1", 1),
            ("PTC193307240", "CAG", 2),
            ("STRICT_BAD", "A", 2),
            ("STRICT_BAD", "B", 1),
            ("UNKNOWN_BAD", "U", 2),
            ("UNKNOWN_BAD", "V", 1),
            ("PBP_SCRATCH", "BX2", 1),
            ("GAMELOG_OBSERVED", "GL2", 2),
            ("GAMELOG_OBSERVED", "GL3", 1),
            ("BOX_1874_AMBIG", "H74", 2),
            ("BOX_1874_AMBIG", "A74", 1),
            ("BOX_1874_NULL_CLASS", "AN74", 1),
            ("BOX_1875_AMBIG", "H75", 2),
            ("BOX_1875_AMBIG", "A75", 1),
            ("PBP_1874_BAD", "P74", 0),
            ("PBP_1874_BAD", "PA74", 1),
            ("GAMELOG_1874_BAD", "GL74", 2),
            ("GAMELOG_1874_BAD", "GLA74", 1),
            ("NO_CLASSIFIER", "MISSING_INFO", 2),
        ],
    )
    rows = {
        excused: {
            (row[0], row[1])
            for row in _results(
                connection,
                _render(
                    pitching_model,
                    *_scoped(pitching_model, "team_game_has_one_starter", excused),
                    "bc.main_models.player_game_pitching_stats",
                ),
            )
        }
        for excused in (False, True)
    }

    assert rows[False] == {
        ("STRICT_BAD", "A"),
        ("UNKNOWN_BAD", "U"),
        ("NO_CLASSIFIER", "MISSING_INFO"),
        ("GAMELOG_OBSERVED", "GL2"),
        ("BOX_1875_AMBIG", "H75"),
        ("PBP_1874_BAD", "P74"),
        ("GAMELOG_1874_BAD", "GL74"),
    }
    assert rows[True] == expected_excused
