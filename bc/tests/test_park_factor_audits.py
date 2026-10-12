from pathlib import Path

import duckdb
import pytest
from sqlglot import exp
from sqlmesh import Context
from sqlmesh.core.audit.definition import Audit
from sqlmesh.core.model import Model
from sqlmesh.core.renderer import QueryRenderer


@pytest.fixture(scope="module")
def context() -> Context:
    return Context(paths=Path(__file__).resolve().parents[2] / "bc", load_state=False)


def rendered_audit(
    model: Model, column: str, excused_population: bool
) -> tuple[Audit, dict[str, exp.Expr], exp.Query]:
    audit, args = next(
        (audit, args)
        for audit, args in model.audits_with_args
        if audit.name == "park_factor_bounds"
        and args["column"].name == column
        and args["excused_population"].to_py() is excused_population
    )
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
        table_mapping={
            "bc.main_models.game_start_info": "bc.sqlmesh__main_models.games_fixture"
        },
        this_model=exp.to_table("bc.sqlmesh__main_models.factor_fixture"),
        column=args["column"],
        max_v=args["max_v"],
        excused_population=args["excused_population"],
    )
    assert query is not None
    return audit, args, query


@pytest.mark.parametrize(
    "variant",
    [
        "in_play",
        "outs",
        "plate_appearances",
        "trajectory_outs",
        "hit_location",
        "out_location",
    ],
)
def test_park_bounds_require_confirmed_nlb_population(
    context: Context, variant: str
) -> None:
    model = context.get_model(f"main_models.calc_park_factor_{variant}")
    assert {
        '"bc"."main_models"."game_start_info"',
        '"bc"."main_models"."event_states_full"',
        '"bc"."main_models"."event_offense_stats"',
    } <= model.depends_on
    strict_audit, strict_args = next(
        (audit, args)
        for audit, args in model.audits_with_args
        if audit.name == "park_factor_bounds"
    )
    column = strict_args["column"].name
    maximum = float(strict_args["max_v"].this)
    excessive = 11.01 if maximum == 10 else maximum + 1
    _, strict_args, strict_query = rendered_audit(model, column, False)
    _, warning_args, warning_query = rendered_audit(model, column, True)
    assert strict_args["blocking"].to_py() is True
    assert warning_args["blocking"].to_py() is False
    assert "games_fixture" in strict_query.sql(dialect="duckdb")
    assert "games_fixture" in warning_query.sql(dialect="duckdb")

    factors = [
        ("NLB_HIGH", excessive),
        ("NON_NLB_HIGH", excessive),
        ("UNKNOWN_HIGH", excessive),
        ("MIXED_HIGH", excessive),
        ("MIXED_UNKNOWN", excessive),
        ("ABSENT_HIGH", excessive),
        ("UNDER_QUALIFIED", excessive),
        ("OTHER_LEAGUE", excessive),
        ("OTHER_SEASON", excessive),
        ("INTERLEAGUE", excessive),
        ("NULL_AWAY_LEAGUE", excessive),
        ("NON_REGULAR", excessive),
        ("NLB_NEGATIVE", -1.0),
        ("NLB_NAN", float("nan")),
        ("NLB_INFINITY", float("inf")),
        ("NLB_NEG_INFINITY", float("-inf")),
        ("NLB_ZERO", 0.0),
        ("NLB_AT_LIMIT", maximum),
        ("NLB_WITHIN", 1.0),
        ("STRICT_AT_LIMIT", maximum),
        ("NLB_UNAVAILABLE", None),
    ]
    game_rows: list[tuple[str, int, str, str | None, str, bool | None]] = []
    for park, _ in factors:
        if park == "ABSENT_HIGH":
            continue
        count = 25 if park == "UNDER_QUALIFIED" else 26
        classifier = (
            None
            if park == "UNKNOWN_HIGH"
            else park not in {"NON_NLB_HIGH", "STRICT_AT_LIMIT"}
        )
        league = "OTHER" if park == "OTHER_LEAGUE" else "SYNTHETIC"
        away_league = (
            None
            if park == "NULL_AWAY_LEAGUE"
            else "OTHER"
            if park == "INTERLEAGUE"
            else league
        )
        season = 1948 if park == "OTHER_SEASON" else 1949
        game_type = "Exhibition" if park == "NON_REGULAR" else "RegularSeason"
        game_rows.extend(
            (park, season, league, away_league, game_type, classifier)
            for _ in range(count)
        )
    game_rows.extend(
        [
            ("MIXED_HIGH", 1949, "SYNTHETIC", "SYNTHETIC", "RegularSeason", False),
            ("MIXED_UNKNOWN", 1949, "SYNTHETIC", "SYNTHETIC", "RegularSeason", None),
        ]
    )
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS bc")
        connection.execute("CREATE SCHEMA bc.sqlmesh__main_models")
        connection.execute(
            "CREATE TABLE bc.sqlmesh__main_models.games_fixture "
            "(park_id VARCHAR, season INTEGER, home_league VARCHAR, "
            "away_league VARCHAR, game_type VARCHAR, is_negro_leagues BOOLEAN)"
        )
        connection.executemany(
            "INSERT INTO bc.sqlmesh__main_models.games_fixture VALUES (?, ?, ?, ?, ?, ?)",
            game_rows,
        )
        connection.execute(
            "CREATE TABLE bc.sqlmesh__main_models.factor_fixture "
            f"(park_id VARCHAR, season INTEGER, league VARCHAR, {column} DOUBLE)"
        )
        connection.executemany(
            "INSERT INTO bc.sqlmesh__main_models.factor_fixture "
            "VALUES (?, 1949, 'SYNTHETIC', ?)",
            factors,
        )
        strict_rows = connection.execute(strict_query.sql(dialect="duckdb")).fetchall()
        warning_rows = connection.execute(
            warning_query.sql(dialect="duckdb")
        ).fetchall()
    assert {row[0] for row in strict_rows} == {
        "NON_NLB_HIGH",
        "UNKNOWN_HIGH",
        "MIXED_HIGH",
        "MIXED_UNKNOWN",
        "ABSENT_HIGH",
        "UNDER_QUALIFIED",
        "OTHER_LEAGUE",
        "OTHER_SEASON",
        "INTERLEAGUE",
        "NULL_AWAY_LEAGUE",
        "NON_REGULAR",
        "NLB_NEGATIVE",
        "NLB_NAN",
        "NLB_INFINITY",
        "NLB_NEG_INFINITY",
    }
    assert {row[0] for row in warning_rows} == {"NLB_HIGH"}
