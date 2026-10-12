from pathlib import Path

import duckdb
from sqlmesh import Context


def test_home_run_source_not_null_audit_keeps_pitcher_optional() -> None:
    repository = Path(__file__).resolve().parents[2]
    model = Context(paths=repository / "bc", load_state=False).get_model(
        "box_score.box_score_home_runs"
    )
    not_null_audit, not_null_args = next(
        (audit, args)
        for audit, args in model.audits_with_args
        if audit.name == "not_null"
    )
    required_columns = [
        expression.name for expression in not_null_args["columns"].expressions
    ]

    assert set(required_columns) == {"batter_id", "batting_side", "game_id"}

    connection = duckdb.connect()
    connection.execute(
        "CREATE TEMP TABLE home_run_fixture ("
        "batter_id VARCHAR, batting_side VARCHAR, game_id VARCHAR, pitcher_id VARCHAR)"
    )
    connection.executemany(
        "INSERT INTO home_run_fixture VALUES (?, ?, ?, ?)",
        [
            ("batter", "Home", "VALID", None),
            (None, "Home", "MISSING_BATTER", "pitcher"),
            ("batter", None, "MISSING_SIDE", "pitcher"),
            ("batter", "Home", None, "pitcher"),
        ],
    )
    audit_query = model.render_audit_query(
        not_null_audit,
        this_model="home_run_fixture",
        columns=not_null_args["columns"],
    )
    invalid_rows = connection.execute(
        f"SELECT game_id FROM ({audit_query.sql(dialect='duckdb')})"
    ).fetchall()

    assert {row[0] for row in invalid_rows} == {
        "MISSING_BATTER",
        "MISSING_SIDE",
        None,
    }
