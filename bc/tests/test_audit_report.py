from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import duckdb

from scripts.audit_report import main


def project(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "project"
    (root / "models").mkdir(parents=True)
    (root / "audits").mkdir()
    db = tmp_path / "warehouse.db"
    (root / "config.yaml").write_text(
        "gateways:\n"
        + "  local:\n"
        + "    connection:\n"
        + "      type: duckdb\n"
        + f"      database: {db}\n"
        + "default_gateway: local\n"
        + "model_defaults:\n"
        + "  dialect: duckdb\n"
    )
    (root / "audits" / "large_values.sql").write_text(
        "AUDIT (name large_values);\nSELECT * FROM @this_model WHERE a > 1\n"
    )
    (root / "models" / "values_model.sql").write_text(
        "MODEL (name fixture.values_model, kind FULL, audits ("
        + "not_null(columns := (a)), large_values(blocking := false)));\n"
        + "SELECT 1 AS a\n"
    )
    (root / "models" / "clean.sql").write_text(
        "MODEL (name fixture.clean, kind FULL, audits (not_null(columns := (a))));\n"
        + "SELECT 1 AS a\n"
    )
    return root, db


def load(output: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(output.read_text()))


def test_blocking_failures_and_warnings_are_split(tmp_path: Path) -> None:
    root, db = project(tmp_path)
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE SCHEMA fixture")
        connection.execute(
            "CREATE TABLE fixture.values_model AS SELECT * FROM (VALUES (NULL), (2), (1)) t(a)"
        )
        connection.execute("CREATE TABLE fixture.clean AS SELECT 1 AS a")
    output = tmp_path / "report.json"

    code = main(["--project", str(root), "--db", str(db), "--output", str(output)])

    report = load(output)
    assert code == 1
    assert report["summary"] == {
        "audits": 3,
        "blocking_failures": 1,
        "warnings": 1,
        "errors": 0,
    }
    failures = cast(list[dict[str, object]], report["blocking_failures"])
    warnings = cast(list[dict[str, object]], report["warnings"])
    assert [(entry["audit"], entry["rows"]) for entry in failures] == [("not_null", 1)]
    assert [(entry["audit"], entry["rows"]) for entry in warnings] == [
        ("large_values", 1)
    ]


def test_warnings_alone_pass(tmp_path: Path) -> None:
    root, db = project(tmp_path)
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE SCHEMA fixture")
        connection.execute("CREATE TABLE fixture.values_model AS SELECT 2 AS a")
        connection.execute("CREATE TABLE fixture.clean AS SELECT 1 AS a")
    output = tmp_path / "report.json"

    assert main(["--project", str(root), "--db", str(db), "--output", str(output)]) == 0
    assert load(output)["summary"] == {
        "audits": 3,
        "blocking_failures": 0,
        "warnings": 1,
        "errors": 0,
    }


def test_missing_table_is_an_error_not_a_pass(tmp_path: Path) -> None:
    root, db = project(tmp_path)
    with duckdb.connect(str(db)) as connection:
        connection.execute("CREATE SCHEMA fixture")
        connection.execute("CREATE TABLE fixture.values_model AS SELECT 1 AS a")
    output = tmp_path / "report.json"

    code = main(["--project", str(root), "--db", str(db), "--output", str(output)])

    report = load(output)
    assert code == 1
    errors = cast(list[dict[str, object]], report["errors"])
    assert [entry["model"] for entry in errors] == ["fixture.clean"]
