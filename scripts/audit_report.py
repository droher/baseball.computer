from __future__ import annotations

import argparse
import json
import logging
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any, cast

import duckdb
from sqlglot import exp
from sqlmesh import Context
from sqlmesh.core.audit import Audit
from sqlmesh.core.model import Model

LOGGER = logging.getLogger("audit_report")
REPO_ROOT = Path(__file__).resolve().parents[1]


def effective_blocking(default: bool, args: dict[str, exp.Expr]) -> bool:
    override = args.get("blocking")
    return default if override is None else bool(override.to_py())


def literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def prepare_audit(
    model: Model, audit: Audit, args: dict[str, exp.Expr]
) -> dict[str, object]:
    entry: dict[str, object] = {
        "model": model.name,
        "audit": audit.name,
        "args": {
            key: value.sql(dialect="duckdb") for key, value in sorted(args.items())
        },
        "blocking": effective_blocking(audit.blocking, args),
    }
    try:
        entry["query"] = model.render_audit_query(
            audit, this_model=model.fqn, **cast(dict[str, Any], args)
        ).sql(dialect="duckdb")
    except Exception as error:
        entry["error"] = f"{type(error).__name__}: {error}"
        LOGGER.error("Audit %s on %s did not render: %s", audit.name, model.name, error)
    return entry


def execute_audit(
    entry: dict[str, object], cursor: duckdb.DuckDBPyConnection, sample_rows: int
) -> dict[str, object]:
    query = entry.get("query")
    if not isinstance(query, str):
        return entry
    try:
        count = cursor.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()
        assert count is not None
        entry["rows"] = int(count[0])
        if entry["rows"]:
            result = cursor.execute(f"SELECT * FROM ({query}) LIMIT {sample_rows}")
            names = [column[0] for column in result.description]
            entry["sample"] = [
                dict(zip(names, row, strict=True)) for row in result.fetchall()
            ]
            LOGGER.info(
                "%s %s on %s: %d rows",
                "Blocking failure" if entry["blocking"] else "Warning",
                entry["audit"],
                entry["model"],
                entry["rows"],
            )
        else:
            LOGGER.debug("Passed %s on %s", entry["audit"], entry["model"])
    except Exception as error:
        entry["error"] = f"{type(error).__name__}: {error}"
        LOGGER.error(
            "Audit %s on %s errored: %s", entry["audit"], entry["model"], error
        )
    return entry


def run_audits(
    models: Iterable[Model],
    connection: duckdb.DuckDBPyConnection,
    catalog: str,
    sample_rows: int,
) -> dict[str, object]:
    entries = [
        prepare_audit(model, audit, args)
        for model in sorted(models, key=lambda model: model.fqn)
        for audit, args in model.audits_with_args
    ]
    LOGGER.info("Running %d audits", len(entries))
    _ = connection.execute(f"USE {catalog}")
    results = [execute_audit(entry, connection, sample_rows) for entry in entries]
    blocking_failures = [
        entry for entry in results if entry["blocking"] and entry.get("rows")
    ]
    warnings = [
        entry for entry in results if not entry["blocking"] and entry.get("rows")
    ]
    errors = [entry for entry in results if "error" in entry]
    return {
        "summary": {
            "audits": len(results),
            "blocking_failures": len(blocking_failures),
            "warnings": len(warnings),
            "errors": len(errors),
        },
        "blocking_failures": blocking_failures,
        "warnings": warnings,
        "errors": errors,
    }


def report(
    project: Path, db: Path, output: Path, sample_rows: int
) -> dict[str, object]:
    context = Context(paths=project, load_state=False)
    catalog = context.default_catalog or "bc"
    connection = duckdb.connect()
    try:
        connection.execute(f"ATTACH {literal(str(db))} AS {catalog} (READ_ONLY)")
        result = run_audits(context.models.values(), connection, catalog, sample_rows)
    finally:
        connection.close()
    result["db"] = str(db)
    output.parent.mkdir(parents=True, exist_ok=True)
    _ = output.write_text(json.dumps(result, indent=2, default=str) + "\n")
    LOGGER.info("Audit report %s: %s", output, result["summary"])
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run every model audit read-only with its configured blocking status."
    )
    parser.add_argument("--project", type=Path, default=REPO_ROOT / "bc")
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sample-rows", type=int, default=20)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    result = report(
        args.project.resolve(),
        args.db.resolve(),
        args.output,
        args.sample_rows,
    )
    summary = cast(dict[str, int], result["summary"])
    return 1 if summary["blocking_failures"] or summary["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
