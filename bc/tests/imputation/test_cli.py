from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import duckdb

from python_models.imputation.officials import OFFICIAL_ROLES
from python_models.imputation.cli import (
    VALUE_COMPONENTS,
    component_modules,
    component_query,
)
from python_models.imputation.context import ContextCompletionConfig

ROOT = Path(__file__).resolve().parents[3]


def test_component_bindings_include_existing_shared_implementations() -> None:
    package = ROOT / "bc" / "python_models" / "imputation"
    for name in ("geometry", *VALUE_COMPONENTS):
        modules = component_modules(name)
        assert all((package / module).is_file() for module in modules)
        query, schema = component_query(name, ContextCompletionConfig(sample_games=1))
        assert query and schema
    assert "geometry_fast.py" in component_modules("geometry")
    assert all("values.py" in component_modules(name) for name in VALUE_COMPONENTS)


def _source(path: Path) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA main_models")
        roles = ", ".join(f"{role} VARCHAR" for role in OFFICIAL_ROLES)
        connection.execute(
            "CREATE TABLE main_models.game_start_info (game_id VARCHAR, season SMALLINT, "
            "home_league VARCHAR, away_league VARCHAR, game_type VARCHAR, source_type VARCHAR, "
            + roles
            + ")"
        )
        connection.execute(
            "INSERT INTO main_models.game_start_info VALUES "
            "('pbp',1903,'NL','NL','RegularSeason','PlayByPlay',NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
        )


def _run(
    database: Path, output: Path, *, resume: bool = False
) -> subprocess.CompletedProcess[str]:
    arguments = [
        "uv",
        "run",
        "--no-sync",
        "python",
        "-m",
        "python_models.imputation",
        "--database",
        str(database),
        "--output",
        str(output),
        "--components",
        "officials",
        "--full",
        "--threads",
        "1",
        "--memory-limit",
        "256MB",
    ]
    if resume:
        arguments.append("--resume")
    return subprocess.run(
        arguments,
        cwd=ROOT,
        env={**os.environ, "PYTHONPATH": str(ROOT / "bc")},
        capture_output=True,
        text=True,
        check=False,
        timeout=60,
    )


def test_resume_preserves_verified_components_and_rejects_source_changes(
    tmp_path: Path,
) -> None:
    database = tmp_path / "source.db"
    output = tmp_path / "artifacts"
    _source(database)
    initial = _run(database, output)
    assert initial.returncode == 0, initial.stderr
    first = json.loads((output / "manifest.json").read_text())
    assert first["status"] == "components_complete"
    assert first["project_complete"] is False
    assert not any(first["validation"]["officials"].values())
    assert first["artifacts"]["officials"]["dependencies"]
    resumed = _run(database, output, resume=True)
    assert resumed.returncode == 0, resumed.stderr
    second = json.loads((output / "manifest.json").read_text())
    assert second["artifacts"] == first["artifacts"]
    with duckdb.connect(str(database)) as connection:
        connection.execute("UPDATE main_models.game_start_info SET season = 1904")
    changed = _run(database, output, resume=True)
    assert changed.returncode != 0
    assert "source database content changed" in changed.stderr
    rejected = json.loads((output / "manifest.json").read_text())
    assert rejected["status"] == "failed"
    assert rejected["artifacts"] == first["artifacts"]
