from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path
from typing import Protocol, cast

import duckdb
import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "materialize_geometry_research.py"


class MaterializationModule(Protocol):
    def materialization_counts(
        self, database: Path, schema: str
    ) -> dict[str, object]: ...

    def validate_environment(self, environment: str) -> None: ...


def _load_script() -> MaterializationModule:
    spec = importlib.util.spec_from_file_location(
        "materialize_geometry_research_test_target", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return cast(MaterializationModule, cast(object, module))


def _write_geometry_table(
    database: Path, *, contract: str | None, side_class: str | None
) -> None:
    with duckdb.connect(str(database)) as connection:
        _ = connection.execute("CREATE SCHEMA main_models__research")
        _ = connection.execute(
            """
            CREATE TABLE main_models__research.model_input_geometry (
                geometry_dimension VARCHAR,
                observed_status VARCHAR,
                class VARCHAR,
                is_observed_class BOOLEAN,
                geometry_target_contract VARCHAR,
                dl_artifact_id VARCHAR,
                dl_p_class DOUBLE,
                propensity_p_observed DOUBLE,
                propensity_artifact_id VARCHAR,
                sentinel_type VARCHAR,
                event_key INTEGER
            )
            """
        )
        _ = connection.executemany(
            "INSERT INTO main_models__research.model_input_geometry VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    "location_side",
                    "observed",
                    side_class,
                    True,
                    contract,
                    None,
                    None,
                    None,
                    None,
                    "valid_value",
                    1,
                ),
                (
                    "location_angle",
                    "default_code",
                    "Default",
                    False,
                    contract,
                    None,
                    None,
                    None,
                    None,
                    "default",
                    1,
                ),
            ],
        )


def test_cli_rejects_unsafe_and_reserved_environments_before_output_creation(
    tmp_path: Path,
) -> None:
    for index, environment in enumerate(("prod", "dev", "Bad-Name", "x;drop")):
        output_dir = tmp_path / f"output-{index}"
        evidence = tmp_path / f"evidence-{index}.json"
        result = subprocess.run(
            [
                sys.executable,
                str(SCRIPT_PATH),
                "--output-dir",
                str(output_dir),
                "--evidence",
                str(evidence),
                "--environment",
                environment,
                "--snapshot",
                "test-snapshot",
            ],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode != 0
        assert not output_dir.exists()
        assert not evidence.exists()


@pytest.mark.parametrize(
    ("contract", "side_class", "failure"),
    [
        (None, "Left", "wrong_contract"),
        ("geometry-v2-global-side", None, "invalid_observed_side_class"),
    ],
)
def test_null_provenance_values_fail_closed(
    tmp_path: Path,
    contract: str | None,
    side_class: str | None,
    failure: str,
) -> None:
    materialization = _load_script()
    database = tmp_path / "invalid.db"
    _write_geometry_table(database, contract=contract, side_class=side_class)

    with pytest.raises(RuntimeError, match=failure):
        _ = materialization.materialization_counts(database, "main_models__research")


def test_valid_environment_and_rows_pass(tmp_path: Path) -> None:
    materialization = _load_script()
    materialization.validate_environment("arbitrary_research_env_42")
    database = tmp_path / "valid.db"
    _write_geometry_table(
        database,
        contract="geometry-v2-global-side",
        side_class="Left",
    )

    result = materialization.materialization_counts(database, "main_models__research")

    assert result["dimension_rows"] == {"location_angle": 1, "location_side": 1}
    assert result["invariant_failures"] == {
        "wrong_contract": 0,
        "stale_side_inputs": 0,
        "invalid_observed_side_class": 0,
        "invalid_angle_default_status": 0,
        "duplicate_event_dimension": 0,
    }
