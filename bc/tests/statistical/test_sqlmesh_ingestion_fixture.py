"""Confirm an ingestion-style helper reads a local manifest without importing PyMC.

Mirrors the doc-05 ingestion @model contract: when a published manifest
is missing the helper returns a typed empty DataFrame and logs a
warning (per 2026-05-13 review decision); otherwise it returns rows
from the artifact-id-referenced Parquet.
"""

from __future__ import annotations

import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl

from python_models.statistical import config as cfg
from python_models.statistical.manifests import (
    find_published_manifest,
    new_artifact_id,
    package_versions,
    write_manifest,
)
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.schemas import ArtifactManifest

EXAMPLE_COLUMNS: dict[str, str] = {
    "event_key": "uint32",
    "expected_credit": "float64",
    "model_version": "string",
    "artifact_id": "string",
}


def _empty_typed_frame() -> pd.DataFrame:
    return pd.DataFrame({n: pd.Series(dtype=dt) for n, dt in EXAMPLE_COLUMNS.items()})


def _ingestion_step(model_name: str, log: logging.Logger) -> pd.DataFrame:
    manifest_path = find_published_manifest(model_name)
    if manifest_path is None:
        log.warning(
            "manifest missing for %s; emitting empty table",
            model_name,
        )
        return _empty_typed_frame()
    manifest = ArtifactManifest.model_validate_json(manifest_path.read_text(encoding="utf-8"))
    parquet_path = Path(str(manifest.output_paths["expected_counters"]))
    return pl.read_parquet(parquet_path).to_pandas()


def test_missing_manifest_typed_empty(tmp_path: Path, caplog) -> None:
    branch_root = tmp_path / "branch"
    global_root = tmp_path / "global"
    branch_root.mkdir()
    global_root.mkdir()
    previous_branch = os.environ.get(cfg.ENV_PUBLISHED_ROOT)
    previous_global = cfg.GLOBAL_PUBLISHED_ROOT
    os.environ[cfg.ENV_PUBLISHED_ROOT] = str(branch_root)
    cfg.GLOBAL_PUBLISHED_ROOT = global_root  # type: ignore[misc]
    log = logging.getLogger("test_sqlmesh_ingestion_fixture")
    log.propagate = True
    try:
        with caplog.at_level(logging.WARNING, logger="test_sqlmesh_ingestion_fixture"):
            df = _ingestion_step("never_published", log)
    finally:
        cfg.GLOBAL_PUBLISHED_ROOT = previous_global  # type: ignore[misc]
        if previous_branch is None:
            _ = os.environ.pop(cfg.ENV_PUBLISHED_ROOT, None)
        else:
            os.environ[cfg.ENV_PUBLISHED_ROOT] = previous_branch

    assert list(df.columns) == list(EXAMPLE_COLUMNS)
    assert len(df) == 0
    for name, dtype in EXAMPLE_COLUMNS.items():
        assert str(df[name].dtype) == dtype
    assert any("manifest missing" in rec.message for rec in caplog.records)


def test_present_manifest_returns_data(tmp_path: Path) -> None:
    branch_root = tmp_path / "branch"
    global_root = tmp_path / "global"
    branch_root.mkdir()
    global_root.mkdir()
    parquet_path = tmp_path / "expected_counters.parquet"
    write_parquet_atomic(
        pl.DataFrame(
            {
                "event_key": pl.Series([1, 2], dtype=pl.UInt32),
                "expected_credit": [0.25, 0.75],
                "model_version": ["0.1.0", "0.1.0"],
                "artifact_id": ["a-1", "a-1"],
            }
        ),
        parquet_path,
    )
    manifest = ArtifactManifest(
        artifact_id=new_artifact_id(),
        kind="sql_export",
        name="fielding_credit",
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-1",
        output_paths={"expected_counters": parquet_path},
        package_versions=package_versions(),
    )
    manifest_path = branch_root / "fielding_credit.json"
    write_manifest(manifest, manifest_path)

    previous_branch = os.environ.get(cfg.ENV_PUBLISHED_ROOT)
    previous_global = cfg.GLOBAL_PUBLISHED_ROOT
    os.environ[cfg.ENV_PUBLISHED_ROOT] = str(branch_root)
    cfg.GLOBAL_PUBLISHED_ROOT = global_root  # type: ignore[misc]
    try:
        df = _ingestion_step("fielding_credit", logging.getLogger(__name__))
    finally:
        cfg.GLOBAL_PUBLISHED_ROOT = previous_global  # type: ignore[misc]
        if previous_branch is None:
            _ = os.environ.pop(cfg.ENV_PUBLISHED_ROOT, None)
        else:
            os.environ[cfg.ENV_PUBLISHED_ROOT] = previous_branch

    assert len(df) == 2
    assert df["expected_credit"].tolist() == [0.25, 0.75]


def test_ingestion_path_does_not_import_pymc() -> None:
    """The ingestion-only modules must not transitively pull pymc.

    Runs in a fresh subprocess so an earlier pytest test that imported
    pymc (e.g. the smoke fit) cannot pollute ``sys.modules`` here.
    """
    import subprocess

    code = (
        "import sys\n"
        "import python_models.statistical.manifests as m\n"
        "import python_models.statistical.schemas as s\n"
        "import python_models.statistical.outputs as o\n"
        "_ = (m, s, o)\n"
        "assert 'pymc' not in sys.modules, sorted(sys.modules)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(cfg.PROJECT_ROOT / "bc"),
    )
    assert proc.returncode == 0, (
        f"ingestion modules transitively imported pymc.\nstdout: {proc.stdout}\nstderr: {proc.stderr}"
    )
