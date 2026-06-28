"""Repo paths, output roots, and env-var names for the statistical package."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]

ENV_PUBLISHED_ROOT: str = "BC_STATS_PUBLISHED_ROOT"
ENV_DB_PATH: str = "BC_DB_PATH"
ENV_STATE_DB_PATH: str = "BC_STATE_DB_PATH"

ARTIFACT_ROOT: Path = PROJECT_ROOT / "artifacts" / "statistical"
DATASETS_ROOT: Path = ARTIFACT_ROOT / "datasets"
EDA_ROOT: Path = ARTIFACT_ROOT / "eda"
DEEP_ROOT: Path = ARTIFACT_ROOT / "deep"
BAYES_ROOT: Path = ARTIFACT_ROOT / "bayes"
BASELINE_ROOT: Path = ARTIFACT_ROOT / "baseline"
GLOBAL_PUBLISHED_ROOT: Path = ARTIFACT_ROOT / "published"

DEFAULT_DEV_DB_PATH: Path = PROJECT_ROOT / "bc_dev.db"
DEFAULT_PROD_DB_PATH: Path = PROJECT_ROOT / "bc.db"

DEFAULT_START_SEASON: int = 1910
DEFAULT_END_SEASON: int = 2025


def resolve_db_path() -> Path:
    """DuckDB path the statistical CLI/library should read.

    Falls back to ``bc_dev.db`` so a bare ``python -m`` invocation does
    not silently touch the prod DB. ``just`` recipes that need prod set
    ``BC_DB_PATH`` explicitly.
    """
    raw = os.environ.get(ENV_DB_PATH)
    if raw:
        return Path(raw)
    return DEFAULT_DEV_DB_PATH


def resolve_published_roots() -> tuple[Path, Path]:
    """Return ``(branch_root, global_root)`` for published manifest lookup."""
    branch = Path(os.environ.get(ENV_PUBLISHED_ROOT, str(GLOBAL_PUBLISHED_ROOT)))
    return branch, GLOBAL_PUBLISHED_ROOT
