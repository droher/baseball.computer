"""Repo paths, output roots, and env-var names for the statistical package."""

from __future__ import annotations

import os
from pathlib import Path

PROJECT_ROOT: Path = Path(__file__).resolve().parents[3]

ENV_ARTIFACTS_ROOT: str = "BC_STATS_ARTIFACTS_ROOT"
ENV_PUBLISHED_ROOT: str = "BC_STATS_PUBLISHED_ROOT"
ENV_GLOBAL_PUBLISHED_ROOT: str = "BC_STATS_GLOBAL_PUBLISHED_ROOT"
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


def resolve_artifact_root() -> Path:
    """Directory holding the canonical statistical artifacts.

    Defaults to ``artifacts/statistical/`` inside this checkout.
    ``BC_STATS_ARTIFACTS_ROOT`` points a different checkout (the CI
    runner's) at the canonical directory; published pointers and any
    relative ``manifest_path`` resolve under it.
    """
    raw = os.environ.get(ENV_ARTIFACTS_ROOT)
    if raw:
        return Path(raw)
    return ARTIFACT_ROOT


def resolve_global_published_root() -> Path:
    """Global cross-branch published root, overridable for test hermeticity.

    Defaults to ``published/`` under ``resolve_artifact_root()`` so
    production behavior is unchanged; ``BC_STATS_GLOBAL_PUBLISHED_ROOT``
    redirects it.
    """
    raw = os.environ.get(ENV_GLOBAL_PUBLISHED_ROOT)
    if raw:
        return Path(raw)
    return resolve_artifact_root() / "published"


def resolve_published_roots() -> tuple[Path, Path]:
    """Return ``(branch_root, global_root)`` for published manifest lookup."""
    global_root = resolve_global_published_root()
    branch = Path(os.environ.get(ENV_PUBLISHED_ROOT, str(global_root)))
    return branch, global_root
