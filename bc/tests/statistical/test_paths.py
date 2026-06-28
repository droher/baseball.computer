"""Verify path/env resolution in python_models.statistical.config."""

from __future__ import annotations

import os
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

from python_models.statistical import config


@contextmanager
def _temporary_env(values: dict[str, str | None]) -> Generator[None, None, None]:
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                _ = os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                _ = os.environ.pop(key, None)
            else:
                os.environ[key] = value


def test_project_root_points_at_repo() -> None:
    assert (config.PROJECT_ROOT / "pyproject.toml").exists()


def test_artifact_root_under_project() -> None:
    assert config.ARTIFACT_ROOT == config.PROJECT_ROOT / "artifacts" / "statistical"


def test_resolve_db_path_defaults_to_dev() -> None:
    with _temporary_env({config.ENV_DB_PATH: None}):
        resolved = config.resolve_db_path()
    assert resolved == config.DEFAULT_DEV_DB_PATH


def test_resolve_db_path_honors_env(tmp_path: Path) -> None:
    target = tmp_path / "elsewhere.db"
    with _temporary_env({config.ENV_DB_PATH: str(target)}):
        resolved = config.resolve_db_path()
    assert resolved == target


def test_resolve_published_roots_defaults_to_global() -> None:
    with _temporary_env({config.ENV_PUBLISHED_ROOT: None}):
        branch, global_ = config.resolve_published_roots()
    assert branch == config.GLOBAL_PUBLISHED_ROOT
    assert global_ == config.GLOBAL_PUBLISHED_ROOT


def test_resolve_published_roots_branch_override(tmp_path: Path) -> None:
    branch_root = tmp_path / "published-branch"
    with _temporary_env({config.ENV_PUBLISHED_ROOT: str(branch_root)}):
        branch, global_ = config.resolve_published_roots()
    assert branch == branch_root
    assert global_ == config.GLOBAL_PUBLISHED_ROOT
