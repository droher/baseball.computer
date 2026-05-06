from __future__ import annotations

import importlib.util
import os
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config.py"
REPO_ROOT = CONFIG_PATH.parent.parent


@contextmanager
def _temporary_env(values: dict[str, str | None]) -> Iterator[None]:
    previous = {key: os.environ.get(key) for key in values}
    try:
        for key, value in values.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _load_config_module() -> object:
    module_name = f"bc_config_test_{uuid.uuid4().hex}"
    spec = importlib.util.spec_from_file_location(module_name, CONFIG_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Could not load spec for {CONFIG_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_config_uses_repo_default_database_paths() -> None:
    with _temporary_env({"BC_DB_PATH": None, "BC_STATE_DB_PATH": None}):
        module = _load_config_module()

    gateway = module.config.gateways["bc"]
    assert gateway.connection.catalogs["bc"] == str(REPO_ROOT / "bc.db")
    assert gateway.state_connection.database == str(REPO_ROOT / "bc" / "bc_state.db")


def test_config_allows_database_path_overrides() -> None:
    warehouse_path = "/private/tmp/sqlmesh-test-warehouse.db"
    state_path = "/private/tmp/sqlmesh-test-state.db"

    with _temporary_env({"BC_DB_PATH": warehouse_path, "BC_STATE_DB_PATH": state_path}):
        module = _load_config_module()

    gateway = module.config.gateways["bc"]
    assert gateway.connection.catalogs["bc"] == warehouse_path
    assert gateway.state_connection.database == state_path
