"""SQLMesh configuration for baseball.computer (Phase 1.5 native path).

Phase 1.5 drops the dbt-import path. Loader is the stock `SqlMeshLoader`,
so `bc/macros/_init_db.py` and other Python `@macro` defs register
naturally. State lives in a separate DuckDB file (`bc_state.db`) to mirror
the Phase 4 DuckLake requirement.

Connection settings mirror the dbt profile at `~/.dbt/profiles.yml` (bc/dev):
DuckDB at `bc.db`, httpfs + parquet extensions, the same per-session
settings, and `disable_transactions` via connector_config.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import sys

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
    PlanConfig,
)
from sqlmesh.core.config.common import VirtualEnvironmentMode
from sqlmesh.core.model.kind import FullKind

# `bc/` is added to sys.path by SQLMesh's config loader. Import `loader` and
# `jinja_globals` as top-level modules so `BcSqlMeshLoader` can reference
# `jinja_globals` via importlib at runtime.
sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent))
from loader import BcSqlMeshLoader  # type: ignore[import-not-found]  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent

_DUCKDB_SETTINGS = {
    "enable_fsst_vectors": True,
    "enable_http_metadata_cache": True,
    "preserve_insertion_order": False,
    "parquet_metadata_cache": True,
    "checkpoint_threshold": "1GB",
}

_DUCKDB_CONNECTION = DuckDBConnectionConfig(
    database=str(PROJECT_ROOT.parent / "bc.db"),
    extensions=["httpfs", "parquet"],
    connector_config=_DUCKDB_SETTINGS,
    concurrent_tasks=6,
)

_STATE_CONNECTION = DuckDBConnectionConfig(
    database=str(PROJECT_ROOT / "bc_state.db"),
)

config = Config(
    loader=BcSqlMeshLoader,
    default_gateway="bc",
    gateways={
        "bc": GatewayConfig(
            connection=_DUCKDB_CONNECTION,
            state_connection=_STATE_CONNECTION,
        ),
    },
    model_defaults=ModelDefaultsConfig(
        dialect="duckdb",
        start=date(2026, 5, 1),
        kind=FullKind(),
    ),
    virtual_environment_mode=VirtualEnvironmentMode.DEV_ONLY,
    plan=PlanConfig(always_recreate_environment=True),
    before_all=[
        "@init_db()",
        "@create_enums()",
        "@alter_types()",
        "@load_seeds()",
    ],
    variables={
        "source_roots": {
            "event": "https://data.baseball.computer/event",
            "game": "https://data.baseball.computer/event",
            "box_score": "https://data.baseball.computer/event",
            "misc": "https://data.baseball.computer/misc",
            "baseballdatabank": "https://data.baseball.computer/baseballdatabank",
            "biodata": "https://data.baseball.computer/biodata",
        },
        "force_reload": False,
    },
    # YAMLs are dbt-format model metadata; under SqlMeshLoader they are not
    # consumed directly (Step 5 migrates them into MODEL() blocks + audits).
    ignore_patterns=[
        "models/**/*.yml",
        "seeds/**/*.yml",
    ],
)
