"""SQLMesh configuration for baseball.computer.

State lives in a separate DuckDB file (bc_state.db) so the Phase 4
DuckLake publish layer can manage the data file independently.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    LinterConfig,
    ModelDefaultsConfig,
    PlanConfig,
)
from sqlmesh.core.config.common import VirtualEnvironmentMode
from sqlmesh.core.config.connection import DuckDBAttachOptions
from sqlmesh.core.model.kind import FullKind

PROJECT_ROOT = Path(__file__).resolve().parent

_DUCKDB_SETTINGS = {
    "enable_fsst_vectors": True,
    "enable_http_metadata_cache": True,
    "preserve_insertion_order": False,
    "parquet_metadata_cache": True,
    "checkpoint_threshold": "1GB",
    "memory_limit": "48GB",
    "threads": 7,
}

_DUCKDB_CONNECTION = DuckDBConnectionConfig(
    extensions=["httpfs", "parquet", "ducklake"],
    catalogs={
        # First entry is the default catalog. bc.db stays the build target
        # for state and intermediate model materializations; only the
        # publish layer uses the bc_publish DuckLake catalog (populated by
        # scripts/publish_ducklake.py post-build).
        "bc": str(PROJECT_ROOT.parent / "bc.db"),
        # Relative data_path so the catalog can be uploaded to R2 and
        # consumers attach by URL — DuckLake resolves relative paths
        # against the catalog's parent URL at attach time. SQLMesh always
        # runs with cwd=bc/, so the local resolution is bc/bc_publish_data/.
        "bc_publish": DuckDBAttachOptions(
            type="ducklake",
            path=str(PROJECT_ROOT / "bc_publish.ducklake"),
            data_path="bc_publish_data/",
            data_inlining_row_limit=0,
        ),
    },
    connector_config=_DUCKDB_SETTINGS,
    concurrent_tasks=2,
)

_STATE_CONNECTION = DuckDBConnectionConfig(
    database=str(PROJECT_ROOT / "bc_state.db"),
)

config = Config(
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
    ignore_patterns=[
        "models/**/*.yml",
        "seeds/**/*.yml",
    ],
    linter=LinterConfig(
        enabled=True,
        warn_rules={"nomissingaudits"},
    ),
)
