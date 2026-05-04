"""SQLMesh configuration for baseball.computer.

State lives in a separate DuckDB file (bc_state.db) so the DuckLake
publish layer can manage the data file independently.

When ``BC_PERF_MODE=1`` is set, the gateway is reconfigured for the
instrumentation harness in ``scripts/perf_run.py``: pool size drops to
1 (so per-snapshot pragma SETs stick to the same connection) and
DuckDB JSON profiling is enabled so the harness can copy the per-query
plan tree out after each evaluation.
"""

from __future__ import annotations

import os
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
_PERF_MODE = os.environ.get("BC_PERF_MODE") == "1"

_DUCKDB_THREADS = int(os.environ.get("BC_DUCKDB_THREADS", "14"))

_DUCKDB_SETTINGS: dict[str, object] = {
    "enable_fsst_vectors": True,
    "enable_http_metadata_cache": True,
    "preserve_insertion_order": False,
    "parquet_metadata_cache": True,
    "checkpoint_threshold": "1GB",
    "memory_limit": "48GB",
    "threads": _DUCKDB_THREADS,
}

if _PERF_MODE:
    _DUCKDB_SETTINGS["enable_profiling"] = "json"
    _DUCKDB_SETTINGS["profiling_mode"] = "detailed"
    _DUCKDB_SETTINGS["profiling_coverage"] = "ALL"
    # Initial path; perf_run.py overrides per-snapshot via SET on the
    # adapter so each evaluation's plan tree lands in its own file.
    _DUCKDB_SETTINGS["profile_output"] = str(
        PROJECT_ROOT.parent / "logs" / "perf" / "_last_query.json"
    )

_DEFAULT_POOL_SIZE = 1 if _PERF_MODE else 6
_POOL_SIZE = int(os.environ.get("BC_CONCURRENT_TASKS", str(_DEFAULT_POOL_SIZE)))

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
    concurrent_tasks=_POOL_SIZE,
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
    # Build-only DB; FULL kind replaces every table on each plan, so
    # old versioned snapshot tables are pure dead weight. Tight TTL +
    # a janitor sweep after each plan keeps bc.db from accreting GBs
    # of orphaned `sqlmesh__main_models.*__<hash>__dev` tables.
    snapshot_ttl="in 1 hour",
    environment_ttl="in 1 hour",
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
