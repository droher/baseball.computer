"""SQLMesh configuration for baseball.computer (Phase 1 dbt-import path).

Replaces `sqlmesh.yaml` so we can wire `loader=PatchedDbtLoader`. The
yaml schema's `loader: t.Type[Loader]` field can't accept a string —
pydantic won't auto-import a class — so the config has to be Python.

State lives in a separate DuckDB file (`bc_state.db`) so it never
collides with `bc.db`. Matches the Phase 4 DuckLake requirement and
sidesteps the `disable_transactions: true` interaction with the default
state schema.

Uses `sqlmesh_config()` helper from `sqlmesh.dbt.loader` so dialect /
gateway / target wiring is auto-derived from the dbt profile.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

from sqlmesh.core.config import (
    DuckDBConnectionConfig,
    ModelDefaultsConfig,
    PlanConfig,
)
from sqlmesh.core.config.common import VirtualEnvironmentMode
from sqlmesh.dbt.loader import sqlmesh_config

# `bc/` is added to sys.path by SQLMesh's config loader. Import `loader`
# as a top-level module rather than `bc.loader`.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from loader import PatchedDbtLoader  # type: ignore[import-not-found]  # noqa: E402


config = sqlmesh_config(
    project_root=Path(__file__).resolve().parent,
    state_connection=DuckDBConnectionConfig(database="bc_state.db"),
    loader=PatchedDbtLoader,
    model_defaults=ModelDefaultsConfig(start=date(2026, 5, 1)),
    virtual_environment_mode=VirtualEnvironmentMode.DEV_ONLY,
    plan=PlanConfig(always_recreate_environment=True),
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
)
