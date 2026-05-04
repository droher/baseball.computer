"""BSL semantic-table factories backed by the Pydantic Metric registry.

This package is import-isolated from sqlmesh. It runs under the
``spikes-bsl`` uv group (which excludes sqlmesh because boring-semantic-
layer pulls an incompatible sqlglot pin via xorq).

Entry points:
- ``connect(env, db_path)`` — open ``bc.db`` read-only, return an Ibis
  DuckDB connection.
- ``offense_seasons / offense_events / pitching_seasons / pitching_events
  / fielding_seasons / fielding_events`` — six BSL ``SemanticTable``
  builders, one per (kind × grain).
"""

from __future__ import annotations

from .tables import (
    connect,
    fielding_events,
    fielding_seasons,
    offense_events,
    offense_seasons,
    pitching_events,
    pitching_seasons,
)

__all__ = [
    "connect",
    "fielding_events",
    "fielding_seasons",
    "offense_events",
    "offense_seasons",
    "pitching_events",
    "pitching_seasons",
]
