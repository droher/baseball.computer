"""Column-set + description helpers for the metrics_* models.

Each metrics_* table has the same shape for a given (kind):
    grain cols + counting stats (INTEGER) + season ratio metrics (DOUBLE)
    + event_coverage_rate (DOUBLE) + event ratio metrics (DOUBLE).

Only the grain columns differ across the three scopes
(player_career, player_season_league, team_season).
"""

from __future__ import annotations

from python_models._doc_lookup import doc_dict
from python_models.metrics._constants import INT_COLS

_GRAIN_TYPES: dict[str, str] = {
    "player_id": "VARCHAR",
    "team_id": "VARCHAR",
    "season": "SMALLINT",
    "league": "VARCHAR",
}


def metric_columns(kind: str, grain: list[str]) -> dict[str, str]:
    """Output schema for one metrics_* table: grain → ints → ratios."""
    from python_models.metrics import _metric_registrations  # noqa: F401
    from python_models.metrics.registry import metrics_for

    cols: dict[str, str] = {g: _GRAIN_TYPES[g] for g in grain}
    for c in INT_COLS[kind]:
        cols[c] = "INTEGER"
    for m in metrics_for(kind, "season"):  # type: ignore[arg-type]
        cols[m.name] = m.dtype
    cols["event_coverage_rate"] = "DOUBLE"
    for m in metrics_for(kind, "event"):  # type: ignore[arg-type]
        cols[m.name] = m.dtype
    return cols


def metric_column_descriptions(kind: str, grain: list[str]) -> dict[str, str]:
    docs = doc_dict()
    return {col: docs[col] for col in metric_columns(kind, grain) if col in docs}
