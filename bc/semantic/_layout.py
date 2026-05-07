"""Pure-data layout for the BSL semantic tables.

Lives outside ``tables.py`` so the LSF context generator (which runs
under the SQLMesh uv group) can describe the BSL tables without
importing ``boring_semantic_layer``. The lambdas that turn these names
into BSL dimensions stay in ``tables.py``.
"""

from __future__ import annotations

OFFENSE_PITCHING_SEASON_DIM_NAMES: list[str] = [
    "player_id",
    "team_id",
    "season",
    "league",
    "game_type",
]

FIELDING_SEASON_DIM_NAMES: list[str] = [
    *OFFENSE_PITCHING_SEASON_DIM_NAMES,
    "fielding_position",
]

EVENT_DIM_NAMES: list[str] = [
    "player_id",
    "team_id",
    "game_id",
    "season",
    "league",
    "park_id",
    "game_type",
    "is_regular_season",
]


def dim_names(kind: str, grain: str) -> list[str]:
    """Return the dimension-name list for a (kind, grain) BSL semantic table."""
    if grain == "event":
        return list(EVENT_DIM_NAMES)
    if grain == "season":
        if kind == "fielding":
            return list(FIELDING_SEASON_DIM_NAMES)
        return list(OFFENSE_PITCHING_SEASON_DIM_NAMES)
    raise ValueError(f"unknown (kind={kind!r}, grain={grain!r})")
