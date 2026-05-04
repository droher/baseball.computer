"""Pure-Polars FSM transforms for game-level models.

Mirrors the lag-mapping conventions documented in
`python_models/event_locality/__init__.py`.

    LAG(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY o)
        = x.forward_fill().over(g, order_by=o).shift(1).over(g, order_by=o)

    COALESCE(LAG(x IGNORE NULLS) OVER (...), x)
        = x.forward_fill().over(g, order_by=o)   # inclusive of current row
"""

from python_models.game_level.team_game_results import (
    TEAM_GAME_RESULTS_INPUT_COLUMNS,
    TEAM_GAME_RESULTS_OUTPUT_COLUMNS,
    compute_team_game_results,
)

__all__ = [
    "TEAM_GAME_RESULTS_INPUT_COLUMNS",
    "TEAM_GAME_RESULTS_OUTPUT_COLUMNS",
    "compute_team_game_results",
]
