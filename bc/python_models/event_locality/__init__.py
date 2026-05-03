"""Phase 5 — pure-Polars FSM transforms over event-locality data.

The SQL counterparts (`event_pitching_flags`, future `team_game_results`,
`team_game_start_info`) lean on `LAG(... IGNORE NULLS) OVER (...)` to
forward-propagate state through windowed partitions. The mapping in Polars:

    LAG(x IGNORE NULLS) OVER (PARTITION BY g ORDER BY o)
        = x.forward_fill().over(g, order_by=o).shift(1).over(g, order_by=o)

    LAG(x) OVER (PARTITION BY g ORDER BY o)
        = x.shift(1).over(g, order_by=o)

    LEAD(x) OVER (PARTITION BY g ORDER BY o)
        = x.shift(-1).over(g, order_by=o)
"""

from python_models.event_locality.pitching_flags import (
    PITCHING_FLAGS_INPUT_COLUMNS,
    PITCHING_FLAGS_OUTPUT_COLUMNS,
    compute_pitching_flags,
)

__all__ = [
    "PITCHING_FLAGS_INPUT_COLUMNS",
    "PITCHING_FLAGS_OUTPUT_COLUMNS",
    "compute_pitching_flags",
]
