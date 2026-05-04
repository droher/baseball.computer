"""Pure-Polars FSM transforms over event-locality data.

DuckDB window translations used in this package:

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
