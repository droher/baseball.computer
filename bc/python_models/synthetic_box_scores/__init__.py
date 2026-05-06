"""Pure-Polars helpers for the synthetic_box_score.* models.

Filling in lineup skeletons for the ~25K games that exist only in
`misc.gamelog`. Stat columns stay NULL elsewhere; this package only
produces the modal seasonal lineup that feeds those skeletons.
"""

from python_models.synthetic_box_scores.modal_lineups import (
    APPEARANCES_INPUT_COLUMNS,
    BATTING_INPUT_COLUMNS,
    MODAL_LINEUP_OUTPUT_COLUMNS,
    compute_modal_lineups,
)
from python_models.synthetic_box_scores.game_lineups import (
    build_synthetic_batting_core,
    build_synthetic_fielding_core,
)

__all__ = [
    "APPEARANCES_INPUT_COLUMNS",
    "BATTING_INPUT_COLUMNS",
    "MODAL_LINEUP_OUTPUT_COLUMNS",
    "build_synthetic_batting_core",
    "build_synthetic_fielding_core",
    "compute_modal_lineups",
]
