"""Helpers for the synthetic_box_score.* models.

Filling in lineup skeletons for the ~25K games that exist only in
`misc.gamelog`. Stat columns stay NULL elsewhere; this package builds
the default seasonal lineups and the game-level optimizer assignments.
"""

from python_models.synthetic_box_scores.modal_lineups import (
    APPEARANCES_INPUT_COLUMNS,
    BATTING_INPUT_COLUMNS,
    MODAL_LINEUP_OUTPUT_COLUMNS,
    compute_modal_lineups,
)
from python_models.synthetic_box_scores.game_lineups import (
    ASSIGNMENT_INPUT_COLUMNS,
    REPORT_OUTPUT_COLUMNS,
    build_synthetic_batting_core,
    build_synthetic_fielding_core,
    build_synthetic_lineup_assignments,
    build_synthetic_lineup_report,
    build_synthetic_lineup_report_from_assignments,
)

__all__ = [
    "ASSIGNMENT_INPUT_COLUMNS",
    "APPEARANCES_INPUT_COLUMNS",
    "BATTING_INPUT_COLUMNS",
    "MODAL_LINEUP_OUTPUT_COLUMNS",
    "REPORT_OUTPUT_COLUMNS",
    "build_synthetic_batting_core",
    "build_synthetic_fielding_core",
    "build_synthetic_lineup_assignments",
    "build_synthetic_lineup_report",
    "build_synthetic_lineup_report_from_assignments",
    "compute_modal_lineups",
]
