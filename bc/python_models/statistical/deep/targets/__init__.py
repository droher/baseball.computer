"""Per-target deep proposal definitions.

Importing the package registers every shipped target with
``python_models.statistical.deep.registry`` and ``feature_layout``
side-effects. PR3 ships Geometry; PR4 (pitch_summary, advancement),
PR5 (fielding_credit), PR6 (embeddings) add their own modules.
"""

from __future__ import annotations

from python_models.statistical.deep.targets import geometry as _geometry  # noqa: F401
from python_models.statistical.deep.targets import pitch_summary as _pitch_summary  # noqa: F401
