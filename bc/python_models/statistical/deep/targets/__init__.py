"""Per-target deep proposal definitions.

Importing the package registers every shipped target with
``python_models.statistical.deep.registry`` and ``feature_layout``
side-effects.
"""

from __future__ import annotations

from python_models.statistical.deep.targets import (
    advancement as _advancement,  # noqa: F401
)
from python_models.statistical.deep.targets import geometry as _geometry  # noqa: F401
