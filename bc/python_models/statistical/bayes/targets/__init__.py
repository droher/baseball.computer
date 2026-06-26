"""Bayes-target registrations. Importing this package registers all shipped targets."""

from __future__ import annotations

from python_models.statistical.bayes.targets import (
    advancement as _advancement,  # noqa: F401
    ball_handler as _ball_handler,  # noqa: F401
    credit as _credit,  # noqa: F401
    geometry as _geometry,  # noqa: F401
    observation as _observation,  # noqa: F401
    park_factor as _park_factor,  # noqa: F401
    pitch_coverage as _pitch_coverage,  # noqa: F401
    pitch_summary as _pitch_summary,  # noqa: F401
    run_values as _run_values,  # noqa: F401
    state_transition as _state_transition,  # noqa: F401
)
