"""Phase 6 ML pipeline.

Imports as `python_models.ml` (matches the `python_models/event_locality`
precedent). The `sf-hamilton` PyPI package imports as `hamilton`; it is
referenced from this package's training/prediction entry points but
lives at the top level, so there is no name clash.

Sets `KERAS_BACKEND=torch` before any keras import. Apple Silicon MPS
support ships in torch core; no `tensorflow-metal` / `jax-metal`
plugin. This module is imported transitively by every training and
scoring entry point, so the env var is set in time.
"""

from __future__ import annotations

import os
from pathlib import Path

_ = os.environ.setdefault("KERAS_BACKEND", "torch")

_ARTIFACT_DIR = Path(__file__).resolve().parent / "artifacts"


def artifact_exists(target: str) -> bool:
    """True when the trained scorer pin JSON for ``target`` is on disk.

    Used by the ``predictions_<target>.py`` SQLMesh wrappers to gate
    their ``@model(enabled=...)`` flag — a target without a pin can't
    score, so its model stays out of the build.
    """
    return (_ARTIFACT_DIR / f"{target}.json").exists()
