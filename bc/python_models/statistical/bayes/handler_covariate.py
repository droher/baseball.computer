"""Handler-posterior covariate derived from Model D's published export.

Model D (``ball_handler_imputation``) publishes a per-event softmax over the
nine fielding positions (``ball_handler_probabilities.parquet``: columns
``event_key``, ``fielding_position`` 1..9, ``expected_share``). The smallest
defensible summary of that posterior for batted-ball geometry is the
probability that the handling fielder is an outfielder (positions 7, 8, 9):
trajectory and location-depth load most directly on the in/out-field split.

This module resolves Model D's published export and reduces each event's
nine-way share vector to ``logit(P(handler is OF))``. Standardization and the
frozen training-stat discipline live in ``_geometry_data`` alongside the DL
and propensity covariates.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false

from __future__ import annotations

import logging
import os
from pathlib import Path

import numpy as np
import numpy.typing as npt
import polars as pl

from python_models.statistical.manifests import (
    find_published_manifest,
    read_published_pointer,
)

FloatArray = npt.NDArray[np.float64]

_log = logging.getLogger(__name__)

HANDLER_ENV_FLAG: str = "BC_GEOMETRY_HANDLER"
HANDLER_ARTIFACT_ENV: str = "BC_GEOMETRY_HANDLER_ARTIFACT"

HANDLER_MODEL_NAME: str = "ball_handler_imputation"
HANDLER_EXPORT_FILENAME: str = "ball_handler_probabilities.parquet"

OUTFIELD_POSITIONS: tuple[int, ...] = (7, 8, 9)
HANDLER_PROB_CLIP: float = 1e-6


def handler_covariate_enabled() -> bool:
    return os.environ.get(HANDLER_ENV_FLAG, "").strip().lower() in {"1", "true", "yes"}


def _resolve_handler_export_path() -> Path | None:
    override = os.environ.get(HANDLER_ARTIFACT_ENV, "").strip()
    if override:
        artifact_dir = Path(override)
        candidate = artifact_dir / "exports" / HANDLER_EXPORT_FILENAME
        if candidate.is_file():
            return candidate
        direct = artifact_dir / HANDLER_EXPORT_FILENAME
        if direct.is_file():
            return direct
        _log.warning(
            "%s=%s but no %s under it; falling back to the published pointer",
            HANDLER_ARTIFACT_ENV,
            override,
            HANDLER_EXPORT_FILENAME,
        )
    pointer_path = find_published_manifest(HANDLER_MODEL_NAME)
    if pointer_path is None:
        _log.warning(
            "no published pointer for %s; handler covariate cannot be sourced",
            HANDLER_MODEL_NAME,
        )
        return None
    pointer = read_published_pointer(pointer_path)
    export_path = pointer.manifest_path.parent / "exports" / HANDLER_EXPORT_FILENAME
    if not export_path.is_file():
        _log.warning(
            "published %s artifact %s lacks %s at %s; handler covariate unavailable",
            HANDLER_MODEL_NAME,
            pointer.artifact_id,
            HANDLER_EXPORT_FILENAME,
            export_path,
        )
        return None
    return export_path


def load_handler_outfield_logit() -> dict[int, float] | None:
    """Per-event ``logit(P(handler is OF))`` from Model D's published export.

    Returns a mapping ``event_key -> logit`` over the events Model D scored,
    or ``None`` when no export can be resolved. P(OF) is the sum of the
    expected shares on positions 7/8/9, clipped before the logit.
    """
    export_path = _resolve_handler_export_path()
    if export_path is None:
        return None
    frame = (
        pl.scan_parquet(export_path)
        .filter(pl.col("fielding_position").is_in(list(OUTFIELD_POSITIONS)))
        .group_by("event_key")
        .agg(pl.col("expected_share").sum().alias("p_of"))
        .collect()
    )
    if frame.height == 0:
        _log.warning("handler export %s yielded zero outfield rows", export_path)
        return None
    event_keys = frame.get_column("event_key").cast(pl.Int64).to_numpy()
    p_of = frame.get_column("p_of").cast(pl.Float64).to_numpy()
    clipped = np.clip(p_of, HANDLER_PROB_CLIP, 1.0 - HANDLER_PROB_CLIP)
    logit = np.log(clipped / (1.0 - clipped))
    _log.info(
        "loaded handler covariate from %s: %d events with P(OF) logit",
        export_path,
        frame.height,
    )
    return {int(k): float(v) for k, v in zip(event_keys, logit)}
