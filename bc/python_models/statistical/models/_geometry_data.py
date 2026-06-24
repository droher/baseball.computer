"""Event-grain prep for the batted-ball geometry imputation model (Model E).

Reads the frozen ``model_input_geometry`` Parquet at grain
``(event_key, geometry_dimension)`` and shapes a single-arm per-dimension
categorical softmax over the directly recorded geometry class. Unlike the
ball-handler model's fixed K=9 position set, the class set is
dimension-specific: the four DL-backed dimensions (trajectory,
location_side, location_depth, location_edge) carry a locked ordered
vocabulary matching the DL softmax output order, while general_location
derives its vocabulary from the observed-data class frequencies.

Filters to the geometry-observed rows (``geometry_dimension=<dim>,
observed_status='observed', training_weight > 0``), applies the
per-dimension raw→vocab remap, and maps each event's remapped label to a
class index. Truth is the directly recorded class in ``raw_value``, one
class per event.

The output ``GeometryInputs`` reuses the ``FixedEffectDesign`` /
``HeldOutSet`` / ``ProductionScoringFrame`` types from ``_credit_data`` so
downstream reconstruction / held-out helpers accept it. A deterministic
per-game held-out split (``HOLDOUT_FOLD_COUNT=10`` via ``game_hash_fold``,
fold 0) is removed before training; the held-out events ship alongside
training inputs for OOS scoring. The production scoring slice is the
geometry-unobserved rows (``observed_status != 'observed'``).
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical import config as cfg
from python_models.statistical.bayes.dl_covariate import compute_dl_log_probs_per_class
from python_models.statistical.bayes.handler_covariate import (
    handler_covariate_enabled,
    load_handler_outfield_logit,
)
from python_models.statistical.deep.targets.geometry import (
    GEOMETRY_SPECS as DEEP_GEOMETRY_SPECS,
)
from python_models.statistical.manifests import (
    find_published_manifest,
    read_published_pointer,
)
from python_models.statistical.models._credit_data import (
    FixedEffectDesign,
    HeldOutSet,
    ProductionScoringFrame,
)
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]

DEFAULT_SEED: int = 20260513
MIN_EVENTS_PER_SEASON: int = 50

UNKNOWN_LEVEL: str = "__unknown__"

HOLDOUT_FOLD_COUNT: int = 10
HOLDOUT_FOLD_ID: int = 0

OBSERVED_STATUS: str = "observed"

DIMENSION_COLUMN: str = "geometry_dimension"
LABEL_COLUMN: str = "raw_value"

PROPENSITY_COLUMN: str = "propensity_p_observed"
PROPENSITY_CLIP: float = 1e-6

FIXED_EFFECT_COLUMNS: tuple[str, ...] = (
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)

PRODUCTION_FE_COVERAGE_FLOOR: float = 0.01

_BUNT_VARIANTS: tuple[str, ...] = (
    "FoulBunt",
    "GroundBallBunt",
    "LineDriveBunt",
    "PopUpBunt",
    "UnspecifiedBunt",
)


class GeometryDimensionSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    dimension: str
    class_labels: tuple[str, ...] | None
    remap: dict[str, str]
    dl_active: bool


GEOMETRY_DIMENSIONS: dict[str, GeometryDimensionSpec] = {
    "trajectory": GeometryDimensionSpec(
        dimension="trajectory",
        class_labels=("Fly", "GroundBall", "LineDrive", "PopUp", "Bunt"),
        remap={variant: "Bunt" for variant in _BUNT_VARIANTS},
        dl_active=True,
    ),
    "location_side": GeometryDimensionSpec(
        dimension="location_side",
        class_labels=("Default", "Foul", "FoulLine", "Left", "Middle", "Right"),
        remap={},
        dl_active=True,
    ),
    "location_depth": GeometryDimensionSpec(
        dimension="location_depth",
        class_labels=("Deep", "Default", "ExtraDeep", "Shallow"),
        remap={},
        dl_active=True,
    ),
    "location_edge": GeometryDimensionSpec(
        dimension="location_edge",
        class_labels=("All", "Left", "Middle", "Right"),
        remap={},
        dl_active=True,
    ),
    "general_location": GeometryDimensionSpec(
        dimension="general_location",
        class_labels=None,
        remap={},
        dl_active=False,
    ),
}


class GeometryProductionFrame(ProductionScoringFrame):
    """Production scoring frame plus a per-class DL logit covariate.

    Extends ``ProductionScoringFrame`` with ``dl_logit_per_class`` of shape
    ``(n_event, n_classes)`` so the builder can apply γ_dl on the
    geometry-unobserved production slice. All-zeros for non-DL dimensions
    (``dl_p_class`` is NULL there).

    ``handler_z`` carries the standardized handler-posterior covariate
    (frozen training stats; NULL→0) so the export can apply γ_handler on
    the production slice.
    """

    dl_logit_per_class: FloatArray
    handler_z: FloatArray | None = None


class GeometryInputs(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    event_keys: IntArray
    dimension: str
    class_labels: list[str]
    n_classes: int
    dl_active: bool

    counts: IntArray
    dl_logit_per_class: FloatArray
    dl_logit_class_means: FloatArray

    season_league_idx: IntArray
    scorer_idx: IntArray

    fixed_effects: dict[str, FixedEffectDesign]

    coords: dict[str, list[str]]

    held_out: HeldOutSet
    held_out_dl_logit_per_class: FloatArray

    propensity_z: FloatArray | None = None
    held_out_propensity_z: FloatArray | None = None
    propensity_logit_mean: float = 0.0
    propensity_logit_std: float = 1.0
    propensity_active: bool = False

    handler_z: FloatArray | None = None
    held_out_handler_z: FloatArray | None = None
    handler_logit_mean: float = 0.0
    handler_logit_std: float = 1.0
    handler_active: bool = False

    @property
    def n_events(self) -> int:
        return int(self.counts.shape[0])

    @property
    def n_positions(self) -> int:
        return self.n_classes

    @property
    def credit_type(self) -> str:
        return "geometry"


def _assert_contiguous(idx: IntArray, labels: list[str], column: str) -> None:
    expected = set(range(len(labels)))
    actual: set[int] = {int(v) for v in np.unique(idx).tolist()}
    if actual != expected:
        raise AssertionError(
            f"{column} indexer contiguity check failed: expected {expected}, got {actual}"
        )


def _category_index(
    df: pl.DataFrame, column: str, *, fill_value: str = UNKNOWN_LEVEL
) -> tuple[IntArray, list[str]]:
    series = df.get_column(column)
    if series.dtype == pl.Boolean:
        series = series.cast(pl.Utf8)
    series = series.fill_null(fill_value).cast(pl.Utf8)
    raw_values: list[object] = list(series.unique().to_list())
    labels: list[str] = sorted(str(v) for v in raw_values)
    mapping: dict[str, int] = {c: i for i, c in enumerate(labels)}
    codes = (
        series.replace_strict(mapping, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    _assert_contiguous(codes, labels, column)
    return codes, labels


def _build_fixed_effect_design(df: pl.DataFrame, column: str) -> FixedEffectDesign:
    codes, labels = _category_index(df, column)
    if not labels:
        raise ValueError(f"fixed-effect column {column!r} has zero levels")
    return FixedEffectDesign(levels=tuple(labels), codes=codes)


def _encode_codes_with_vocab(
    per_event: pl.DataFrame, column: str, labels: Sequence[str]
) -> IntArray:
    """Map a per-event column to int codes against a fixed ``labels`` vocab.

    Booleans cast to utf8, NULLs fill to ``UNKNOWN_LEVEL``; unseen values
    (and NULLs) fall back to ``UNKNOWN_LEVEL``'s index, or ``-1`` when
    ``UNKNOWN_LEVEL`` is not in the vocab.
    """
    series = per_event.get_column(column)
    if series.dtype == pl.Boolean:
        series = series.cast(pl.Utf8)
    series = series.fill_null(UNKNOWN_LEVEL).cast(pl.Utf8)
    mapping = {c: i for i, c in enumerate(labels)}
    fallback = mapping.get(UNKNOWN_LEVEL, -1)
    return np.fromiter(
        (mapping.get(str(v), fallback) for v in series.to_list()),
        dtype=np.int64,
        count=series.len(),
    )


def _resolve_spec(dimension: str) -> GeometryDimensionSpec:
    spec = GEOMETRY_DIMENSIONS.get(dimension)
    if spec is None:
        raise ValueError(
            f"unknown geometry dimension {dimension!r}; "
            f"expected one of {sorted(GEOMETRY_DIMENSIONS)}"
        )
    return spec


def _with_remapped_label_and_season_league(
    lf: pl.LazyFrame, *, remap: dict[str, str]
) -> pl.LazyFrame:
    label = pl.col(LABEL_COLUMN).cast(pl.Utf8)
    remapped = label.replace(remap) if remap else label
    return lf.with_columns(
        remapped.alias("_label"),
        (
            pl.col("season").cast(pl.Utf8)
            + pl.lit("|")
            + pl.col("league").cast(pl.Utf8).fill_null(UNKNOWN_LEVEL)
        ).alias("season_league"),
    )


def _derive_general_location_vocab(df: pl.DataFrame) -> list[str]:
    freq = (
        df.group_by("_label")
        .agg(pl.len().alias("_n"))
        .sort(["_n", "_label"], descending=[True, False])
    )
    return [str(v) for v in freq.get_column("_label").to_list()]


def _resolve_class_labels(
    df: pl.DataFrame,
    *,
    spec: GeometryDimensionSpec,
    class_labels: list[str] | None,
) -> list[str]:
    if class_labels is not None:
        return list(class_labels)
    if spec.class_labels is not None:
        return list(spec.class_labels)
    return _derive_general_location_vocab(df)


def _frame_dl_artifact_id(df: pl.DataFrame, dimension: str) -> str | None:
    if "dl_artifact_id" not in df.columns:
        return None
    distinct = sorted(
        str(v) for v in df.get_column("dl_artifact_id").drop_nulls().unique().to_list()
    )
    if not distinct:
        return None
    if len(distinct) > 1:
        raise ValueError(
            f"dataset carries {len(distinct)} distinct dl_artifact_id values for "
            f"dimension={dimension!r}: {distinct}; the dl_p_class covariate must "
            f"come from a single DL artifact per dimension — re-freeze the dataset"
        )
    return distinct[0]


def _deep_target_name_for_dimension(dimension: str) -> str | None:
    pointer_name = f"dl_proposal_{dimension}"
    for spec in DEEP_GEOMETRY_SPECS:
        if spec.published_manifest_name() == pointer_name:
            return spec.name
    return None


def _read_dl_class_labels(
    labels_path: Path, *, dimension: str, source_desc: str
) -> list[str]:
    if not labels_path.is_file():
        raise FileNotFoundError(
            f"class_labels.json missing for {source_desc} "
            f"(dimension={dimension!r}): expected {labels_path}. The DL "
            f"artifact's exports are incomplete or were pruned; re-run "
            f"fit-deep for this dimension or re-freeze the dataset against "
            f"an artifact whose exports/class_labels.json exists"
        )
    payload = json.loads(labels_path.read_text(encoding="utf-8"))
    raw_labels = payload.get("labels", payload.get(dimension))
    if not isinstance(raw_labels, list):
        raise ValueError(
            f"class_labels.json at {labels_path} carries no label list for "
            f"dimension={dimension!r}"
        )
    return [str(v) for v in raw_labels]


def _compare_dl_vocab(
    dl_labels: list[str], vocab: list[str], *, source_desc: str
) -> None:
    if dl_labels != vocab:
        raise ValueError(
            f"DL proposal class vocabulary from {source_desc} does not match the "
            f"Bayes class vocabulary (length and order must be identical): "
            f"DL={dl_labels!r} vs Bayes={vocab!r}"
        )
    _log.info(
        "DL class-vocab alignment verified against %s (K=%d)",
        source_desc,
        len(vocab),
    )


def _assert_dl_class_vocab_alignment(
    dimension: str, vocab: list[str], *, dl_artifact_id: str | None
) -> None:
    """Assert the Bayes class vocab matches the DL proposal's labels.

    When the dataset frame carries a ``dl_artifact_id``, resolves
    ``DEEP_ROOT/<deep_target_name>/<dl_artifact_id>/exports/class_labels.json``
    directly — the artifact that actually produced the frozen ``dl_p_class``
    covariate, regardless of where the published pointer has since advanced.
    Only when the frame lacks a usable ``dl_artifact_id`` does it fall back
    to the published ``dl_proposal_<dimension>`` pointer (with a warning). A
    missing pointer logs a warning and skips the check; a
    present-but-mismatched vocabulary raises.
    """
    pointer_name = f"dl_proposal_{dimension}"
    if dl_artifact_id is not None:
        target_name = _deep_target_name_for_dimension(dimension)
        if target_name is not None:
            source_desc = f"deep target {target_name!r} artifact {dl_artifact_id!r}"
            labels_path = (
                cfg.DEEP_ROOT
                / target_name
                / dl_artifact_id
                / "exports"
                / "class_labels.json"
            )
            dl_labels = _read_dl_class_labels(
                labels_path, dimension=dimension, source_desc=source_desc
            )
            _compare_dl_vocab(dl_labels, vocab, source_desc=source_desc)
            return
        _log.warning(
            "no registered deep target publishes %s; falling back to the "
            "published-pointer resolution despite dataset dl_artifact_id=%s",
            pointer_name,
            dl_artifact_id,
        )
    else:
        _log.warning(
            "dataset frame carries no dl_artifact_id for dimension=%s; "
            "falling back to the published %s pointer, which may have advanced "
            "past the artifact that produced the frozen dl_p_class covariate",
            dimension,
            pointer_name,
        )
    pointer_path = find_published_manifest(pointer_name)
    if pointer_path is None:
        _log.warning(
            "no published pointer for %s; skipping DL class-vocab alignment check",
            pointer_name,
        )
        return
    pointer = read_published_pointer(pointer_path)
    source_desc = f"published pointer {pointer_name!r} artifact {pointer.artifact_id!r}"
    labels_path = pointer.manifest_path.parent / "exports" / "class_labels.json"
    dl_labels = _read_dl_class_labels(
        labels_path, dimension=dimension, source_desc=source_desc
    )
    _compare_dl_vocab(dl_labels, vocab, source_desc=source_desc)


def _propensity_logit_with_mask(
    df: pl.DataFrame, *, column: str = PROPENSITY_COLUMN
) -> tuple[FloatArray, npt.NDArray[np.bool_]]:
    raw = df.get_column(column).cast(pl.Float64).to_numpy().astype(np.float64)
    finite = np.isfinite(raw)
    safe = np.where(finite, raw, 0.5)
    clipped = np.clip(safe, PROPENSITY_CLIP, 1.0 - PROPENSITY_CLIP)
    logit = np.log(clipped / (1.0 - clipped))
    return logit.astype(np.float64), finite


def _standardized_propensity_logit(
    df: pl.DataFrame, *, dimension: str
) -> tuple[FloatArray, float, float]:
    """Standardized logit of ``propensity_p_observed`` over the training slice.

    Clips p to ``[PROPENSITY_CLIP, 1 - PROPENSITY_CLIP]``, takes the logit,
    and standardizes with mean/std computed on the non-NULL training rows
    only (std floors to 1.0 when degenerate). NULL rows map to z=0. Raises
    when the column is present but 100% NULL on a non-empty training slice
    — the dataset predates the published observation-propensity artifacts
    and a silent all-zero covariate would be a wrong fit.
    """
    logit, finite = _propensity_logit_with_mask(df)
    n = int(finite.shape[0])
    n_null = int((~finite).sum())
    if n > 0 and n_null == n:
        raise ValueError(
            f"{PROPENSITY_COLUMN} is 100% NULL on the training slice for "
            f"dimension={dimension!r}; the dataset was frozen before "
            f"observation propensities were published — re-freeze the dataset "
            f"against a snapshot whose {PROPENSITY_COLUMN} join is populated"
        )
    _log.info(
        "propensity covariate null rate on training slice dimension=%s: %d/%d (%.4f)",
        dimension,
        n_null,
        n,
        (n_null / n) if n else 0.0,
    )
    if finite.any():
        mean = float(np.mean(logit[finite]))
        std = float(np.std(logit[finite], ddof=0))
        if std < 1e-12:
            std = 1.0
    else:
        mean = 0.0
        std = 1.0
    z = np.where(finite, (logit - mean) / std, 0.0)
    return z.astype(np.float64), mean, std


def _propensity_z_with_frozen_stats(
    df: pl.DataFrame,
    *,
    mean: float,
    std: float,
    dimension: str,
    context: str,
) -> FloatArray:
    """z = (logit(clip(p)) - mean) / std with the frozen training stats; NULL -> 0."""
    logit, finite = _propensity_logit_with_mask(df)
    n = int(finite.shape[0])
    n_null = int((~finite).sum())
    _log.info(
        "propensity covariate null rate on %s slice dimension=%s: %d/%d (%.4f)",
        context,
        dimension,
        n_null,
        n,
        (n_null / n) if n else 0.0,
    )
    z = np.where(finite, (logit - mean) / std, 0.0)
    return z.astype(np.float64)


def _handler_logit_with_mask(
    event_keys: IntArray, logit_by_event: dict[int, float]
) -> tuple[FloatArray, npt.NDArray[np.bool_]]:
    n = int(event_keys.shape[0])
    logit = np.zeros(n, dtype=np.float64)
    finite = np.zeros(n, dtype=np.bool_)
    for i, key in enumerate(event_keys.tolist()):
        value = logit_by_event.get(int(key))
        if value is not None:
            logit[i] = value
            finite[i] = True
    return logit, finite


def _standardized_handler_logit(
    event_keys: IntArray,
    logit_by_event: dict[int, float],
    *,
    dimension: str,
) -> tuple[FloatArray, float, float]:
    """Standardized handler OF-logit over the training slice.

    Joins Model D's per-event ``logit(P(handler is OF))`` by ``event_key``,
    standardizes with mean/std computed on the events present in Model D's
    export only (std floors to 1.0 when degenerate), and maps events absent
    from the export to z=0.
    """
    logit, finite = _handler_logit_with_mask(event_keys, logit_by_event)
    n = int(finite.shape[0])
    n_null = int((~finite).sum())
    _log.info(
        "handler covariate null rate on training slice dimension=%s: %d/%d (%.4f)",
        dimension,
        n_null,
        n,
        (n_null / n) if n else 0.0,
    )
    if finite.any():
        mean = float(np.mean(logit[finite]))
        std = float(np.std(logit[finite], ddof=0))
        if std < 1e-12:
            std = 1.0
    else:
        mean = 0.0
        std = 1.0
    z = np.where(finite, (logit - mean) / std, 0.0)
    return z.astype(np.float64), mean, std


def _handler_z_with_frozen_stats(
    event_keys: IntArray,
    logit_by_event: dict[int, float],
    *,
    mean: float,
    std: float,
    dimension: str,
    context: str,
) -> FloatArray:
    """z = (handler_logit - mean) / std with the frozen training stats; NULL -> 0."""
    logit, finite = _handler_logit_with_mask(event_keys, logit_by_event)
    n = int(finite.shape[0])
    n_null = int((~finite).sum())
    _log.info(
        "handler covariate null rate on %s slice dimension=%s: %d/%d (%.4f)",
        context,
        dimension,
        n_null,
        n,
        (n_null / n) if n else 0.0,
    )
    z = np.where(finite, (logit - mean) / std, 0.0)
    return z.astype(np.float64)


def _one_hot_counts(class_idx: IntArray, n_classes: int) -> IntArray:
    n = int(class_idx.shape[0])
    counts = np.zeros((n, n_classes), dtype=np.int64)
    counts[np.arange(n), class_idx] = 1
    sums = counts.sum(axis=1)
    if not (sums == 1).all():
        bad = int((sums != 1).sum())
        raise AssertionError(f"_one_hot_counts: {bad} rows do not sum to 1")
    return counts


def _assert_fixed_effects_cover_production_slice(
    parquet_path: Path,
    *,
    dimension: str,
    floor: float = PRODUCTION_FE_COVERAGE_FLOOR,
) -> None:
    """Each FE must be populated on more than ``floor`` of the production slice.

    The production target is the geometry-unobserved slice
    (``geometry_dimension=<dim> AND observed_status != 'observed'``) — the
    events Model E actually scores. A fixed effect that is mostly NULL
    there cannot generalize to the inference target.
    """
    production = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col(DIMENSION_COLUMN) == dimension)
            & (pl.col("observed_status") != OBSERVED_STATUS)
        )
        .select(["event_key", *FIXED_EFFECT_COLUMNS])
        .collect()
    )
    n_production = production.get_column("event_key").n_unique()
    if n_production == 0:
        _log.info(
            "FE coverage guard skipped: no geometry-unobserved production events "
            "(dimension=%r)",
            dimension,
        )
        return
    sparse: list[tuple[str, float]] = []
    for column in FIXED_EFFECT_COLUMNS:
        if column not in production.columns:
            sparse.append((column, 0.0))
            continue
        events_with_value = (
            production.filter(pl.col(column).is_not_null())
            .get_column("event_key")
            .n_unique()
        )
        rate = events_with_value / n_production
        if rate < floor:
            sparse.append((column, rate))
        else:
            _log.info(
                "FE coverage on production slice (n=%d, dimension=%s): %s = %.4f",
                n_production,
                dimension,
                column,
                rate,
            )
    if sparse:
        detail = ", ".join(f"{name}={rate:.4f}" for name, rate in sparse)
        raise ValueError(
            f"fixed-effect coverage on the geometry-unobserved production slice "
            f"(n={n_production}, dimension={dimension!r}) below floor {floor:.4f}: {detail}. "
            "Drop the column from FIXED_EFFECT_COLUMNS — features that are not "
            "populated on the inference target cannot generalize."
        )


def build_geometry_production_frame(
    parquet_path: Path,
    *,
    dimension: str,
    fixed_effects: dict[str, FixedEffectDesign],
    n_classes: int,
    dl_logit_class_means: FloatArray,
    propensity_logit_mean: float = 0.0,
    propensity_logit_std: float = 1.0,
    propensity_active: bool = False,
    handler_logit_mean: float = 0.0,
    handler_logit_std: float = 1.0,
    handler_active: bool = False,
) -> GeometryProductionFrame:
    """One row per geometry-unobserved event with FE codes + per-class DL logits.

    The production slice is ``geometry_dimension=<dim> AND observed_status
    != 'observed'``. Every FE is encoded against its training ``levels``
    vocabulary; unseen levels encode to ``-1`` and are dropped by the
    softmax reconstruction's validity mask. The per-class DL logit array
    rides along so the builder can apply γ_dl on the production slice; the
    scalar propensity z rides along (computed with the FROZEN training
    mean/std) so the export can apply γ_propensity.
    """
    schema_names = set(pl.scan_parquet(parquet_path).collect_schema().names())
    select_columns = ["event_key", "dl_p_class", *FIXED_EFFECT_COLUMNS]
    has_propensity_column = PROPENSITY_COLUMN in schema_names
    if has_propensity_column:
        select_columns.append(PROPENSITY_COLUMN)
    per_event = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col(DIMENSION_COLUMN) == dimension)
            & (pl.col("observed_status") != OBSERVED_STATUS)
        )
        .select(select_columns)
        .unique(subset=["event_key"])
        .sort("event_key")
        .collect()
    )
    if per_event.height == 0:
        return GeometryProductionFrame(
            event_keys=np.zeros(0, dtype=np.int64),
            fixed_effects={},
            dl_logit_per_class=np.zeros((0, n_classes), dtype=np.float64),
            propensity_z=np.zeros(0, dtype=np.float64),
            handler_z=np.zeros(0, dtype=np.float64),
        )

    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    scoring_fe: dict[str, FixedEffectDesign] = {
        column: FixedEffectDesign(
            levels=design.levels,
            codes=_encode_codes_with_vocab(per_event, column, list(design.levels)),
        )
        for column, design in fixed_effects.items()
    }
    dl_logit_per_class = compute_dl_log_probs_per_class(per_event, n_classes=n_classes)
    dl_logit_per_class = dl_logit_per_class - dl_logit_class_means[None, :]
    if propensity_active and has_propensity_column:
        propensity_z = _propensity_z_with_frozen_stats(
            per_event,
            mean=propensity_logit_mean,
            std=propensity_logit_std,
            dimension=dimension,
            context="production",
        )
    else:
        propensity_z = np.zeros(per_event.height, dtype=np.float64)
    if handler_active:
        handler_logit_by_event = load_handler_outfield_logit()
        if handler_logit_by_event is not None:
            handler_z = _handler_z_with_frozen_stats(
                event_keys,
                handler_logit_by_event,
                mean=handler_logit_mean,
                std=handler_logit_std,
                dimension=dimension,
                context="production",
            )
        else:
            handler_z = np.zeros(per_event.height, dtype=np.float64)
    else:
        handler_z = np.zeros(per_event.height, dtype=np.float64)
    return GeometryProductionFrame(
        event_keys=event_keys,
        fixed_effects=scoring_fe,
        dl_logit_per_class=dl_logit_per_class,
        propensity_z=propensity_z,
        handler_z=handler_z,
    )


def prepare_geometry_inputs(
    parquet_path: Path,
    *,
    dimension: str,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    min_events_per_season: int = MIN_EVENTS_PER_SEASON,
    held_out_fold_id: int = HOLDOUT_FOLD_ID,
    held_out_fold_count: int = HOLDOUT_FOLD_COUNT,
    class_labels: list[str] | None = None,
) -> GeometryInputs:
    """Read the modeling-dataset parquet and shape per-dimension categorical inputs.

    Filters to the geometry-observed slice (``observed_status='observed'``,
    ``training_weight > 0``), applies the per-dimension raw→vocab remap,
    maps each event's remapped label to a class index over the ordered
    vocabulary, drops season-league cells below ``min_events_per_season``,
    holds out 10% of games for OOS scoring, and emits one-hot per-event
    truth, a per-class DL logit covariate, and FE / season-league / scorer
    designs.

    The class vocabulary is resolved per dimension: an explicit
    ``class_labels`` override wins; otherwise the four DL dimensions use
    their locked ordered constants and general_location derives its vocab
    from observed-data class frequency (descending, alphabetical
    tiebreaker).
    """
    spec = _resolve_spec(dimension)
    _assert_fixed_effects_cover_production_slice(parquet_path, dimension=dimension)

    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col(DIMENSION_COLUMN) == dimension)
            & (pl.col("observed_status") == OBSERVED_STATUS)
            & (pl.col("training_weight") > 0.0)
        )
        .pipe(_with_remapped_label_and_season_league, remap=spec.remap)
        .filter(pl.col("_label").is_not_null())
        .collect()
    )
    if df.height == 0:
        raise ValueError(
            f"no geometry-observed rows for dimension={dimension!r} in {parquet_path}"
        )

    vocab = _resolve_class_labels(df, spec=spec, class_labels=class_labels)
    if not vocab:
        raise ValueError(
            f"empty class vocabulary for dimension={dimension!r}; nothing to fit"
        )
    if spec.dl_active:
        _assert_dl_class_vocab_alignment(
            dimension, vocab, dl_artifact_id=_frame_dl_artifact_id(df, dimension)
        )
    vocab_set = set(vocab)

    in_vocab = df.get_column("_label").is_in(vocab)
    dropped_labels = int((~in_vocab).sum())
    if dropped_labels:
        offending = df.filter(~in_vocab).get_column("_label").unique().sort().to_list()
        _log.warning(
            "prepare_geometry_inputs dropped %d observed rows whose remapped label "
            "is not in the dimension=%s vocabulary: %s",
            dropped_labels,
            dimension,
            offending,
        )
        df = df.filter(in_vocab)
        if df.height == 0:
            raise ValueError(
                f"every observed row fell outside the vocabulary for "
                f"dimension={dimension!r}; nothing to fit"
            )

    class_to_idx = {label: i for i, label in enumerate(vocab)}
    df = df.with_columns(
        pl.col("_label")
        .replace_strict(class_to_idx, return_dtype=pl.Int64)
        .alias("_class_idx")
    )

    cell_counts = df.group_by("season_league").agg(pl.len().alias("_n"))
    informative_cells = cell_counts.filter(
        pl.col("_n") >= min_events_per_season
    ).get_column("season_league")
    dropped = cell_counts.height - informative_cells.len()
    if dropped:
        dropped_cells = (
            cell_counts.filter(pl.col("_n") < min_events_per_season)
            .sort("season_league")
            .get_column("season_league")
            .to_list()
        )
        _log.info(
            "prepare_geometry_inputs dropped %d season-league cells with <%d events: %s",
            dropped,
            min_events_per_season,
            dropped_cells,
        )
        df = df.filter(pl.col("season_league").is_in(informative_cells.implode()))
        if df.height == 0:
            raise ValueError(
                f"every season-league cell fell below min_events_per_season="
                f"{min_events_per_season} for dimension={dimension!r}; nothing to fit"
            )
        if vocab_set != set(df.get_column("_label").unique().to_list()):
            _log.info(
                "prepare_geometry_inputs season-league floor pruned the observed "
                "class support below the full vocabulary for dimension=%s",
                dimension,
            )

    distinct_game_ids = df.get_column("game_id").unique().to_list()
    holdout_game_ids = [
        g
        for g in distinct_game_ids
        if game_hash_fold(g, fold_count=held_out_fold_count) == held_out_fold_id
    ]
    held_out_df = df.filter(pl.col("game_id").is_in(holdout_game_ids))
    df_train = df.filter(~pl.col("game_id").is_in(holdout_game_ids))
    _log.info(
        "prepare_geometry_inputs held-out %d/%d games via fold %d/%d",
        len(holdout_game_ids),
        len(distinct_game_ids),
        held_out_fold_id,
        held_out_fold_count,
    )
    if df_train.height == 0:
        raise ValueError("every game landed in the held-out fold; nothing to fit")

    if smoke_limit is not None:
        events_per_game = (
            df_train.select(["game_id", "event_key"])
            .unique()
            .group_by("game_id")
            .agg(pl.len().alias("n_events"))
            .sort("game_id")
        )
        total_events = int(events_per_game.get_column("n_events").sum())
        if total_events > smoke_limit:
            shuffled = events_per_game.sample(
                fraction=1.0, seed=seed, shuffle=True
            ).with_columns(pl.col("n_events").cum_sum().alias("cum_events"))
            kept_games = shuffled.filter(
                pl.col("cum_events") <= smoke_limit
            ).get_column("game_id")
            if kept_games.len() == 0:
                kept_games = shuffled.head(1).get_column("game_id")
            df_train = df_train.filter(pl.col("game_id").is_in(kept_games.implode()))
            _log.info(
                "prepare_geometry_inputs game-subsampled to %d games (%d events; "
                "budget=%d, seed=%d)",
                kept_games.len(),
                df_train.height,
                smoke_limit,
                seed,
            )

    per_event = df_train.sort("event_key")
    n_classes = len(vocab)

    class_idx = per_event.get_column("_class_idx").to_numpy().astype(np.int64)
    counts = _one_hot_counts(class_idx, n_classes)
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    dl_logit_per_class = compute_dl_log_probs_per_class(per_event, n_classes=n_classes)
    dl_logit_class_means = dl_logit_per_class.mean(axis=0).astype(np.float64)
    dl_logit_per_class = dl_logit_per_class - dl_logit_class_means[None, :]

    if PROPENSITY_COLUMN in per_event.columns:
        propensity_active = True
        propensity_z, propensity_logit_mean, propensity_logit_std = (
            _standardized_propensity_logit(per_event, dimension=dimension)
        )
    else:
        propensity_active = False
        propensity_z = np.zeros(per_event.height, dtype=np.float64)
        propensity_logit_mean = 0.0
        propensity_logit_std = 1.0
        _log.warning(
            "dataset at %s carries no %s column; the propensity MNAR covariate "
            "is inactive for dimension=%s",
            parquet_path,
            PROPENSITY_COLUMN,
            dimension,
        )

    handler_logit_by_event: dict[int, float] | None = None
    if handler_covariate_enabled():
        handler_logit_by_event = load_handler_outfield_logit()
    if handler_logit_by_event is not None:
        handler_active = True
        handler_z, handler_logit_mean, handler_logit_std = _standardized_handler_logit(
            event_keys, handler_logit_by_event, dimension=dimension
        )
    else:
        handler_active = False
        handler_z = np.zeros(per_event.height, dtype=np.float64)
        handler_logit_mean = 0.0
        handler_logit_std = 1.0
        if handler_covariate_enabled():
            _log.warning(
                "handler covariate requested but no Model D export resolved; "
                "the handler covariate is inactive for dimension=%s",
                dimension,
            )

    season_league_idx, season_league_labels = _category_index(
        per_event, "season_league"
    )
    scorer_idx, scorer_labels = _category_index(per_event, "scorer")
    _, park_labels = _category_index(per_event, "park_id")
    _, source_labels = _category_index(per_event, "source_family")

    per_event_cols = set(per_event.columns)
    fixed_effects: dict[str, FixedEffectDesign] = {}
    for column in FIXED_EFFECT_COLUMNS:
        if column not in per_event_cols:
            _log.info(
                "prepare_geometry_inputs skipping FE column %r — not present",
                column,
            )
            continue
        fixed_effects[column] = _build_fixed_effect_design(per_event, column)

    coords: dict[str, list[str]] = {
        "season_league": list(season_league_labels),
        "scorer": list(scorer_labels),
        "source": list(source_labels),
        "park": list(park_labels),
    }
    for name, design in fixed_effects.items():
        coords[f"{name}_levels"] = list(design.levels)

    held_out = _build_geometry_held_out_set(
        held_out_df,
        vocab=vocab,
        season_league_labels=season_league_labels,
        scorer_labels=scorer_labels,
        park_labels=park_labels,
        source_labels=source_labels,
        fixed_effects=fixed_effects,
    )
    if held_out_df.height == 0:
        held_out_dl_logit_per_class = np.zeros((0, n_classes), dtype=np.float64)
        held_out_propensity_z = np.zeros(0, dtype=np.float64)
        held_out_handler_z = np.zeros(0, dtype=np.float64)
    else:
        held_out_dl_logit_per_class = compute_dl_log_probs_per_class(
            held_out_df.sort("event_key"), n_classes=n_classes
        )
        held_out_dl_logit_per_class = (
            held_out_dl_logit_per_class - dl_logit_class_means[None, :]
        )
        if propensity_active:
            held_out_propensity_z = _propensity_z_with_frozen_stats(
                held_out_df.sort("event_key"),
                mean=propensity_logit_mean,
                std=propensity_logit_std,
                dimension=dimension,
                context="held-out",
            )
        else:
            held_out_propensity_z = np.zeros(held_out_df.height, dtype=np.float64)
        if handler_active and handler_logit_by_event is not None:
            held_out_handler_z = _handler_z_with_frozen_stats(
                held_out_df.sort("event_key").get_column("event_key").to_numpy(),
                handler_logit_by_event,
                mean=handler_logit_mean,
                std=handler_logit_std,
                dimension=dimension,
                context="held-out",
            )
        else:
            held_out_handler_z = np.zeros(held_out_df.height, dtype=np.float64)

    _log.info(
        "prepare_geometry_inputs dimension=%s K=%d dl_active=%s propensity_active=%s "
        "train_events=%d held_out_events=%d season_leagues=%d scorers=%d parks=%d "
        "sources=%d",
        dimension,
        n_classes,
        spec.dl_active,
        propensity_active,
        per_event.height,
        held_out.n_events,
        len(season_league_labels),
        len(scorer_labels),
        len(park_labels),
        len(source_labels),
    )

    return GeometryInputs(
        event_keys=event_keys,
        dimension=dimension,
        class_labels=list(vocab),
        n_classes=n_classes,
        dl_active=spec.dl_active,
        counts=counts,
        dl_logit_per_class=dl_logit_per_class,
        dl_logit_class_means=dl_logit_class_means,
        season_league_idx=season_league_idx,
        scorer_idx=scorer_idx,
        fixed_effects=fixed_effects,
        coords=coords,
        held_out=held_out,
        held_out_dl_logit_per_class=held_out_dl_logit_per_class,
        propensity_z=propensity_z,
        held_out_propensity_z=held_out_propensity_z,
        propensity_logit_mean=propensity_logit_mean,
        propensity_logit_std=propensity_logit_std,
        propensity_active=propensity_active,
        handler_z=handler_z,
        held_out_handler_z=held_out_handler_z,
        handler_logit_mean=handler_logit_mean,
        handler_logit_std=handler_logit_std,
        handler_active=handler_active,
    )


def _build_geometry_held_out_set(
    df: pl.DataFrame,
    *,
    vocab: list[str],
    season_league_labels: list[str],
    scorer_labels: list[str],
    park_labels: list[str],
    source_labels: list[str],
    fixed_effects: dict[str, FixedEffectDesign],
) -> HeldOutSet:
    empty = np.zeros(0, dtype=np.int64)
    if df.height == 0:
        return HeldOutSet(
            event_keys=empty,
            true_position=empty,
            U=empty,
            season_idx=empty,
            scorer_idx=empty,
            park_idx=empty,
            source_idx=empty,
            fixed_effects={},
        )

    per_event = df.sort("event_key")
    event_keys = (
        per_event.get_column("event_key").cast(pl.Int64).to_numpy().astype(np.int64)
    )
    class_to_idx = {label: i for i, label in enumerate(vocab)}
    true_position = (
        per_event.get_column("_label")
        .replace_strict(class_to_idx, return_dtype=pl.Int64)
        .to_numpy()
        .astype(np.int64)
    )
    u_counts = np.ones_like(true_position, dtype=np.int64)

    season_idx = _encode_codes_with_vocab(
        per_event, "season_league", season_league_labels
    )
    scorer_idx = _encode_codes_with_vocab(per_event, "scorer", scorer_labels)
    park_idx = _encode_codes_with_vocab(per_event, "park_id", park_labels)
    source_idx = _encode_codes_with_vocab(per_event, "source_family", source_labels)

    held_fe: dict[str, FixedEffectDesign] = {
        column: FixedEffectDesign(
            levels=design.levels,
            codes=_encode_codes_with_vocab(per_event, column, list(design.levels)),
        )
        for column, design in fixed_effects.items()
    }

    return HeldOutSet(
        event_keys=event_keys,
        true_position=true_position,
        U=u_counts,
        season_idx=season_idx,
        scorer_idx=scorer_idx,
        park_idx=park_idx,
        source_idx=source_idx,
        fixed_effects=held_fe,
    )
