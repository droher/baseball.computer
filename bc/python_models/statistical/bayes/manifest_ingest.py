"""Helpers backing the ``scorer_observation_propensities`` SQLMesh ``@model``.

Iterates every registered Bayes observation ``BayesTargetSpec``, resolves
the published pointer via ``find_published_manifest(spec.published_manifest_name())``,
reads the winner artifact's ``exports/event_propensity.parquet``, and
yields one polars frame per dimension stamped with the estimated-metadata contract.
Always yields at least one (possibly empty) typed frame so the @model
schema persists when no targets have been published yet.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Iterator

import polars as pl

from python_models.statistical.bayes.registry import all_target_names, get_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.manifests import (
    find_published_manifest,
    read_manifest,
)
from python_models.statistical.schemas import ArtifactManifest, PublishedPointer

_log = logging.getLogger(__name__)


ESTIMATED_CONTRACT_SCHEMA: dict[str, pl.DataType] = {
    "artifact_id": pl.Utf8(),
    "model_name": pl.Utf8(),
    "model_version": pl.Utf8(),
    "source_snapshot_id": pl.Utf8(),
    "method": pl.Utf8(),
    "observed_status": pl.Utf8(),
    "confidence_status": pl.Utf8(),
    "weak_identification_flag": pl.Boolean(),
}

ESTIMATED_CONTRACT_COLUMNS: tuple[str, ...] = tuple(ESTIMATED_CONTRACT_SCHEMA.keys())

METHOD_HIERARCHICAL_BAYES_SOFTMAX = "hierarchical_bayes_softmax"
METHOD_HIERARCHICAL_BAYES_NB = "hierarchical_bayes_nb"
METHOD_HIERARCHICAL_LOGISTIC = "hierarchical_logistic"


def stamp_estimated_contract(
    frame: pl.DataFrame,
    manifest: ArtifactManifest,
    *,
    method: str,
    observed_status: str = "estimated",
) -> pl.DataFrame:
    """Stamp the eight estimated-metadata contract columns onto ``frame``.

    The values are constant within an artifact's rows. ``model_name`` /
    ``model_version`` / ``weak_identification_flag`` come from the bayes
    extras; ``artifact_id`` / ``source_snapshot_id`` / ``validation_status``
    come from the top-level manifest; ``method`` / ``observed_status`` are
    per-family constants supplied by the caller.
    """
    extras = manifest.bayes_extras
    if extras is None:
        raise ValueError(
            f"manifest {manifest.artifact_id} has no bayes_extras; "
            "cannot stamp the estimated contract"
        )
    return frame.with_columns(
        pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("artifact_id"),
        pl.lit(extras.model_name, dtype=pl.Utf8).alias("model_name"),
        pl.lit(extras.model_version, dtype=pl.Utf8).alias("model_version"),
        pl.lit(manifest.source_snapshot_id, dtype=pl.Utf8).alias(
            "source_snapshot_id"
        ),
        pl.lit(method, dtype=pl.Utf8).alias("method"),
        pl.lit(observed_status, dtype=pl.Utf8).alias("observed_status"),
        pl.lit(str(manifest.validation_status), dtype=pl.Utf8).alias(
            "confidence_status"
        ),
        pl.lit(extras.weak_identification_flag, dtype=pl.Boolean()).alias(
            "weak_identification_flag"
        ),
    )


PROPENSITY_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "dimension": pl.Utf8(),
    "p_observed_mean": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


CREDIT_SHARE_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "fielding_position": pl.UInt8(),
    "credit_type": pl.Utf8(),
    "expected_share": pl.Float64(),
    "none_share": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


BALL_HANDLER_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "fielding_position": pl.UInt8(),
    "expected_share": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


GEOMETRY_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "geometry_dimension": pl.Utf8(),
    "class_index": pl.UInt8(),
    "class_label": pl.Utf8(),
    "expected_share": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


ADVANCEMENT_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "baserunner": pl.Utf8(),
    "advancement_class": pl.Utf8(),
    "expected_share": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


PARK_FACTOR_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "park_id": pl.Utf8(),
    "season": pl.Int16(),
    "league": pl.Utf8(),
    "outcome": pl.Utf8(),
    "theta_mean": pl.Float64(),
    "theta_sd": pl.Float64(),
    "theta_hdi_lower": pl.Float64(),
    "theta_hdi_upper": pl.Float64(),
    "park_factor_mean": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


RUN_EXPECTANCY_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "state": pl.Utf8(),
    "base_state": pl.Int8(),
    "outs": pl.Int8(),
    "season": pl.Int16(),
    "league": pl.Utf8(),
    "outcome": pl.Utf8(),
    "re_value_mean": pl.Float64(),
    "re_value_sd": pl.Float64(),
    "re_value_hdi_lower": pl.Float64(),
    "re_value_hdi_upper": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


PITCH_SUMMARY_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "result_family": pl.Utf8(),
    "season": pl.Int16(),
    "league": pl.Utf8(),
    "final_count_class": pl.Utf8(),
    "balls": pl.Int8(),
    "strikes": pl.Int8(),
    "outcome": pl.Utf8(),
    "prob_mean": pl.Float64(),
    "prob_sd": pl.Float64(),
    "prob_hdi_lower": pl.Float64(),
    "prob_hdi_upper": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


STATE_TRANSITION_SUMMARY_SCHEMA: dict[str, pl.DataType] = {
    "start_state": pl.Utf8(),
    "season": pl.Int16(),
    "league": pl.Utf8(),
    "end_class": pl.Utf8(),
    "outcome": pl.Utf8(),
    "prob_mean": pl.Float64(),
    "prob_sd": pl.Float64(),
    "prob_hdi_lower": pl.Float64(),
    "prob_hdi_upper": pl.Float64(),
    **ESTIMATED_CONTRACT_SCHEMA,
}


def _empty_frame(schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return pl.DataFrame(schema=schema)


def empty_propensity_frame() -> pl.DataFrame:
    return _empty_frame(PROPENSITY_SCHEMA)


def empty_credit_share_frame() -> pl.DataFrame:
    return _empty_frame(CREDIT_SHARE_SCHEMA)


def empty_ball_handler_frame() -> pl.DataFrame:
    return _empty_frame(BALL_HANDLER_SCHEMA)


def empty_geometry_frame() -> pl.DataFrame:
    return _empty_frame(GEOMETRY_SCHEMA)


def empty_advancement_frame() -> pl.DataFrame:
    return _empty_frame(ADVANCEMENT_SCHEMA)


def empty_park_factor_frame() -> pl.DataFrame:
    return _empty_frame(PARK_FACTOR_SUMMARY_SCHEMA)


def empty_run_expectancy_frame() -> pl.DataFrame:
    return _empty_frame(RUN_EXPECTANCY_SUMMARY_SCHEMA)


def empty_pitch_summary_frame() -> pl.DataFrame:
    return _empty_frame(PITCH_SUMMARY_SUMMARY_SCHEMA)


def empty_state_transition_frame() -> pl.DataFrame:
    return _empty_frame(STATE_TRANSITION_SUMMARY_SCHEMA)


def _iter_specs_by_kind(kind: str) -> Iterator[BayesTargetSpec]:
    from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]

    for name in all_target_names():
        spec = get_target(name)
        if spec.outcome_kind == kind:
            yield spec


def _specs_for_multinomial_export(export: str) -> Iterator[BayesTargetSpec]:
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export == export:
            yield spec


def _specs_for_count_export(export: str) -> Iterator[BayesTargetSpec]:
    for spec in _iter_specs_by_kind("count"):
        if spec.count_export == export:
            yield spec


def _specs_for_bernoulli_dataset(dataset_name: str) -> Iterator[BayesTargetSpec]:
    for spec in _iter_specs_by_kind("bernoulli"):
        if spec.dataset_name == dataset_name:
            yield spec


OBSERVATION_PROPENSITY_DATASET: str = "model_input_observation_batted_ball"
PITCH_COVERAGE_DATASET: str = "model_input_pitch_summary"


FrameBuilder = Callable[
    [pl.DataFrame, ArtifactManifest, BayesTargetSpec], pl.DataFrame
]


def _iterate_published_export_frames(
    specs: Iterable[BayesTargetSpec],
    *,
    export_filename: str,
    build_frame: FrameBuilder,
) -> Iterator[pl.DataFrame]:
    """Yield one transformed frame per published target whose export exists.

    Resolves each spec's published pointer, reads the artifact's
    ``exports/<export_filename>``, and delegates the per-family column
    transform to ``build_frame(df, manifest, spec)``. Skips targets with
    no pointer, a missing export file, or an empty export.
    """
    for spec in specs:
        pointer_path = find_published_manifest(spec.published_manifest_name())
        if pointer_path is None:
            _log.info(
                "bayes.manifest_ingest: no pointer for %s; skipping",
                spec.published_manifest_name(),
            )
            continue
        pointer = PublishedPointer.model_validate_json(
            pointer_path.read_text(encoding="utf-8")
        )
        manifest = read_manifest(pointer.manifest_path)
        export_path = pointer.manifest_path.parent / "exports" / export_filename
        if not export_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing %s for %s at %s",
                export_filename,
                spec.published_manifest_name(),
                export_path,
            )
            continue
        df = pl.read_parquet(str(export_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: %s empty for %s",
                export_filename,
                spec.published_manifest_name(),
            )
            continue
        yield build_frame(df, manifest, spec)


def _aggregate_frames(
    frames: Iterator[pl.DataFrame],
    *,
    schema: dict[str, pl.DataType],
    empty_factory: Callable[[], pl.DataFrame],
    label: str,
) -> Iterator[pl.DataFrame]:
    """Project each frame onto ``schema`` and guarantee one typed frame.

    ``@model`` callers want a non-empty iterator so SQLMesh can persist the
    typed schema even when no targets have published yet.
    """
    emitted = False
    select_exprs = [pl.col(name).cast(dtype) for name, dtype in schema.items()]
    for frame in frames:
        emitted = True
        yield frame.select(select_exprs)
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no %s targets published; yielding empty frame",
            label,
        )
        yield empty_factory()


def _build_propensity_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.with_columns(
        pl.col("event_key").cast(pl.UInt32),
        pl.col("dimension").cast(pl.Utf8),
        pl.col("p_observed_mean").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_LOGISTIC
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_observation_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes observation (Bernoulli) target."""
    yield from _iterate_published_export_frames(
        _specs_for_bernoulli_dataset(OBSERVATION_PROPENSITY_DATASET),
        export_filename="event_propensity.parquet",
        build_frame=_build_propensity_frame,
    )


def iterate_published_pitch_coverage_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes pitch-coverage (Bernoulli) target."""
    yield from _iterate_published_export_frames(
        _specs_for_bernoulli_dataset(PITCH_COVERAGE_DATASET),
        export_filename="event_propensity.parquet",
        build_frame=_build_propensity_frame,
    )


def aggregate_observation_propensity_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_observation_frames(),
        schema=PROPENSITY_SCHEMA,
        empty_factory=empty_propensity_frame,
        label="observation",
    )


def aggregate_pitch_coverage_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_pitch_coverage_frames(),
        schema=PROPENSITY_SCHEMA,
        empty_factory=empty_propensity_frame,
        label="pitch-coverage",
    )


def _build_credit_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    if "none_share" not in df.columns:
        df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("none_share"))
    out = df.with_columns(
        pl.col("event_key").cast(pl.UInt32),
        pl.col("fielding_position").cast(pl.UInt8),
        pl.col("credit_type").cast(pl.Utf8),
        pl.col("expected_share").cast(pl.Float64),
        pl.col("none_share").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_SOFTMAX
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (credit_type=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_credit_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes fielding-credit (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("credit"),
        export_filename="event_credit.parquet",
        build_frame=_build_credit_frame,
    )


def aggregate_fielding_credit_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_credit_frames(),
        schema=CREDIT_SHARE_SCHEMA,
        empty_factory=empty_credit_share_frame,
        label="fielding-credit",
    )


def _build_ball_handler_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.with_columns(
        pl.col("event_key").cast(pl.UInt32),
        pl.col("fielding_position").cast(pl.UInt8),
        pl.col("expected_share").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_SOFTMAX
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_ball_handler_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes ball-handler (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("ball_handler"),
        export_filename="ball_handler_probabilities.parquet",
        build_frame=_build_ball_handler_frame,
    )


def aggregate_ball_handler_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_ball_handler_frames(),
        schema=BALL_HANDLER_SCHEMA,
        empty_factory=empty_ball_handler_frame,
        label="ball-handler",
    )


def _build_geometry_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    if "geometry_dimension" in df.columns:
        exported_dimensions = (
            df.get_column("geometry_dimension").cast(pl.Utf8).unique().to_list()
        )
        if exported_dimensions != [spec.dimension]:
            raise ValueError(
                f"geometry_probabilities.parquet for "
                f"{spec.published_manifest_name()} carries "
                f"geometry_dimension={exported_dimensions!r}; expected "
                f"{spec.dimension!r}"
            )
        dimension_expr = pl.col("geometry_dimension").cast(pl.Utf8)
    else:
        dimension_expr = pl.lit(spec.dimension, dtype=pl.Utf8).alias(
            "geometry_dimension"
        )
    out = df.with_columns(
        pl.col("event_key").cast(pl.UInt32),
        dimension_expr,
        pl.col("class_index").cast(pl.UInt8),
        pl.col("class_label").cast(pl.Utf8),
        pl.col("expected_share").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_SOFTMAX
    ).select(list(GEOMETRY_SCHEMA.keys()))
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_geometry_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes geometry (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("geometry"),
        export_filename="geometry_probabilities.parquet",
        build_frame=_build_geometry_frame,
    )


def aggregate_geometry_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_geometry_frames(),
        schema=GEOMETRY_SCHEMA,
        empty_factory=empty_geometry_frame,
        label="geometry",
    )


def _build_advancement_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.with_columns(
        pl.col("event_key").cast(pl.UInt32),
        pl.col("baserunner").cast(pl.Utf8),
        pl.col("advancement_class").cast(pl.Utf8),
        pl.col("expected_share").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_SOFTMAX
    ).select(list(ADVANCEMENT_SCHEMA.keys()))
    _log.info(
        "bayes.manifest_ingest: %d advancement rows from %s",
        out.height,
        spec.published_manifest_name(),
    )
    return out


def iterate_published_advancement_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes advancement (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("advancement"),
        export_filename="advancement_probabilities.parquet",
        build_frame=_build_advancement_frame,
    )


def aggregate_advancement_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_advancement_frames(),
        schema=ADVANCEMENT_SCHEMA,
        empty_factory=empty_advancement_frame,
        label="advancement",
    )


def _build_park_factor_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.select(
        pl.col("park_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("theta_mean").cast(pl.Float64),
        pl.col("theta_sd").cast(pl.Float64),
        pl.col("theta_hdi_lower").cast(pl.Float64),
        pl.col("theta_hdi_upper").cast(pl.Float64),
        pl.col("park_factor_mean").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_NB
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_park_factor_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes park-factor (count) target."""
    yield from _iterate_published_export_frames(
        _specs_for_count_export("park_factor"),
        export_filename="park_factor_summary.parquet",
        build_frame=_build_park_factor_frame,
    )


def aggregate_park_factor_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_park_factor_frames(),
        schema=PARK_FACTOR_SUMMARY_SCHEMA,
        empty_factory=empty_park_factor_frame,
        label="park-factor",
    )


def _build_run_expectancy_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.select(
        pl.col("state").cast(pl.Utf8),
        pl.col("base_state").cast(pl.Int8),
        pl.col("outs").cast(pl.Int8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("re_value_mean").cast(pl.Float64),
        pl.col("re_value_sd").cast(pl.Float64),
        pl.col("re_value_hdi_lower").cast(pl.Float64),
        pl.col("re_value_hdi_upper").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_NB
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_run_expectancy_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes run-expectancy (count) target."""
    yield from _iterate_published_export_frames(
        _specs_for_count_export("run_expectancy"),
        export_filename="run_expectancy_summary.parquet",
        build_frame=_build_run_expectancy_frame,
    )


def aggregate_run_expectancy_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_run_expectancy_frames(),
        schema=RUN_EXPECTANCY_SUMMARY_SCHEMA,
        empty_factory=empty_run_expectancy_frame,
        label="run-expectancy",
    )


def _build_pitch_summary_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.select(
        pl.col("result_family").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("final_count_class").cast(pl.Utf8),
        pl.col("balls").cast(pl.Int8),
        pl.col("strikes").cast(pl.Int8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("prob_mean").cast(pl.Float64),
        pl.col("prob_sd").cast(pl.Float64),
        pl.col("prob_hdi_lower").cast(pl.Float64),
        pl.col("prob_hdi_upper").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_NB
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_pitch_summary_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes pitch-summary (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("pitch_summary"),
        export_filename="pitch_summary_summary.parquet",
        build_frame=_build_pitch_summary_frame,
    )


def aggregate_pitch_summary_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_pitch_summary_frames(),
        schema=PITCH_SUMMARY_SUMMARY_SCHEMA,
        empty_factory=empty_pitch_summary_frame,
        label="pitch-summary",
    )


def _build_state_transition_frame(
    df: pl.DataFrame, manifest: ArtifactManifest, spec: BayesTargetSpec
) -> pl.DataFrame:
    out = df.select(
        pl.col("start_state").cast(pl.Utf8),
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("end_class").cast(pl.Utf8),
        pl.col("outcome").cast(pl.Utf8),
        pl.col("prob_mean").cast(pl.Float64),
        pl.col("prob_sd").cast(pl.Float64),
        pl.col("prob_hdi_lower").cast(pl.Float64),
        pl.col("prob_hdi_upper").cast(pl.Float64),
    )
    out = stamp_estimated_contract(
        out, manifest, method=METHOD_HIERARCHICAL_BAYES_SOFTMAX
    )
    _log.info(
        "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
        out.height,
        spec.published_manifest_name(),
        spec.dimension,
    )
    return out


def iterate_published_state_transition_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes state-transition (multinomial) target."""
    yield from _iterate_published_export_frames(
        _specs_for_multinomial_export("state_transition"),
        export_filename="state_transition_summary.parquet",
        build_frame=_build_state_transition_frame,
    )


def aggregate_state_transition_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    yield from _aggregate_frames(
        iterate_published_state_transition_frames(),
        schema=STATE_TRANSITION_SUMMARY_SCHEMA,
        empty_factory=empty_state_transition_frame,
        label="state-transition",
    )
