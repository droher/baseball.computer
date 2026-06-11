"""Helpers backing the ``scorer_observation_propensities`` SQLMesh ``@model``.

Iterates every registered Bayes observation ``BayesTargetSpec``, resolves
the published pointer via ``find_published_manifest(spec.published_manifest_name())``,
reads the winner artifact's ``exports/event_propensity.parquet``, and
yields one polars frame per dimension stamped with ``bayes_artifact_id``.
Always yields at least one (possibly empty) typed frame so the @model
schema persists when no targets have been published yet.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator

import polars as pl

from python_models.statistical.bayes.registry import all_target_names, get_target
from python_models.statistical.bayes.specs import BayesTargetSpec
from python_models.statistical.manifests import (
    find_published_manifest,
    read_manifest,
)
from python_models.statistical.schemas import PublishedPointer

_log = logging.getLogger(__name__)


PROPENSITY_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "dimension": pl.Utf8(),
    "p_observed_mean": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


CREDIT_SHARE_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "fielding_position": pl.UInt8(),
    "credit_type": pl.Utf8(),
    "expected_share": pl.Float64(),
    "none_share": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


BALL_HANDLER_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "fielding_position": pl.UInt8(),
    "expected_share": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


GEOMETRY_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "geometry_dimension": pl.Utf8(),
    "class_index": pl.UInt8(),
    "class_label": pl.Utf8(),
    "expected_share": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


ADVANCEMENT_SCHEMA: dict[str, pl.DataType] = {
    "event_key": pl.UInt32(),
    "baserunner": pl.Utf8(),
    "advancement_class": pl.Utf8(),
    "expected_share": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
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
    "bayes_artifact_id": pl.Utf8(),
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
    "ess_bulk": pl.Float64(),
    "rhat": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
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
    "ess_bulk": pl.Float64(),
    "rhat": pl.Float64(),
    "bayes_artifact_id": pl.Utf8(),
}


def empty_propensity_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=PROPENSITY_SCHEMA)


def empty_credit_share_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=CREDIT_SHARE_SCHEMA)


def empty_ball_handler_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=BALL_HANDLER_SCHEMA)


def empty_geometry_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=GEOMETRY_SCHEMA)


def empty_advancement_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=ADVANCEMENT_SCHEMA)


def empty_park_factor_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=PARK_FACTOR_SUMMARY_SCHEMA)


def empty_run_expectancy_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=RUN_EXPECTANCY_SUMMARY_SCHEMA)


def empty_pitch_summary_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=PITCH_SUMMARY_SUMMARY_SCHEMA)


def _iter_specs_by_kind(kind: str) -> Iterator[BayesTargetSpec]:
    from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]

    for name in all_target_names():
        spec = get_target(name)
        if spec.outcome_kind == kind:
            yield spec


def iterate_published_observation_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes observation (Bernoulli) target."""
    for spec in _iter_specs_by_kind("bernoulli"):
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
        event_propensity_path = (
            pointer.manifest_path.parent / "exports" / "event_propensity.parquet"
        )
        if not event_propensity_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing event_propensity.parquet for %s at %s",
                spec.published_manifest_name(),
                event_propensity_path,
            )
            continue
        df = pl.read_parquet(str(event_propensity_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: event_propensity empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.col("dimension").cast(pl.Utf8),
            pl.col("p_observed_mean").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_observation_propensity_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_observation_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("dimension").cast(pl.Utf8),
                pl.col("p_observed_mean").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no observation targets published; yielding empty frame"
        )
        yield empty_propensity_frame()


def iterate_published_credit_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes fielding-credit (multinomial) target."""
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export != "credit":
            continue
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
        share_path = pointer.manifest_path.parent / "exports" / "event_credit.parquet"
        if not share_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing event_credit.parquet for %s at %s",
                spec.published_manifest_name(),
                share_path,
            )
            continue
        df = pl.read_parquet(str(share_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: event_credit empty for %s",
                spec.published_manifest_name(),
            )
            continue
        if "none_share" not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias("none_share"))
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.col("fielding_position").cast(pl.UInt8),
            pl.col("credit_type").cast(pl.Utf8),
            pl.col("expected_share").cast(pl.Float64),
            pl.col("none_share").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (credit_type=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_fielding_credit_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_credit_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("fielding_position").cast(pl.UInt8),
                pl.col("credit_type").cast(pl.Utf8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("none_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no fielding-credit targets published; yielding empty frame"
        )
        yield empty_credit_share_frame()


def iterate_published_ball_handler_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes ball-handler (multinomial) target."""
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export != "ball_handler":
            continue
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
        share_path = (
            pointer.manifest_path.parent
            / "exports"
            / "ball_handler_probabilities.parquet"
        )
        if not share_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing ball_handler_probabilities.parquet for %s at %s",
                spec.published_manifest_name(),
                share_path,
            )
            continue
        df = pl.read_parquet(str(share_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: ball_handler_probabilities empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.col("fielding_position").cast(pl.UInt8),
            pl.col("expected_share").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_ball_handler_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_ball_handler_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("fielding_position").cast(pl.UInt8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no ball-handler targets published; yielding empty frame"
        )
        yield empty_ball_handler_frame()


def iterate_published_geometry_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes geometry (multinomial) target."""
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export != "geometry":
            continue
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
        share_path = (
            pointer.manifest_path.parent / "exports" / "geometry_probabilities.parquet"
        )
        if not share_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing geometry_probabilities.parquet for %s at %s",
                spec.published_manifest_name(),
                share_path,
            )
            continue
        df = pl.read_parquet(str(share_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: geometry_probabilities empty for %s",
                spec.published_manifest_name(),
            )
            continue
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
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            dimension_expr,
            pl.col("class_index").cast(pl.UInt8),
            pl.col("class_label").cast(pl.Utf8),
            pl.col("expected_share").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        ).select(list(GEOMETRY_SCHEMA.keys()))
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_geometry_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_geometry_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("geometry_dimension").cast(pl.Utf8),
                pl.col("class_index").cast(pl.UInt8),
                pl.col("class_label").cast(pl.Utf8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no geometry targets published; yielding empty frame"
        )
        yield empty_geometry_frame()


def iterate_published_advancement_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes advancement (multinomial) target."""
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export != "advancement":
            continue
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
        share_path = (
            pointer.manifest_path.parent
            / "exports"
            / "advancement_probabilities.parquet"
        )
        if not share_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing advancement_probabilities.parquet for %s at %s",
                spec.published_manifest_name(),
                share_path,
            )
            continue
        df = pl.read_parquet(str(share_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: advancement_probabilities empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.with_columns(
            pl.col("event_key").cast(pl.UInt32),
            pl.col("baserunner").cast(pl.Utf8),
            pl.col("advancement_class").cast(pl.Utf8),
            pl.col("expected_share").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        ).select(list(ADVANCEMENT_SCHEMA.keys()))
        _log.info(
            "bayes.manifest_ingest: %d advancement rows from %s",
            df.height,
            spec.published_manifest_name(),
        )
        yield df


def aggregate_advancement_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_advancement_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("event_key").cast(pl.UInt32),
                pl.col("baserunner").cast(pl.Utf8),
                pl.col("advancement_class").cast(pl.Utf8),
                pl.col("expected_share").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no advancement targets published; yielding empty frame"
        )
        yield empty_advancement_frame()


def iterate_published_park_factor_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes park-factor (count) target."""
    for spec in _iter_specs_by_kind("count"):
        if spec.count_export != "park_factor":
            continue
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
        summary_path = (
            pointer.manifest_path.parent / "exports" / "park_factor_summary.parquet"
        )
        if not summary_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing park_factor_summary.parquet for %s at %s",
                spec.published_manifest_name(),
                summary_path,
            )
            continue
        df = pl.read_parquet(str(summary_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: park_factor_summary empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.select(
            pl.col("park_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int16),
            pl.col("league").cast(pl.Utf8),
            pl.col("outcome").cast(pl.Utf8),
            pl.col("theta_mean").cast(pl.Float64),
            pl.col("theta_sd").cast(pl.Float64),
            pl.col("theta_hdi_lower").cast(pl.Float64),
            pl.col("theta_hdi_upper").cast(pl.Float64),
            pl.col("park_factor_mean").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_park_factor_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_park_factor_frames():
        emitted = True
        yield frame.select(
            [
                pl.col("park_id").cast(pl.Utf8),
                pl.col("season").cast(pl.Int16),
                pl.col("league").cast(pl.Utf8),
                pl.col("outcome").cast(pl.Utf8),
                pl.col("theta_mean").cast(pl.Float64),
                pl.col("theta_sd").cast(pl.Float64),
                pl.col("theta_hdi_lower").cast(pl.Float64),
                pl.col("theta_hdi_upper").cast(pl.Float64),
                pl.col("park_factor_mean").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no park-factor targets published; yielding empty frame"
        )
        yield empty_park_factor_frame()


def iterate_published_run_expectancy_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes run-expectancy (count) target."""
    for spec in _iter_specs_by_kind("count"):
        if spec.count_export != "run_expectancy":
            continue
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
        summary_path = (
            pointer.manifest_path.parent / "exports" / "run_expectancy_summary.parquet"
        )
        if not summary_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing run_expectancy_summary.parquet for %s at %s",
                spec.published_manifest_name(),
                summary_path,
            )
            continue
        df = pl.read_parquet(str(summary_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: run_expectancy_summary empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.select(
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
            pl.col("ess_bulk").cast(pl.Float64),
            pl.col("rhat").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_run_expectancy_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_run_expectancy_frames():
        emitted = True
        yield frame.select(
            [
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
                pl.col("ess_bulk").cast(pl.Float64),
                pl.col("rhat").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no run-expectancy targets published; yielding empty frame"
        )
        yield empty_run_expectancy_frame()


def iterate_published_pitch_summary_frames() -> Iterator[pl.DataFrame]:
    """Yield one frame per published Bayes pitch-summary (multinomial) target."""
    for spec in _iter_specs_by_kind("multinomial"):
        if spec.multinomial_export != "pitch_summary":
            continue
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
        summary_path = (
            pointer.manifest_path.parent / "exports" / "pitch_summary_summary.parquet"
        )
        if not summary_path.exists():
            _log.warning(
                "bayes.manifest_ingest: missing pitch_summary_summary.parquet for %s at %s",
                spec.published_manifest_name(),
                summary_path,
            )
            continue
        df = pl.read_parquet(str(summary_path))
        if df.height == 0:
            _log.info(
                "bayes.manifest_ingest: pitch_summary_summary empty for %s",
                spec.published_manifest_name(),
            )
            continue
        df = df.select(
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
            pl.col("ess_bulk").cast(pl.Float64),
            pl.col("rhat").cast(pl.Float64),
            pl.lit(manifest.artifact_id, dtype=pl.Utf8).alias("bayes_artifact_id"),
        )
        _log.info(
            "bayes.manifest_ingest: %d rows from %s (dimension=%s)",
            df.height,
            spec.published_manifest_name(),
            spec.dimension,
        )
        yield df


def aggregate_pitch_summary_frames() -> Iterator[pl.DataFrame]:
    """Adapter that always yields at least one typed frame for the @model."""
    emitted = False
    for frame in iterate_published_pitch_summary_frames():
        emitted = True
        yield frame.select(
            [
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
                pl.col("ess_bulk").cast(pl.Float64),
                pl.col("rhat").cast(pl.Float64),
                pl.col("bayes_artifact_id").cast(pl.Utf8),
            ]
        )
    if not emitted:
        _log.info(
            "bayes.manifest_ingest: no pitch-summary targets published; yielding empty frame"
        )
        yield empty_pitch_summary_frame()
