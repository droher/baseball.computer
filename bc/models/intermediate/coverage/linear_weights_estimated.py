"""Estimated linear weights carrying posterior uncertainty (derives from Model G).

Propagates Model G's published run-expectancy posterior draws through the
deterministic linear-weights formula. Reads the per-combo transition counts
from ``main_models.linear_weights_transition_counts`` and Model G's published
``run_expectancy_posterior.parquet``, computes the per-(season, league, play)
run value for every draw, centers each draw against the per-(season, league)
all-play weighted mean, and collapses across draws to ``run_value_{mean, sd,
hdi_lower, hdi_upper}`` (94% HDI). The deterministic ``main_models.linear_weights``
point surface is untouched; this estimated sibling lives beside it. Materializes
a typed empty frame until Model G's run-expectancy pointer lands.
"""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

_GRAIN_COLUMNS = (
    exp.column("season"),
    exp.column("league"),
    exp.column("play"),
)

_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("season"),
                    exp.column("league"),
                    exp.column("play"),
                    exp.column("play_category"),
                    exp.column("run_value_mean"),
                ]
            ),
        },
    ),
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=list(_GRAIN_COLUMNS))},
    ),
    ("estimated_contract_complete", {}),
]


@model(
    "main_models.linear_weights_estimated",
    kind="FULL",
    columns={
        "season": "SMALLINT",
        "league": "VARCHAR",
        "play": "VARCHAR",
        "play_category": "VARCHAR",
        "n_events": "BIGINT",
        "run_value_mean": "DOUBLE",
        "run_value_sd": "DOUBLE",
        "run_value_hdi_lower": "DOUBLE",
        "run_value_hdi_upper": "DOUBLE",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    grain=["season", "league", "play"],
    audits=_AUDITS,
    description=(
        "Per-(season, league, play) run value with posterior uncertainty, "
        "propagated from the Bayes run-expectancy posterior draws (Model G) "
        "through the deterministic linear-weights formula. Grain "
        "(season, league, play). Carries the centered run value posterior "
        "summary (run_value_mean / sd / 94% HDI) plus the estimated-metadata "
        "contract. The estimated sibling of the deterministic linear_weights "
        "point surface; the two are never joined or unioned (their columns "
        "differ). Materializes a typed empty frame until Model G publishes."
    ),
    depends_on={"main_models.linear_weights_transition_counts"},
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pl.DataFrame]:
    del kwargs
    import logging

    from python_models.statistical.bayes.manifest_ingest import (
        METHOD_HIERARCHICAL_BAYES_NB,
        stamp_estimated_contract,
    )
    from python_models.statistical.linear_weights_estimated import (
        RUN_VALUE_SUMMARY_SCHEMA,
        propagate_linear_weights_draws,
    )
    from python_models.statistical.manifests import (
        find_published_manifest,
        read_manifest,
    )
    from python_models.statistical.schemas import PublishedPointer

    log = logging.getLogger(__name__)

    empty_schema: dict[str, pl.DataType] = {
        **RUN_VALUE_SUMMARY_SCHEMA,
        "artifact_id": pl.Utf8(),
        "model_name": pl.Utf8(),
        "model_version": pl.Utf8(),
        "source_snapshot_id": pl.Utf8(),
        "method": pl.Utf8(),
        "observed_status": pl.Utf8(),
        "confidence_status": pl.Utf8(),
        "weak_identification_flag": pl.Boolean(),
    }

    pointer_path = find_published_manifest("run_expectancy")
    if pointer_path is None:
        log.info(
            "linear_weights_estimated: no run_expectancy pointer; yielding empty frame"
        )
        yield pl.DataFrame(schema=empty_schema)
        return

    pointer = PublishedPointer.model_validate_json(
        pointer_path.read_text(encoding="utf-8")
    )
    manifest = read_manifest(pointer.manifest_path)
    posterior_path = (
        pointer.manifest_path.parent / "exports" / "run_expectancy_posterior.parquet"
    )
    if not posterior_path.exists():
        log.warning(
            "linear_weights_estimated: missing posterior at %s; yielding empty frame",
            posterior_path,
        )
        yield pl.DataFrame(schema=empty_schema)
        return

    re_draws = pl.read_parquet(str(posterior_path))
    if re_draws.height == 0:
        log.info(
            "linear_weights_estimated: empty posterior; yielding empty frame"
        )
        yield pl.DataFrame(schema=empty_schema)
        return

    counts_table = context.resolve_table(
        "main_models.linear_weights_transition_counts"
    )
    cursor = context.engine_adapter.cursor
    transition_counts = cursor.sql(
        f"""
        SELECT
            season,
            league,
            play,
            play_category,
            run_expectancy_start_key,
            run_expectancy_end_key,
            runs_on_play,
            n
        FROM {counts_table}
        """
    ).pl()

    summary = propagate_linear_weights_draws(transition_counts, re_draws)
    if summary.height == 0:
        log.info(
            "linear_weights_estimated: empty after propagation; yielding empty frame"
        )
        yield pl.DataFrame(schema=empty_schema)
        return

    stamped = stamp_estimated_contract(
        summary, manifest, method=METHOD_HIERARCHICAL_BAYES_NB
    )
    log.info("linear_weights_estimated: %d rows", stamped.height)
    yield stamped.select(
        pl.col("season").cast(pl.Int16),
        pl.col("league").cast(pl.Utf8),
        pl.col("play").cast(pl.Utf8),
        pl.col("play_category").cast(pl.Utf8),
        pl.col("n_events").cast(pl.Int64),
        pl.col("run_value_mean").cast(pl.Float64),
        pl.col("run_value_sd").cast(pl.Float64),
        pl.col("run_value_hdi_lower").cast(pl.Float64),
        pl.col("run_value_hdi_upper").cast(pl.Float64),
        pl.col("artifact_id").cast(pl.Utf8),
        pl.col("model_name").cast(pl.Utf8),
        pl.col("model_version").cast(pl.Utf8),
        pl.col("source_snapshot_id").cast(pl.Utf8),
        pl.col("method").cast(pl.Utf8),
        pl.col("observed_status").cast(pl.Utf8),
        pl.col("confidence_status").cast(pl.Utf8),
        pl.col("weak_identification_flag").cast(pl.Boolean()),
    )
