"""EDA pipeline for frozen modeling-dataset Parquet snapshots.

Reads a ``model_input_*`` snapshot via DuckDB ``read_parquet`` (no live
DB), runs the doc-02 EDA module battery, derives blocking findings and
weak-identification flags, writes per-module Parquet outputs +
``report.json`` + ``manifest.json`` + ``eda.md`` under
``artifacts/statistical/eda/<dataset>/<artifact_id>/``.
"""

from __future__ import annotations

import logging
import os
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb
import polars as pl
from pydantic import BaseModel

from python_models.statistical.config import DATASETS_ROOT, EDA_ROOT
from python_models.statistical.dataset_registry import DatasetSpec
from python_models.statistical.leakage import (
    check_split_leakage,
    summarize_violations,
    violations_to_dataframe,
)
from python_models.statistical.manifests import (
    package_versions as snapshot_package_versions,
    query_hash,
    read_manifest,
    utc_now,
    write_manifest,
)
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.schemas import (
    ArtifactManifest,
    BlockingFinding,
    EdaReport,
    WeakIdentificationFlag,
)

_log = logging.getLogger(__name__)


_MODULE_FILES: dict[str, str] = {
    "missingness_by_slice": "missingness_by_slice.parquet",
    "target_distribution": "target_distribution.parquet",
    "source_family_block_missingness": "source_family_block_missingness.parquet",
    "data_error_concentration": "data_error_concentration.parquet",
    "connectivity_edges": "connectivity_edges.parquet",
    "collinearity_report": "collinearity_report.parquet",
    "candidate_interactions": "candidate_interactions.parquet",
    "weak_identification_flags": "weak_identification_flags.parquet",
    "split_leakage_report": "split_leakage_report.parquet",
}

_HOLDOUT_FLAG_NAMES: tuple[str, ...] = (
    "is_heldout_scorer",
    "is_heldout_park",
    "is_heldout_alignment_regime",
    "is_heldout_source_acquisition_block",
    "is_heldout_season_block",
    "is_heldout_aggregate_total",
    "is_heldout_player_group",
)


class EdaThresholds(BaseModel):
    """Tunable thresholds for blocking findings + interaction discovery."""

    dominant_share: float = 0.95
    dominant_min_rows: int = 100
    interaction_min_rows: int = 50
    interaction_min_lift: float = 0.05
    weak_identification_share: float = 0.90


@dataclass(frozen=True)
class _Snapshot:
    parquet_path: Path
    columns: frozenset[str]
    holdout_flag_columns: tuple[str, ...]


@dataclass(frozen=True)
class _InteractionScan:
    left: str
    right: str
    target: str


def _quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_columns(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> frozenset[str]:
    rows = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet({_quote_literal(str(parquet_path))})"
    ).fetchall()
    return frozenset(str(r[0]) for r in rows)


def _detect_holdout_flag_columns(
    con: duckdb.DuckDBPyConnection, parquet_path: Path
) -> tuple[str, ...]:
    if "holdout_flags" not in _read_columns(con, parquet_path):
        return ()
    sample = con.execute(
        f"SELECT holdout_flags FROM read_parquet({_quote_literal(str(parquet_path))}) "
        "WHERE holdout_flags IS NOT NULL LIMIT 1"
    ).fetchone()
    if sample is None:
        return _HOLDOUT_FLAG_NAMES
    payload = sample[0]
    if isinstance(payload, dict):
        keys = tuple(k.lower() for k in payload.keys())
        return tuple(name for name in _HOLDOUT_FLAG_NAMES if name in keys)
    return _HOLDOUT_FLAG_NAMES


def _open_snapshot(con: duckdb.DuckDBPyConnection, parquet_path: Path) -> _Snapshot:
    columns = _read_columns(con, parquet_path)
    flag_cols = _detect_holdout_flag_columns(con, parquet_path)
    return _Snapshot(
        parquet_path=parquet_path,
        columns=columns,
        holdout_flag_columns=flag_cols,
    )


def _from_clause(snapshot: _Snapshot) -> str:
    return f"read_parquet({_quote_literal(str(snapshot.parquet_path))})"


def _resolve_source_snapshot_id(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot
) -> str:
    if "source_snapshot_id" not in snapshot.columns:
        raise ValueError(
            f"snapshot {snapshot.parquet_path} missing source_snapshot_id column"
        )
    rows = con.execute(
        f"SELECT DISTINCT source_snapshot_id FROM {_from_clause(snapshot)}"
    ).fetchall()
    if len(rows) != 1:
        raise ValueError(
            f"snapshot {snapshot.parquet_path} carries {len(rows)} distinct "
            "source_snapshot_id values; EDA requires exactly one."
        )
    value = rows[0][0]
    if value is None:
        raise ValueError(
            f"snapshot {snapshot.parquet_path} carries NULL source_snapshot_id."
        )
    return str(value)


def _has_columns(snapshot: _Snapshot, *names: str) -> bool:
    return all(n in snapshot.columns for n in names)


def _scalar_int(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    if row is None or row[0] is None:
        return 0
    return int(row[0])


def _dataset_summary(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[int, int, int, int, int]:
    src = _from_clause(snapshot)
    row_count = _scalar_int(con, f"SELECT COUNT(*) FROM {src}")
    target_population_count = (
        _scalar_int(
            con,
            f"SELECT COUNT(*) FROM {src} WHERE target_population_status = 'event_level'",
        )
        if "target_population_status" in snapshot.columns
        else row_count
    )
    observed_truth_count = _scalar_int(
        con,
        f"SELECT COUNT_IF({spec.observed_truth_predicate}) FROM {src}",
    )
    source_family_block_missing_count = (
        _scalar_int(
            con,
            f"SELECT COUNT_IF(source_acquisition_status = 'not_acquired') FROM {src}",
        )
        if "source_acquisition_status" in snapshot.columns
        else 0
    )
    data_error_excluded_count = (
        _scalar_int(
            con,
            f"SELECT COUNT_IF(data_error_risk IS NOT NULL AND data_error_risk != 'none') FROM {src}",
        )
        if "data_error_risk" in snapshot.columns
        else 0
    )
    return (
        row_count,
        target_population_count,
        observed_truth_count,
        source_family_block_missing_count,
        data_error_excluded_count,
    )


def _missingness_by_slice(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    base_slices = ["season", "league", "source_family"]
    optional = ["sentinel_type", "dimension", "geometry_dimension"]
    extras = [c for c in optional if c in snapshot.columns]
    slices = [c for c in base_slices if c in snapshot.columns] + extras
    if not slices:
        slices = (
            ["season"] if "season" in snapshot.columns else list(spec.slice_columns)
        )
    select_cols = ", ".join(_quote_ident(c) for c in slices)
    extra_data_error = (
        ", COUNT_IF(data_error_risk != 'none') AS data_error_rows"
        if "data_error_risk" in snapshot.columns
        else ""
    )
    sql = (
        f"SELECT {select_cols}, COUNT(*) AS rows{extra_data_error} "
        f"FROM {src} GROUP BY {select_cols} ORDER BY {select_cols}"
    )
    df = con.execute(sql).pl()
    return df, sql


def _target_distribution(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    targets = [t for t in spec.target_columns if t in snapshot.columns]
    if not targets:
        sql = (
            f"SELECT 'no_targets' AS target, NULL::VARCHAR AS slice_kind, "
            f"NULL::VARCHAR AS slice_value, COUNT(*) AS rows, "
            f"NULL::DOUBLE AS observed_rate FROM {src}"
        )
        return con.execute(sql).pl(), sql
    parts: list[str] = []
    if "primary_fold" in snapshot.columns:
        for target in targets:
            tq = _quote_ident(target)
            parts.append(
                f"SELECT {_quote_literal(target)} AS target, "
                f"'primary_fold' AS slice_kind, "
                f"COALESCE(primary_fold::VARCHAR, '__NULL__') AS slice_value, "
                f"COUNT(*) AS rows, "
                f"AVG(CASE WHEN {tq} IS NOT NULL THEN 1.0 ELSE 0.0 END) AS observed_rate, "
                f"AVG(TRY_CAST({tq} AS DOUBLE)) AS mean_value "
                f"FROM {src} GROUP BY 1,2,3"
            )
    for flag in snapshot.holdout_flag_columns:
        flag_path = f"holdout_flags.{flag}"
        for target in targets:
            tq = _quote_ident(target)
            parts.append(
                f"SELECT {_quote_literal(target)} AS target, "
                f"{_quote_literal(flag)} AS slice_kind, "
                f"COALESCE(({flag_path})::VARCHAR, '__NULL__') AS slice_value, "
                f"COUNT(*) AS rows, "
                f"AVG(CASE WHEN {tq} IS NOT NULL THEN 1.0 ELSE 0.0 END) AS observed_rate, "
                f"AVG(TRY_CAST({tq} AS DOUBLE)) AS mean_value "
                f"FROM {src} GROUP BY 1,2,3"
            )
    if not parts:
        for target in targets:
            tq = _quote_ident(target)
            parts.append(
                f"SELECT {_quote_literal(target)} AS target, "
                f"'__all__' AS slice_kind, '__all__' AS slice_value, "
                f"COUNT(*) AS rows, "
                f"AVG(CASE WHEN {tq} IS NOT NULL THEN 1.0 ELSE 0.0 END) AS observed_rate, "
                f"AVG(TRY_CAST({tq} AS DOUBLE)) AS mean_value FROM {src}"
            )
    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY target, slice_kind, slice_value"
    df = con.execute(sql).pl()
    return df, sql


def _source_family_block_missingness(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    if "source_acquisition_status" not in snapshot.columns:
        sql = (
            f"SELECT NULL::VARCHAR AS source_family, NULL::VARCHAR AS season, "
            f"0::BIGINT AS rows, 0::BIGINT AS source_block_missing_rows, "
            f"0::BIGINT AS event_present_block_missing_rows FROM {src} WHERE FALSE"
        )
        return con.execute(sql).pl(), sql
    cols = []
    if "source_family" in snapshot.columns:
        cols.append("source_family")
    if "season" in snapshot.columns:
        cols.append("season")
    if not cols:
        cols.append("'__all__' AS slice_value")
    select_cols = ", ".join(cols)
    group_cols = ", ".join(c if " AS " not in c else c.split(" AS ")[-1] for c in cols)
    event_present_clause = (
        "model_input_eligible IS TRUE OR model_input_eligible IS NULL"
        if "model_input_eligible" in snapshot.columns
        else "TRUE"
    )
    sql = (
        f"SELECT {select_cols}, COUNT(*) AS rows, "
        f"COUNT_IF(source_acquisition_status = 'not_acquired') AS source_block_missing_rows, "
        f"COUNT_IF(source_acquisition_status = 'not_acquired' AND ({event_present_clause})) "
        f"AS event_present_block_missing_rows "
        f"FROM {src} GROUP BY {group_cols} ORDER BY {group_cols}"
    )
    df = con.execute(sql).pl()
    return df, sql


def _data_error_concentration(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    if "data_error_risk" not in snapshot.columns:
        sql = (
            f"SELECT NULL::VARCHAR AS dimension, NULL::VARCHAR AS slice_value, "
            f"NULL::VARCHAR AS data_error_risk, 0::BIGINT AS rows FROM {src} WHERE FALSE"
        )
        return con.execute(sql).pl(), sql
    candidates = ("scorer", "source_family", "season", "park_id")
    dims = [c for c in candidates if c in snapshot.columns]
    if not dims:
        sql = (
            f"SELECT '__all__' AS dimension, '__all__' AS slice_value, "
            f"data_error_risk, COUNT(*) AS rows FROM {src} GROUP BY 1,2,3"
        )
        return con.execute(sql).pl(), sql
    parts: list[str] = []
    for dim in dims:
        dq = _quote_ident(dim)
        parts.append(
            f"SELECT {_quote_literal(dim)} AS dimension, "
            f"COALESCE({dq}::VARCHAR, '__NULL__') AS slice_value, "
            f"COALESCE(data_error_risk, '__NULL__') AS data_error_risk, "
            f"COUNT(*) AS rows FROM {src} GROUP BY 1,2,3"
        )
    sql = (
        "\nUNION ALL\n".join(parts)
        + "\nORDER BY dimension, slice_value, data_error_risk"
    )
    return con.execute(sql).pl(), sql


def _connectivity_edges(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    edges: list[str] = []
    if _has_columns(snapshot, "park_id", "batter_id", "pitcher_id", "season", "league"):
        edges.append(
            f"SELECT 'park_park' AS edge_kind, "
            f"a.park_id::VARCHAR AS from_node, b.park_id::VARCHAR AS to_node, "
            f"a.season::VARCHAR AS season, a.league AS league, COUNT(*) AS shared_rows "
            f"FROM {src} AS a INNER JOIN {src} AS b "
            f"ON a.batter_id = b.batter_id AND a.pitcher_id = b.pitcher_id "
            f"AND a.season = b.season AND a.league = b.league "
            f"AND a.park_id IS DISTINCT FROM b.park_id "
            f"GROUP BY 1,2,3,4,5"
        )
    if _has_columns(snapshot, "scorer", "park_id"):
        edges.append(
            f"SELECT 'scorer_park' AS edge_kind, "
            f"COALESCE(scorer, '__NULL__') AS from_node, "
            f"COALESCE(park_id::VARCHAR, '__NULL__') AS to_node, "
            f"NULL::VARCHAR AS season, NULL::VARCHAR AS league, COUNT(*) AS shared_rows "
            f"FROM {src} GROUP BY 1,2,3,4,5"
        )
    if _has_columns(snapshot, "source_family", "season"):
        edges.append(
            f"SELECT 'source_family_season' AS edge_kind, "
            f"COALESCE(source_family, '__NULL__') AS from_node, "
            f"season::VARCHAR AS to_node, "
            f"NULL::VARCHAR AS season, NULL::VARCHAR AS league, COUNT(*) AS shared_rows "
            f"FROM {src} GROUP BY 1,2,3,4,5"
        )
    if not edges:
        sql = (
            f"SELECT NULL::VARCHAR AS edge_kind, NULL::VARCHAR AS from_node, "
            f"NULL::VARCHAR AS to_node, NULL::VARCHAR AS season, "
            f"NULL::VARCHAR AS league, 0::BIGINT AS shared_rows FROM {src} WHERE FALSE"
        )
        return con.execute(sql).pl(), sql
    sql = "\nUNION ALL\n".join(edges) + "\nORDER BY edge_kind, from_node, to_node"
    return con.execute(sql).pl(), sql


def _collinearity_report(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, spec: DatasetSpec
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    pairs: list[tuple[str, str, str]] = []
    if _has_columns(snapshot, "scorer", "park_id"):
        pairs.append(("scorer_park", "scorer", "park_id"))
    if _has_columns(snapshot, "source_family", "season"):
        pairs.append(("source_family_era", "source_family", "season"))
    if _has_columns(snapshot, "park_id", "result_family"):
        pairs.append(("park_result", "park_id", "result_family"))
    if _has_columns(snapshot, "alignment_regime", "batter_hand"):
        pairs.append(("alignment_handedness", "alignment_regime", "batter_hand"))
    if _has_columns(snapshot, "scorer", "source_family"):
        pairs.append(("scorer_source_family", "scorer", "source_family"))
    if not pairs:
        sql = (
            f"SELECT NULL::VARCHAR AS pair_kind, NULL::VARCHAR AS left_value, "
            f"NULL::VARCHAR AS right_value, 0::BIGINT AS rows, "
            f"0.0::DOUBLE AS dominant_share FROM {src} WHERE FALSE"
        )
        return con.execute(sql).pl(), sql
    parts: list[str] = []
    for tag, left, right in pairs:
        lq = _quote_ident(left)
        rq = _quote_ident(right)
        if tag == "source_family_era":
            era_bin = f"((CAST({rq} AS BIGINT) / 5) * 5)::VARCHAR"
            parts.append(
                f"SELECT {_quote_literal(tag)} AS pair_kind, "
                f"COALESCE({lq}::VARCHAR, '__NULL__') AS left_value, "
                f"{era_bin} AS right_value, "
                f"COUNT(*) AS rows, "
                f"COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER (PARTITION BY {era_bin}) AS dominant_share "
                f"FROM {src} WHERE {lq} IS NOT NULL AND {rq} IS NOT NULL GROUP BY 1,2,3"
            )
        else:
            parts.append(
                f"SELECT {_quote_literal(tag)} AS pair_kind, "
                f"COALESCE({lq}::VARCHAR, '__NULL__') AS left_value, "
                f"COALESCE({rq}::VARCHAR, '__NULL__') AS right_value, "
                f"COUNT(*) AS rows, "
                f"COUNT(*)::DOUBLE / SUM(COUNT(*)) OVER (PARTITION BY COALESCE({lq}::VARCHAR, '__NULL__')) AS dominant_share "
                f"FROM {src} GROUP BY 1,2,3"
            )
    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY pair_kind, dominant_share DESC"
    return con.execute(sql).pl(), sql


def _candidate_interaction_specs(
    snapshot: _Snapshot, spec: DatasetSpec
) -> list[_InteractionScan]:
    targets = [t for t in spec.target_columns if t in snapshot.columns]
    candidates: list[tuple[str, str]] = [
        ("season", "source_family"),
        ("scorer", "result_family"),
        ("scorer", "source_family"),
        ("park_id", "scorer"),
        ("batter_hand", "alignment_regime"),
        ("base_state_start", "outs_start"),
        ("park_id", "batter_hand"),
        ("game_type", "denominator_policy"),
        ("source_family", "park_id"),
    ]
    out: list[_InteractionScan] = []
    for left, right in candidates:
        if not _has_columns(snapshot, left, right):
            continue
        for target in targets:
            out.append(_InteractionScan(left=left, right=right, target=target))
    return out


def _candidate_interactions(
    con: duckdb.DuckDBPyConnection,
    snapshot: _Snapshot,
    spec: DatasetSpec,
    thresholds: EdaThresholds,
) -> tuple[pl.DataFrame, str]:
    src = _from_clause(snapshot)
    scans = _candidate_interaction_specs(snapshot, spec)
    if not scans:
        sql = (
            f"SELECT NULL::VARCHAR AS left_col, NULL::VARCHAR AS right_col, "
            f"NULL::VARCHAR AS target, NULL::VARCHAR AS left_value, NULL::VARCHAR AS right_value, "
            f"0::BIGINT AS rows, 0.0::DOUBLE AS target_rate, 0.0::DOUBLE AS abs_lift "
            f"FROM {src} WHERE FALSE"
        )
        return con.execute(sql).pl(), sql
    parts: list[str] = []
    for scan in scans:
        lq = _quote_ident(scan.left)
        rq = _quote_ident(scan.right)
        tq = _quote_ident(scan.target)
        target_expr = f"AVG(TRY_CAST({tq} AS DOUBLE))"
        baseline_expr = f"(SELECT AVG(TRY_CAST({tq} AS DOUBLE)) FROM {src})"
        parts.append(
            f"SELECT * FROM (SELECT {_quote_literal(scan.left)} AS left_col, "
            f"{_quote_literal(scan.right)} AS right_col, "
            f"{_quote_literal(scan.target)} AS target, "
            f"COALESCE({lq}::VARCHAR, '__NULL__') AS left_value, "
            f"COALESCE({rq}::VARCHAR, '__NULL__') AS right_value, "
            f"COUNT(*) AS rows, "
            f"{target_expr} AS target_rate, "
            f"ABS({target_expr} - {baseline_expr}) AS abs_lift "
            f"FROM {src} GROUP BY 1,2,3,4,5 "
            f"HAVING COUNT(*) >= {thresholds.interaction_min_rows} "
            f"AND ABS({target_expr} - {baseline_expr}) >= {thresholds.interaction_min_lift})"
        )
    sql = "\nUNION ALL\n".join(parts) + "\nORDER BY abs_lift DESC"
    df = con.execute(sql).pl()
    return df, sql


def _weak_identification_flags(
    edges: pl.DataFrame,
    collinearity: pl.DataFrame,
    target_distribution: pl.DataFrame,
    snapshot: _Snapshot,
    thresholds: EdaThresholds,
) -> tuple[pl.DataFrame, tuple[WeakIdentificationFlag, ...]]:
    flags: list[WeakIdentificationFlag] = []
    if collinearity.height > 0 and "dominant_share" in collinearity.columns:
        dominant = collinearity.filter(
            (pl.col("dominant_share") >= thresholds.weak_identification_share)
            & (pl.col("rows") >= thresholds.dominant_min_rows)
        )
        for row in dominant.iter_rows(named=True):
            flags.append(
                WeakIdentificationFlag(
                    effect=str(row["pair_kind"]),
                    slice=f"{row['left_value']}|{row['right_value']}",
                    share=float(row["dominant_share"]),
                    reason="collinearity_dominant_share",
                )
            )
    if edges.height > 0 and "edge_kind" in edges.columns:
        per_kind = edges.group_by("edge_kind").agg(
            pl.col("from_node").n_unique().alias("nodes")
        )
        for row in per_kind.iter_rows(named=True):
            if int(row["nodes"]) <= 1:
                flags.append(
                    WeakIdentificationFlag(
                        effect=str(row["edge_kind"]),
                        slice="__global__",
                        share=1.0,
                        reason="single_node_component",
                    )
                )
    if target_distribution.height > 0 and {
        "slice_kind",
        "slice_value",
        "rows",
    }.issubset(target_distribution.columns):
        for flag_name in snapshot.holdout_flag_columns:
            per_flag = target_distribution.filter(pl.col("slice_kind") == flag_name)
            if per_flag.height == 0:
                continue
            first_target = per_flag.select(pl.col("target").first()).item()
            slice_df = per_flag.filter(pl.col("target") == first_target)
            total_rows = int(slice_df.select(pl.col("rows").sum()).item())
            if total_rows == 0:
                continue
            heldout = slice_df.filter(pl.col("slice_value") == "true")
            if heldout.height == 0:
                continue
            heldout_rows = int(heldout.select(pl.col("rows").sum()).item())
            share = heldout_rows / total_rows
            if share >= thresholds.weak_identification_share:
                flags.append(
                    WeakIdentificationFlag(
                        effect=flag_name,
                        slice="holdout_true",
                        share=share,
                        reason="holdout_dominates_split",
                    )
                )
    rows: list[dict[str, object]] = [
        {
            "effect": f.effect,
            "slice": f.slice,
            "share": f.share,
            "reason": f.reason,
        }
        for f in flags
    ]
    df = pl.DataFrame(
        rows,
        schema={
            "effect": pl.Utf8,
            "slice": pl.Utf8,
            "share": pl.Float64,
            "reason": pl.Utf8,
        },
    )
    return df, tuple(flags)


def _derive_blocking_findings(
    *,
    snapshot: _Snapshot,
    summary: tuple[int, int, int, int, int],
    block_missing: pl.DataFrame,
    collinearity: pl.DataFrame,
    edges: pl.DataFrame,
    target_distribution: pl.DataFrame,
    con: duckdb.DuckDBPyConnection,
    thresholds: EdaThresholds,
    output_paths: dict[str, Path],
) -> list[BlockingFinding]:
    findings: list[BlockingFinding] = []
    src = _from_clause(snapshot)

    (_, _, _, source_block_missing_count, _) = summary
    if (
        "source_acquisition_status" in snapshot.columns
        and source_block_missing_count > 0
        and "model_input_eligible" in snapshot.columns
    ):
        bad_rows = _scalar_int(
            con,
            f"SELECT COUNT(*) FROM {src} WHERE source_acquisition_status = 'not_acquired' "
            f"AND model_input_eligible IS TRUE",
        )
        if bad_rows > 0:
            findings.append(
                BlockingFinding(
                    code="source_family_block_as_event_missing",
                    severity="block",
                    message=(
                        f"{bad_rows} rows mark source_acquisition_status='not_acquired' "
                        "but remain model_input_eligible=TRUE; source-block missingness "
                        "is leaking through as event-level missingness."
                    ),
                    evidence_path=output_paths.get("source_family_block_missingness"),
                )
            )

    if collinearity.height > 0 and "dominant_share" in collinearity.columns:
        dominant = collinearity.filter(
            (pl.col("dominant_share") >= thresholds.dominant_share)
            & (pl.col("rows") >= thresholds.dominant_min_rows)
        )
        if dominant.height > 0:
            top = dominant.sort("dominant_share", descending=True).row(0, named=True)
            findings.append(
                BlockingFinding(
                    code="dominant_single_scorer_park_team",
                    severity="warn",
                    message=(
                        f"pair {top['pair_kind']} has dominant share "
                        f"{float(top['dominant_share']):.3f} for "
                        f"({top['left_value']}, {top['right_value']}) over {int(top['rows'])} rows."
                    ),
                    evidence_path=output_paths.get("collinearity_report"),
                )
            )

    if edges.height > 0 and "edge_kind" in edges.columns:
        per_kind = edges.group_by("edge_kind").agg(
            pl.col("from_node").n_unique().alias("nodes")
        )
        singletons = per_kind.filter(pl.col("nodes") <= 1)
        if singletons.height > 0:
            kinds = ", ".join(str(k) for k in singletons["edge_kind"].to_list())
            findings.append(
                BlockingFinding(
                    code="no_connected_component_for_effect",
                    severity="warn",
                    message=f"edge kinds with single-node components: {kinds}.",
                    evidence_path=output_paths.get("connectivity_edges"),
                )
            )

    if "data_error_risk" in snapshot.columns:
        offenders = _scalar_int(
            con,
            f"SELECT COUNT(*) FROM {src} WHERE data_error_risk IS NOT NULL "
            f"AND data_error_risk != 'none' AND training_weight > 0",
        )
        if offenders > 0:
            findings.append(
                BlockingFinding(
                    code="data_error_rows_train_as_truth",
                    severity="block",
                    message=(
                        f"{offenders} rows carry data_error_risk != 'none' and "
                        "training_weight > 0; training treats data-error rows as truth."
                    ),
                    evidence_path=output_paths.get("data_error_concentration"),
                )
            )

    if "primary_fold" in snapshot.columns:
        train_categories = _category_set(con, snapshot, fold_value="TRAIN")
        for column, train_set in train_categories.items():
            test_set = _category_set_for_column(
                con, snapshot, column=column, fold_value="TEST"
            )
            new_in_test = test_set - train_set
            if new_in_test:
                sample = sorted(new_in_test)[:5]
                findings.append(
                    BlockingFinding(
                        code="category_absent_in_train_present_in_test",
                        severity="warn",
                        message=(
                            f"column {column!r} has values in TEST that are absent from "
                            f"TRAIN (e.g. {sample})."
                        ),
                        evidence_path=output_paths.get("target_distribution"),
                    )
                )

    return findings


_SPLIT_LEAKAGE_CATEGORICAL_DIMENSIONS: tuple[str, ...] = (
    "source_family",
    "park_id",
    "scorer",
    "league",
    "alignment_regime",
)


def _category_set(
    con: duckdb.DuckDBPyConnection, snapshot: _Snapshot, *, fold_value: str
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for column in _SPLIT_LEAKAGE_CATEGORICAL_DIMENSIONS:
        if column in snapshot.columns:
            out[column] = _category_set_for_column(
                con, snapshot, column=column, fold_value=fold_value
            )
    return out


def _category_set_for_column(
    con: duckdb.DuckDBPyConnection,
    snapshot: _Snapshot,
    *,
    column: str,
    fold_value: str,
) -> set[str]:
    src = _from_clause(snapshot)
    cq = _quote_ident(column)
    rows = con.execute(
        f"SELECT DISTINCT {cq}::VARCHAR FROM {src} WHERE primary_fold = ? AND {cq} IS NOT NULL",
        [fold_value],
    ).fetchall()
    return {str(r[0]) for r in rows}


def _render_markdown(
    report: EdaReport,
    *,
    path: Path,
    spec: DatasetSpec,
) -> None:
    lines: list[str] = []
    lines.append(f"# EDA — {report.dataset_name} ({report.dataset_version})")
    lines.append("")
    lines.append(f"- artifact_id: `{report.dataset_artifact_id}`")
    lines.append(f"- source_snapshot_id: `{report.source_snapshot_id}`")
    lines.append(f"- row_count: {report.row_count:,}")
    lines.append(f"- target_population_count: {report.target_population_count:,}")
    lines.append(f"- observed_truth_count: {report.observed_truth_count:,}")
    lines.append(
        f"- source_family_block_missing_count: {report.source_family_block_missing_count:,}"
    )
    lines.append(f"- data_error_excluded_count: {report.data_error_excluded_count:,}")
    lines.append("")
    lines.append("## Module outputs")
    for name, p in report.module_paths.items():
        lines.append(f"- `{name}` → `{p.name}`")
    lines.append("")
    lines.append("## Blocking findings")
    if report.blocking_findings:
        for f in report.blocking_findings:
            lines.append(f"- **{f.code}** ({f.severity}): {f.message}")
    else:
        lines.append("- _none_")
    lines.append("")
    lines.append("## Weak-identification flags")
    if report.weak_identification_flags:
        for f in report.weak_identification_flags:
            lines.append(
                f"- **{f.effect}** ({f.reason}) slice=`{f.slice}` share={f.share:.3f}"
            )
    else:
        lines.append("- _none_")
    lines.append("")
    lines.append(f"_targets analyzed_: {', '.join(spec.target_columns) or '(none)'}")
    lines.append("")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=".eda.", suffix=".md")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("\n".join(lines))
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _write_report_atomic(report: EdaReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=".report.", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(report.model_dump_json(indent=2))
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _resolve_dataset_artifact_root(
    spec: DatasetSpec, *, dataset_artifact_id: str, dataset_artifact_root: Path | None
) -> Path:
    root = dataset_artifact_root if dataset_artifact_root is not None else DATASETS_ROOT
    return root / spec.name / dataset_artifact_id


def _resolve_eda_artifact_root(
    spec: DatasetSpec, *, artifact_id: str, output_root: Path | None
) -> Path:
    root = output_root if output_root is not None else EDA_ROOT
    return root / spec.name / artifact_id


def _composite_query_hash(module_sql: dict[str, str]) -> str:
    blob = "\n---\n".join(f"{name}:\n{sql}" for name, sql in sorted(module_sql.items()))
    return query_hash(blob)


def _verify_rerun_metadata(
    *,
    existing_report_path: Path,
    new_source_snapshot_id: str,
    new_dataset_artifact_id: str,
) -> EdaReport:
    existing = EdaReport.model_validate_json(
        existing_report_path.read_text(encoding="utf-8")
    )
    if existing.source_snapshot_id != new_source_snapshot_id:
        raise ValueError(
            "rerun of existing EDA artifact_id with a different source_snapshot_id "
            f"is forbidden ({existing.source_snapshot_id!r} on disk vs "
            f"{new_source_snapshot_id!r} requested)."
        )
    if existing.dataset_artifact_id != new_dataset_artifact_id:
        raise ValueError(
            "rerun of existing EDA artifact_id pointed at a different dataset "
            f"artifact ({existing.dataset_artifact_id!r} on disk vs "
            f"{new_dataset_artifact_id!r} requested)."
        )
    return existing


def run_eda(
    spec: DatasetSpec,
    *,
    dataset_artifact_id: str,
    artifact_id: str,
    output_root: Path | None = None,
    dataset_artifact_root: Path | None = None,
    thresholds: EdaThresholds | None = None,
    artifact_versions: dict[str, str] | None = None,
) -> ArtifactManifest:
    """Run EDA against a frozen dataset snapshot, write artifact directory.

    Reads ``<dataset_artifact_root>/<spec.name>/<dataset_artifact_id>/dataset.parquet``
    via DuckDB ``read_parquet`` and writes per-module Parquet outputs +
    ``report.json`` + ``manifest.json`` + ``eda.md`` under
    ``<output_root>/<spec.name>/<artifact_id>/``.

    Idempotent on ``artifact_id``: rerun returns the existing manifest
    when the dataset's source_snapshot_id still matches; mismatch raises.
    """
    thresholds = thresholds if thresholds is not None else EdaThresholds()
    dataset_root = _resolve_dataset_artifact_root(
        spec,
        dataset_artifact_id=dataset_artifact_id,
        dataset_artifact_root=dataset_artifact_root,
    )
    parquet_path = dataset_root / "dataset.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {parquet_path}; run prepare-dataset first."
        )
    eda_root = _resolve_eda_artifact_root(
        spec, artifact_id=artifact_id, output_root=output_root
    )
    eda_root.mkdir(parents=True, exist_ok=True)
    manifest_path = eda_root / "manifest.json"
    report_path = eda_root / "report.json"
    md_path = eda_root / "eda.md"

    output_paths: dict[str, Path] = {
        name: eda_root / filename for name, filename in _MODULE_FILES.items()
    }

    _log.info(
        "run_eda start name=%s artifact_id=%s dataset_artifact_id=%s parquet=%s",
        spec.name,
        artifact_id,
        dataset_artifact_id,
        parquet_path,
    )

    con = duckdb.connect(":memory:")
    try:
        snapshot = _open_snapshot(con, parquet_path)
        source_snapshot_id = _resolve_source_snapshot_id(con, snapshot)

        if report_path.exists() and manifest_path.exists():
            existing = _verify_rerun_metadata(
                existing_report_path=report_path,
                new_source_snapshot_id=source_snapshot_id,
                new_dataset_artifact_id=dataset_artifact_id,
            )
            _log.info(
                "run_eda verified existing artifact_id=%s row_count=%d",
                artifact_id,
                existing.row_count,
            )
            return read_manifest(manifest_path)

        summary = _dataset_summary(con, snapshot, spec)
        (
            row_count,
            target_population_count,
            observed_truth_count,
            source_family_block_missing_count,
            data_error_excluded_count,
        ) = summary

        module_sql: dict[str, str] = {}

        miss_df, miss_sql = _missingness_by_slice(con, snapshot, spec)
        module_sql["missingness_by_slice"] = miss_sql
        write_parquet_atomic(miss_df, output_paths["missingness_by_slice"])

        target_df, target_sql = _target_distribution(con, snapshot, spec)
        module_sql["target_distribution"] = target_sql
        write_parquet_atomic(target_df, output_paths["target_distribution"])

        block_df, block_sql = _source_family_block_missingness(con, snapshot, spec)
        module_sql["source_family_block_missingness"] = block_sql
        write_parquet_atomic(block_df, output_paths["source_family_block_missingness"])

        derr_df, derr_sql = _data_error_concentration(con, snapshot, spec)
        module_sql["data_error_concentration"] = derr_sql
        write_parquet_atomic(derr_df, output_paths["data_error_concentration"])

        edges_df, edges_sql = _connectivity_edges(con, snapshot, spec)
        module_sql["connectivity_edges"] = edges_sql
        write_parquet_atomic(edges_df, output_paths["connectivity_edges"])

        col_df, col_sql = _collinearity_report(con, snapshot, spec)
        module_sql["collinearity_report"] = col_sql
        write_parquet_atomic(col_df, output_paths["collinearity_report"])

        inter_df, inter_sql = _candidate_interactions(con, snapshot, spec, thresholds)
        module_sql["candidate_interactions"] = inter_sql
        write_parquet_atomic(inter_df, output_paths["candidate_interactions"])

        weak_df, weak_flags = _weak_identification_flags(
            edges_df, col_df, target_df, snapshot, thresholds
        )
        module_sql["weak_identification_flags"] = "derived_in_python"
        write_parquet_atomic(weak_df, output_paths["weak_identification_flags"])

        leakage_violations = check_split_leakage(parquet_path, con=con)
        leakage_df = violations_to_dataframe(leakage_violations)
        module_sql["split_leakage_report"] = "derived_in_python"
        write_parquet_atomic(leakage_df, output_paths["split_leakage_report"])

        blocking = _derive_blocking_findings(
            snapshot=snapshot,
            summary=summary,
            block_missing=block_df,
            collinearity=col_df,
            edges=edges_df,
            target_distribution=target_df,
            con=con,
            thresholds=thresholds,
            output_paths=output_paths,
        )

        if leakage_violations:
            leakage_summary = summarize_violations(leakage_violations)
            parts = ", ".join(
                f"{kind}={count}" for kind, count in sorted(leakage_summary.items())
            )
            blocking.append(
                BlockingFinding(
                    code="split_leakage_detected",
                    severity="block",
                    message=(
                        f"split-registry leakage detected across "
                        f"{len(leakage_violations)} unit(s): {parts}. "
                        "See split_leakage_report.parquet for offending unit_ids."
                    ),
                    evidence_path=output_paths["split_leakage_report"],
                )
            )

        composite_hash = _composite_query_hash(module_sql)

        report = EdaReport(
            dataset_name=spec.name,
            dataset_version=spec.dataset_version,
            dataset_artifact_id=dataset_artifact_id,
            source_snapshot_id=source_snapshot_id,
            row_count=row_count,
            target_population_count=target_population_count,
            observed_truth_count=observed_truth_count,
            source_family_block_missing_count=source_family_block_missing_count,
            data_error_excluded_count=data_error_excluded_count,
            module_paths=output_paths,
            blocking_findings=tuple(blocking),
            weak_identification_flags=weak_flags,
        )
        _write_report_atomic(report, report_path)
        _render_markdown(report, path=md_path, spec=spec)

        manifest_outputs: dict[str, Path] = {"report": report_path, "markdown": md_path}
        manifest_outputs.update(output_paths)

        manifest = ArtifactManifest(
            artifact_id=artifact_id,
            kind="eda",
            name=spec.name,
            version=spec.dataset_version,
            created_at=utc_now(),
            source_snapshot_id=source_snapshot_id,
            query_hash=composite_hash,
            dataset_artifact_id=dataset_artifact_id,
            output_paths=manifest_outputs,
            package_versions=artifact_versions
            if artifact_versions is not None
            else snapshot_package_versions(),
            blocking_findings=tuple(_blocking_codes(blocking)),
            metadata={
                "row_count": row_count,
                "target_population_count": target_population_count,
                "observed_truth_count": observed_truth_count,
                "source_family_block_missing_count": source_family_block_missing_count,
                "data_error_excluded_count": data_error_excluded_count,
                "weak_identification_flag_count": len(weak_flags),
                "blocking_finding_count": len(blocking),
            },
        )
        write_manifest(manifest, manifest_path)
        _log.info(
            "run_eda done name=%s artifact_id=%s row_count=%d blocking=%d weak=%d",
            spec.name,
            artifact_id,
            row_count,
            len(blocking),
            len(weak_flags),
        )
        return manifest
    finally:
        con.close()


def _blocking_codes(findings: Sequence[BlockingFinding]) -> list[str]:
    return [f.code for f in findings]
