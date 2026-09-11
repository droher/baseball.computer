from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

GEOMETRY_TARGET_CONTRACT = "geometry-v2-global-side"
CONTRACT_COLUMN = "geometry_target_contract"
GLOBAL_SIDE_LABELS = ("All", "Left", "Middle", "Right")
LEARNED_SIDE_COLUMNS = (
    "dl_artifact_id",
    "dl_p_class",
    "propensity_artifact_id",
    "propensity_p_observed",
)
DATASET_VERSIONS = {
    "model_input_geometry": "0.5.0",
    "model_input_observation_batted_ball": "0.4.0",
}


def require_geometry_contract(
    frame: pl.LazyFrame, *, dimension_column: str, check_classes: bool = False
) -> None:
    columns = frame.collect_schema().names()
    if CONTRACT_COLUMN not in columns:
        raise ValueError(
            f"location_side requires {GEOMETRY_TARGET_CONTRACT}; "
            "legacy datasets contain angle modifiers. Build and freeze a new dataset."
        )
    side = frame.filter(pl.col(dimension_column) == "location_side")
    invalid = (
        side.filter(
            pl.col(CONTRACT_COLUMN).is_null()
            | (pl.col(CONTRACT_COLUMN) != GEOMETRY_TARGET_CONTRACT)
        )
        .limit(1)
        .collect()
    )
    if invalid.height:
        raise ValueError(f"location_side requires {GEOMETRY_TARGET_CONTRACT}")
    learned = [
        pl.col(name).is_not_null() for name in LEARNED_SIDE_COLUMNS if name in columns
    ]
    if learned and side.filter(pl.any_horizontal(learned)).limit(1).collect().height:
        raise ValueError("global-side contract forbids legacy learned covariates")
    if check_classes:
        if "class" not in columns:
            raise ValueError("global-side target requires a separate mapped class")
        bad_labels = (
            side.filter(
                (pl.col("observed_status") == "observed")
                & (
                    pl.col("class").is_null()
                    | ~pl.col("class").is_in(GLOBAL_SIDE_LABELS)
                )
            )
            .limit(1)
            .collect()
        )
        if bad_labels.height:
            raise ValueError("observed global-side class is outside the mapped domain")


def require_geometry_parquet(
    path: Path, *, dimension_column: str, check_classes: bool = False
) -> None:
    require_geometry_contract(
        pl.scan_parquet(path),
        dimension_column=dimension_column,
        check_classes=check_classes,
    )


def require_geometry_relation(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    *,
    dimension_column: str | None = None,
    check_classes: bool = False,
) -> None:
    columns = [
        item[0] for item in con.execute(f"DESCRIBE SELECT * FROM {relation}").fetchall()
    ]
    if CONTRACT_COLUMN not in columns:
        raise ValueError(
            f"{relation} requires {GEOMETRY_TARGET_CONTRACT}; "
            "materialize the corrected SQL models before exporting a new dataset."
        )
    invalid = con.execute(
        f"SELECT 1 FROM {relation} WHERE {CONTRACT_COLUMN} IS DISTINCT FROM ? LIMIT 1",
        [GEOMETRY_TARGET_CONTRACT],
    ).fetchone()
    if invalid is not None:
        raise ValueError(f"{relation} contains a stale geometry target contract")
    if dimension_column is None:
        return
    learned = [
        f"{name} IS NOT NULL" for name in LEARNED_SIDE_COLUMNS if name in columns
    ]
    if learned:
        predicate = " OR ".join(learned)
        if (
            con.execute(
                f"SELECT 1 FROM {relation} WHERE {dimension_column} = 'location_side' "
                f"AND ({predicate}) LIMIT 1"
            ).fetchone()
            is not None
        ):
            raise ValueError("global-side contract forbids legacy learned covariates")
    if check_classes:
        if "class" not in columns or "observed_status" not in columns:
            raise ValueError("global-side target requires class and observed_status")
        placeholders = ", ".join("?" for _ in GLOBAL_SIDE_LABELS)
        if (
            con.execute(
                f"SELECT 1 FROM {relation} WHERE {dimension_column} = 'location_side' "
                f"AND observed_status = 'observed' AND (class IS NULL OR class NOT IN ({placeholders})) LIMIT 1",
                list(GLOBAL_SIDE_LABELS),
            ).fetchone()
            is not None
        ):
            raise ValueError("observed global-side class is outside the mapped domain")
