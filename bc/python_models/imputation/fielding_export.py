from __future__ import annotations

import logging
from pathlib import Path
from typing import Mapping

import duckdb

from python_models.imputation.artifacts import (
    ComponentArtifact,
    export_component,
    sql_literal,
)
from python_models.imputation.fielding_allocation import (
    build_compatible_assignment_patch,
)

_log = logging.getLogger(__name__)


def export_fielding_component(
    connection: duckdb.DuckDBPyConnection,
    output: Path,
    query: str,
    schema: Mapping[str, str],
) -> ComponentArtifact:
    raw = export_component(
        connection, output, name="fielding_raw", query=query, expected_columns=schema
    )
    source = f"read_parquet({sql_literal(raw.data.path)})"
    unknown = connection.execute(
        f"SELECT * FROM {source} WHERE raw_fielding_position = 0"
    ).pl()
    _log.info("Reconciling %s unknown fielding credits", unknown.height)
    patch = build_compatible_assignment_patch(unknown)
    patch_path = output / "fielding_assignment.parquet"
    patch.write_parquet(patch_path)
    replacements = ", ".join(
        f'CASE WHEN p.event_key IS NOT NULL THEN p."{column}" ELSE r."{column}" END AS "{column}"'
        for column in patch.columns
        if column not in {"event_key", "sequence_id", "allocation_applied"}
    )
    completed = (
        f"SELECT r.* REPLACE ({replacements}) FROM {source} r "
        f"LEFT JOIN read_parquet({sql_literal(str(patch_path.resolve()))}) p USING(event_key,sequence_id)"
    )
    _log.info("Fielding allocation reviewed %s rows", patch.height)
    return export_component(
        connection,
        output,
        name="fielding",
        query=completed,
        expected_columns=schema,
        dependencies=(Path(raw.data.path), Path(raw.query.path), patch_path),
    )
