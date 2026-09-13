from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Mapping, Sequence

import duckdb
from pydantic import BaseModel, ConfigDict, Field

_log = logging.getLogger(__name__)


class FileIdentity(BaseModel):
    model_config = ConfigDict(frozen=True)

    path: str
    bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ComponentArtifact(BaseModel):
    model_config = ConfigDict(frozen=True)

    name: str
    rows: int = Field(ge=0)
    data: FileIdentity
    query: FileIdentity
    columns: dict[str, str]
    dependencies: tuple[FileIdentity, ...] = ()
    confidence_status: str = "exploratory"


def file_identity(path: Path) -> FileIdentity:
    digest = hashlib.sha256()
    size = 0
    next_checkpoint = 4 * 1024**3
    with path.open("rb") as stream:
        while chunk := stream.read(8 * 1024**2):
            digest.update(chunk)
            size += len(chunk)
            if size >= next_checkpoint:
                _log.info("Fingerprinting %s: %.1f GiB", path.name, size / 1024**3)
                next_checkpoint += 4 * 1024**3
    return FileIdentity(path=str(path.resolve()), bytes=size, sha256=digest.hexdigest())


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def export_component(
    connection: duckdb.DuckDBPyConnection,
    output_root: Path,
    *,
    name: str,
    query: str,
    expected_columns: Mapping[str, str],
    dependencies: Sequence[Path] = (),
) -> ComponentArtifact:
    if not name.replace("_", "").isalnum():
        raise ValueError("Invalid component name")
    data_path = output_root / f"{name}.parquet"
    query_path = output_root / f"{name}.sql"
    if data_path.exists() or query_path.exists():
        raise FileExistsError(f"Component already exists: {name}")
    output_root.mkdir(parents=True, exist_ok=True)
    query_path.write_text(query, encoding="utf-8")
    _log.info("Building %s", name)
    description = connection.execute(f"DESCRIBE ({query})").fetchall()
    actual_columns = {str(row[0]): str(row[1]) for row in description}
    if set(actual_columns) != set(expected_columns):
        raise ValueError(
            f"Schema mismatch for {name}: missing={set(expected_columns) - set(actual_columns)}, "
            f"extra={set(actual_columns) - set(expected_columns)}"
        )
    mismatched_types = {
        name: (actual_columns[name], kind)
        for name, kind in expected_columns.items()
        if actual_columns[name] != kind
    }
    if mismatched_types:
        raise ValueError(f"Schema type mismatch for {name}: {mismatched_types}")
    destination = sql_literal(str(data_path.resolve()))
    connection.execute(
        f"COPY ({query}) TO {destination} (FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    count_result = connection.execute(
        f"SELECT count(*) FROM read_parquet({destination})"
    ).fetchone()
    if count_result is None:
        raise ValueError(f"Missing row count for {name}")
    result = ComponentArtifact(
        name=name,
        rows=int(count_result[0]),
        data=file_identity(data_path),
        query=file_identity(query_path),
        columns=actual_columns,
        dependencies=tuple(file_identity(path) for path in dependencies),
    )
    (output_root / f"{name}.json").write_text(result.model_dump_json(indent=2) + "\n")
    _log.info(
        "Built %s: %s rows, %.1f MiB", name, result.rows, result.data.bytes / 1024**2
    )
    return result


def verify_component(artifact: ComponentArtifact) -> None:
    for expected in (artifact.data, artifact.query, *artifact.dependencies):
        actual = file_identity(Path(expected.path))
        if actual != expected:
            raise ValueError(f"Artifact content changed: {expected.path}")


def write_json(path: Path, payload: Mapping[str, object]) -> None:
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
