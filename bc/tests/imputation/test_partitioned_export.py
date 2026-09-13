from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

import duckdb

from python_models.imputation.context import ContextCompletionConfig
from python_models.imputation.partitioned_export import export_partitioned_component


SCHEMA: Mapping[str, str] = {
    "row_id": "INTEGER",
    "season": "INTEGER",
    "raw_value": "INTEGER",
    "completed_value": "INTEGER",
}


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE source_values (row_id INTEGER, season INTEGER, raw_value INTEGER)"
    )
    connection.executemany(
        "INSERT INTO source_values VALUES (?, ?, ?)",
        (
            (1, 1903, 10),
            (2, 1904, None),
            (3, 1905, 30),
            (4, 1907, None),
            (5, 1908, 50),
        ),
    )
    return connection


def _query_factory(
    name: str, config: ContextCompletionConfig
) -> tuple[str, Mapping[str, str]]:
    assert name == "example"
    return (
        f"""
        SELECT row_id, season, raw_value,
            coalesce(raw_value, 0)::INTEGER AS completed_value
        FROM source_values
        WHERE season BETWEEN {config.start_season} AND {config.end_season}
        """,
        SCHEMA,
    )


def _config() -> ContextCompletionConfig:
    return ContextCompletionConfig(start_season=1903, end_season=1908)


def test_partitioned_export_equals_unpartitioned_and_preserves_source(
    tmp_path: Path,
) -> None:
    dependency = tmp_path / "builder.py"
    dependency.write_text("version = 1\n", encoding="utf-8")
    with _connection() as connection:
        artifact = export_partitioned_component(
            connection,
            tmp_path / "partitioned",
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=2,
        )
        partitioned = connection.execute(
            f"SELECT * FROM read_parquet('{artifact.data.path}') ORDER BY row_id"
        ).fetchall()
        query, _ = _query_factory("example", _config())
        unpartitioned = connection.execute(f"{query} ORDER BY row_id").fetchall()
        source = connection.execute(
            "SELECT * FROM source_values ORDER BY row_id"
        ).fetchall()

    assert partitioned == unpartitioned
    assert [row[:3] for row in partitioned] == source
    assert [row[3] for row in partitioned] == [10, 0, 30, 0, 50]
    parts = tmp_path / "partitioned" / "example_parts"
    assert sorted(path.name for path in parts.iterdir()) == [
        "1903-1904",
        "1905-1906",
        "1907-1908",
    ]
    assert all(
        (part / "attempt-1" / "example.json").exists() for part in parts.iterdir()
    )
    dependency_names = {Path(item.path).name for item in artifact.dependencies}
    assert {"example.parquet", "example.sql", "builder.py"} <= dependency_names
    assert len(artifact.dependencies) == 7


def test_verified_shards_and_final_artifact_are_reused(tmp_path: Path) -> None:
    dependency = tmp_path / "builder.py"
    dependency.write_text("version = 1\n", encoding="utf-8")
    with _connection() as connection:
        first = export_partitioned_component(
            connection,
            tmp_path,
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=3,
        )
        second = export_partitioned_component(
            connection,
            tmp_path,
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=3,
        )

    assert second == first
    attempts = list((tmp_path / "example_parts").glob("*/attempt-*"))
    assert len(attempts) == 2
    assert all(path.name == "attempt-1" for path in attempts)


def test_incomplete_attempt_is_preserved_and_next_attempt_is_used(
    tmp_path: Path,
) -> None:
    dependency = tmp_path / "builder.py"
    dependency.write_text("version = 1\n", encoding="utf-8")
    incomplete = tmp_path / "example_parts" / "1903-1908" / "attempt-1"
    incomplete.mkdir(parents=True)
    marker = incomplete / "failure.log"
    marker.write_text("interrupted\n", encoding="utf-8")
    with _connection() as connection:
        export_partitioned_component(
            connection,
            tmp_path,
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=10,
        )

    assert marker.read_text(encoding="utf-8") == "interrupted\n"
    assert (
        tmp_path / "example_parts" / "1903-1908" / "attempt-2" / "example.json"
    ).exists()


def test_code_hash_mismatch_rejects_shard_reuse(tmp_path: Path) -> None:
    dependency = tmp_path / "builder.py"
    dependency.write_text("version = 1\n", encoding="utf-8")
    with _connection() as connection:
        export_partitioned_component(
            connection,
            tmp_path,
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=10,
        )
        (tmp_path / "example.parquet").unlink()
        (tmp_path / "example.sql").unlink()
        (tmp_path / "example.json").unlink()
        dependency.write_text("version = 2\n", encoding="utf-8")
        export_partitioned_component(
            connection,
            tmp_path,
            "example",
            _config(),
            _query_factory,
            (dependency,),
            partition_years=10,
        )

    partition = tmp_path / "example_parts" / "1903-1908"
    assert (partition / "attempt-1" / "example.json").exists()
    assert (partition / "attempt-2" / "example.json").exists()
