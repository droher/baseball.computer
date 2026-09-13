from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path

import duckdb

from python_models.imputation.artifacts import (
    ComponentArtifact,
    FileIdentity,
    export_component,
    file_identity,
    sql_literal,
    verify_component,
)
from python_models.imputation.context import ContextCompletionConfig


logger = logging.getLogger(__name__)
QueryFactory = Callable[[str, ContextCompletionConfig], tuple[str, Mapping[str, str]]]


def _attempt_number(path: Path) -> int | None:
    prefix = "attempt-"
    if not path.is_dir() or not path.name.startswith(prefix):
        return None
    suffix = path.name[len(prefix) :]
    return int(suffix) if suffix.isdigit() else None


def _code_identities(paths: Sequence[Path]) -> dict[str, FileIdentity]:
    by_name: dict[str, FileIdentity] = {}
    for path in paths:
        if path.name in by_name:
            raise ValueError(f"Duplicate code dependency basename: {path.name}")
        by_name[path.name] = file_identity(path)
    return by_name


def _artifact_code_matches(
    artifact: ComponentArtifact, current: Mapping[str, FileIdentity]
) -> bool:
    bound = {
        Path(dependency.path).name: dependency
        for dependency in artifact.dependencies
        if Path(dependency.path).name in current
    }
    return set(bound) == set(current) and all(
        bound[name].sha256 == identity.sha256 for name, identity in current.items()
    )


def _query_matches(
    attempt: Path, name: str, artifact: ComponentArtifact, query: str
) -> bool:
    query_path = (
        attempt / "fielding_raw.sql"
        if name == "fielding"
        else Path(artifact.query.path)
    )
    try:
        return query_path.read_text(encoding="utf-8") == query
    except OSError:
        return False


def _load_reusable_shard(
    partition_root: Path,
    name: str,
    query: str,
    schema: Mapping[str, str],
    code: Mapping[str, FileIdentity],
) -> ComponentArtifact | None:
    attempts = sorted(
        (
            (number, path)
            for path in partition_root.glob("attempt-*")
            if (number := _attempt_number(path)) is not None
        ),
        reverse=True,
    )
    for _, attempt in attempts:
        metadata = attempt / f"{name}.json"
        if not metadata.exists():
            logger.info("Skipping incomplete partition attempt %s", attempt)
            continue
        try:
            artifact = ComponentArtifact.model_validate_json(
                metadata.read_text(encoding="utf-8")
            )
            verify_component(artifact)
        except (OSError, ValueError) as error:
            logger.info(
                "Rejecting unverifiable partition attempt %s: %s", attempt, error
            )
            continue
        if artifact.name != name or artifact.columns != dict(schema):
            logger.info("Rejecting schema-mismatched partition attempt %s", attempt)
            continue
        if not _query_matches(attempt, name, artifact, query):
            logger.info("Rejecting SQL-mismatched partition attempt %s", attempt)
            continue
        if not _artifact_code_matches(artifact, code):
            logger.info("Rejecting code-mismatched partition attempt %s", attempt)
            continue
        logger.info("Reusing verified partition attempt %s", attempt)
        return artifact
    return None


def _next_attempt(partition_root: Path) -> Path:
    numbers = [
        number
        for path in partition_root.glob("attempt-*")
        if (number := _attempt_number(path)) is not None
    ]
    return partition_root / f"attempt-{max(numbers, default=0) + 1}"


def _with_code_dependencies(
    artifact: ComponentArtifact, code_dependencies: Sequence[Path]
) -> ComponentArtifact:
    dependencies = (
        *artifact.dependencies,
        *(file_identity(path) for path in code_dependencies),
    )
    updated = artifact.model_copy(update={"dependencies": dependencies})
    metadata = Path(updated.data.path).parent / f"{updated.name}.json"
    metadata.write_text(updated.model_dump_json(indent=2) + "\n", encoding="utf-8")
    return updated


def _export_shard(
    connection: duckdb.DuckDBPyConnection,
    attempt: Path,
    name: str,
    query: str,
    schema: Mapping[str, str],
    code_dependencies: Sequence[Path],
) -> ComponentArtifact:
    if name == "fielding":
        from python_models.imputation.fielding_export import export_fielding_component

        artifact = export_fielding_component(connection, attempt, query, schema)
        return _with_code_dependencies(artifact, code_dependencies)
    return export_component(
        connection,
        attempt,
        name=name,
        query=query,
        expected_columns=schema,
        dependencies=code_dependencies,
    )


def _dependency_paths(shards: Sequence[ComponentArtifact]) -> tuple[Path, ...]:
    ordered: dict[str, Path] = {}
    for shard in shards:
        identities = (shard.data, shard.query, *shard.dependencies)
        for identity in identities:
            path = Path(identity.path).resolve()
            ordered.setdefault(str(path), path)
    return tuple(ordered.values())


def export_partitioned_component(
    connection: duckdb.DuckDBPyConnection,
    root: Path,
    name: str,
    config: ContextCompletionConfig,
    query_factory: QueryFactory,
    code_dependencies: tuple[Path, ...],
    partition_years: int = 5,
) -> ComponentArtifact:
    if partition_years < 1:
        raise ValueError("partition_years must be positive")
    code = _code_identities(code_dependencies)
    shards: list[ComponentArtifact] = []
    expected_schema: Mapping[str, str] | None = None
    for start in range(config.start_season, config.end_season + 1, partition_years):
        end = min(start + partition_years - 1, config.end_season)
        partition_config = config.model_copy(
            update={"start_season": start, "end_season": end}
        )
        query, schema = query_factory(name, partition_config)
        if expected_schema is None:
            expected_schema = schema
        elif dict(schema) != dict(expected_schema):
            raise ValueError(f"Partition schema changed for {name} at {start}-{end}")
        partition_root = root / f"{name}_parts" / f"{start}-{end}"
        logger.info("Preparing %s partition %d-%d", name, start, end)
        shard = _load_reusable_shard(partition_root, name, query, schema, code)
        if shard is None:
            attempt = _next_attempt(partition_root)
            logger.info("Building %s partition %d-%d in %s", name, start, end, attempt)
            shard = _export_shard(
                connection,
                attempt,
                name,
                query,
                schema,
                code_dependencies,
            )
        shards.append(shard)
    if expected_schema is None:
        raise ValueError(f"No partitions generated for {name}")
    shard_paths = ", ".join(sql_literal(shard.data.path) for shard in shards)
    final_query = f"SELECT * FROM read_parquet([{shard_paths}])"
    metadata = root / f"{name}.json"
    if metadata.exists():
        artifact = ComponentArtifact.model_validate_json(
            metadata.read_text(encoding="utf-8")
        )
        verify_component(artifact)
        if artifact.columns != dict(expected_schema):
            raise ValueError(f"Existing final artifact schema changed for {name}")
        if Path(artifact.query.path).read_text(encoding="utf-8") != final_query:
            raise ValueError(f"Existing final artifact SQL changed for {name}")
        logger.info("Reusing verified final partitioned artifact %s", name)
        return artifact
    return export_component(
        connection,
        root,
        name=name,
        query=final_query,
        expected_columns=expected_schema,
        dependencies=_dependency_paths(shards),
    )
