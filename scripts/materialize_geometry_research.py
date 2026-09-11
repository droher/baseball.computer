from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import logging
import platform
import re
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import NamedTuple, cast

import duckdb
from sqlmesh import Context
from sqlmesh.core.config import (
    Config,
    DuckDBConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
    PlanConfig,
)
from sqlmesh.core.config.common import VirtualEnvironmentMode
from sqlmesh.core.model.kind import FullKind


LOGGER = logging.getLogger(__name__)
SELECTED_MODELS = {
    "main_models.event_observation_geometry",
    "main_models.model_input_geometry",
    "main_models.model_input_observation_batted_ball",
}
ENVIRONMENT_PATTERN = re.compile(r"[a-z][a-z0-9_]*")
RESERVED_ENVIRONMENTS = {"dev", "prod"}
CONNECTION_SETTINGS: dict[str, object] = {
    "memory_limit": "30GB",
    "threads": 1,
    "preserve_insertion_order": False,
    "checkpoint_threshold": "1GB",
}


class ResearchArguments(NamedTuple):
    output_dir: Path
    evidence: Path
    environment: str
    snapshot: str
    start_season: int
    end_season: int
    resume: bool


def _file_identity(path: Path) -> dict[str, object]:
    stat = path.stat()
    sample = hashlib.sha256()
    with path.open("rb") as handle:
        sample.update(handle.read(1024 * 1024))
        if stat.st_size > 1024 * 1024:
            _ = handle.seek(max(0, stat.st_size - 1024 * 1024))
            sample.update(handle.read(1024 * 1024))
    return {
        "path": str(path.resolve()),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "sample_sha256_first_last_1mib": sample.hexdigest(),
    }


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while block := handle.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _code_identity(repo_root: Path) -> dict[str, object]:
    relative_paths = [
        "bc/audits/sentinel_status_consistent.sql",
        "bc/models/intermediate/coverage/event_observation_geometry.sql",
        "bc/models/intermediate/modeling_datasets/model_input_geometry.sql",
        "bc/models/intermediate/modeling_datasets/model_input_observation_batted_ball.sql",
        "scripts/materialize_geometry_research.py",
    ]
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return {
        "git_revision": revision,
        "file_sha256": {
            relative: _file_sha256(repo_root / relative) for relative in relative_paths
        },
    }


def _runtime_identity() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "duckdb": importlib.metadata.version("duckdb"),
        "sqlmesh": importlib.metadata.version("sqlmesh"),
    }


def _tree_identity(path: Path) -> dict[str, object]:
    digest = hashlib.sha256()
    file_count = 0
    total_size = 0
    for file in sorted(
        candidate for candidate in path.rglob("*") if candidate.is_file()
    ):
        stat = file.stat()
        relative = file.relative_to(path).as_posix()
        digest.update(f"{relative}\0{stat.st_size}\0{stat.st_mtime_ns}\n".encode())
        file_count += 1
        total_size += stat.st_size
    return {
        "path": str(path.resolve()),
        "file_count": file_count,
        "total_size": total_size,
        "metadata_sha256": digest.hexdigest(),
    }


def _source_identity(repo_root: Path) -> dict[str, object]:
    return {
        "database": _file_identity(repo_root / "bc.db"),
        "state": _file_identity(repo_root / "bc/bc_state.db"),
        "published_catalog": _file_identity(repo_root / "bc/bc_publish.ducklake"),
        "published_data": _tree_identity(repo_root / "bc/bc_publish_data"),
    }


def _config(
    *, database: Path, state: Path, start_season: int, end_season: int, snapshot: str
) -> Config:
    connection = DuckDBConnectionConfig(
        catalogs={"bc": str(database)},
        connector_config=CONNECTION_SETTINGS,
        concurrent_tasks=1,
    )
    return Config(
        default_gateway="bc",
        gateways={
            "bc": GatewayConfig(
                connection=connection,
                state_connection=DuckDBConnectionConfig(database=str(state)),
            )
        },
        model_defaults=ModelDefaultsConfig(
            dialect="duckdb", start=date(2026, 5, 1), kind=FullKind()
        ),
        virtual_environment_mode=VirtualEnvironmentMode.DEV_ONLY,
        plan=PlanConfig(always_recreate_environment=True),
        variables={
            "start_season": start_season,
            "end_season": end_season,
            "source_snapshot_id": snapshot,
        },
        before_all=[],
        ignore_patterns=["models/**/*.yml", "seeds/**/*.yml"],
        snapshot_ttl="in 1 week",
        environment_ttl="in 1 week",
        disable_anonymized_analytics=True,
    )


def materialization_counts(database: Path, schema: str) -> dict[str, object]:
    with duckdb.connect(str(database), read_only=True) as connection:
        dimension_rows = cast(
            list[tuple[str, int]],
            connection.execute(
                f'SELECT geometry_dimension, COUNT(*) FROM "{schema}".model_input_geometry GROUP BY 1 ORDER BY 1'
            ).fetchall(),
        )
        status_rows = cast(
            list[tuple[str, str, int]],
            connection.execute(
                f'SELECT geometry_dimension, observed_status, COUNT(*) FROM "{schema}".model_input_geometry GROUP BY 1, 2 ORDER BY 1, 2'
            ).fetchall(),
        )
        side_classes = cast(
            list[tuple[str, int]],
            connection.execute(
                f"SELECT class, COUNT(*) FROM \"{schema}\".model_input_geometry WHERE geometry_dimension = 'location_side' AND is_observed_class GROUP BY 1 ORDER BY 1"
            ).fetchall(),
        )
        invariants = cast(
            tuple[int, int, int, int] | None,
            connection.execute(
                f"""
            SELECT
                COUNT(*) FILTER (WHERE geometry_target_contract IS DISTINCT FROM 'geometry-v2-global-side'),
                COUNT(*) FILTER (
                    WHERE geometry_dimension = 'location_side'
                      AND (dl_artifact_id IS NOT NULL OR dl_p_class IS NOT NULL
                           OR propensity_p_observed IS NOT NULL OR propensity_artifact_id IS NOT NULL)
                ),
                COUNT(*) FILTER (
                    WHERE geometry_dimension = 'location_side' AND is_observed_class
                      AND (class IS NULL OR class NOT IN ('All', 'Left', 'Middle', 'Right'))
                ),
                COUNT(*) FILTER (
                    WHERE geometry_dimension = 'location_angle'
                      AND sentinel_type = 'default'
                      AND observed_status IS DISTINCT FROM 'default_code'
                )
            FROM "{schema}".model_input_geometry
            """
            ).fetchone(),
        )
        duplicate_count = cast(
            tuple[int] | None,
            connection.execute(
                f"""
            SELECT COUNT(*) FROM (
                SELECT event_key, geometry_dimension
                FROM "{schema}".model_input_geometry
                GROUP BY 1, 2 HAVING COUNT(*) != 1
            )
            """
            ).fetchone(),
        )
    if invariants is None or duplicate_count is None:
        raise RuntimeError("materialization validation returned no row")
    failures = {
        "wrong_contract": int(invariants[0]),
        "stale_side_inputs": int(invariants[1]),
        "invalid_observed_side_class": int(invariants[2]),
        "invalid_angle_default_status": int(invariants[3]),
        "duplicate_event_dimension": int(duplicate_count[0]),
    }
    if any(failures.values()):
        raise RuntimeError(f"materialization invariant failures: {failures}")
    return {
        "dimension_rows": {str(name): int(count) for name, count in dimension_rows},
        "status_rows": [
            {"dimension": str(dimension), "status": str(status), "rows": int(count)}
            for dimension, status, count in status_rows
        ],
        "observed_location_side_classes": {
            str(name): int(count) for name, count in side_classes
        },
        "invariant_failures": failures,
    }


def _arguments() -> ResearchArguments:
    parser = argparse.ArgumentParser()
    _ = parser.add_argument("--output-dir", type=Path, required=True)
    _ = parser.add_argument("--evidence", type=Path, required=True)
    _ = parser.add_argument("--environment", required=True)
    _ = parser.add_argument("--snapshot", required=True)
    _ = parser.add_argument("--start-season", type=int, default=1910)
    _ = parser.add_argument("--end-season", type=int, default=2025)
    _ = parser.add_argument("--resume", action="store_true")
    values = vars(parser.parse_args())
    return ResearchArguments(
        output_dir=cast(Path, values["output_dir"]),
        evidence=cast(Path, values["evidence"]),
        environment=cast(str, values["environment"]),
        snapshot=cast(str, values["snapshot"]),
        start_season=cast(int, values["start_season"]),
        end_season=cast(int, values["end_season"]),
        resume=cast(bool, values["resume"]),
    )


def validate_environment(environment: str) -> None:
    if environment in RESERVED_ENVIRONMENTS:
        raise ValueError(f"reserved SQLMesh environment: {environment}")
    if ENVIRONMENT_PATTERN.fullmatch(environment) is None:
        raise ValueError(
            "environment must be a lowercase SQL identifier beginning with a letter"
        )


def main() -> None:
    args = _arguments()
    repo_root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir.resolve()
    evidence_path = args.evidence.resolve()
    validate_environment(args.environment)
    if output_dir.exists() and not args.resume:
        raise FileExistsError(
            f"immutable output directory already exists: {output_dir}"
        )
    if evidence_path.exists():
        raise FileExistsError(f"evidence path already exists: {evidence_path}")
    if args.start_season > args.end_season:
        raise ValueError("start-season must not exceed end-season")
    before = _source_identity(repo_root)
    database = output_dir / "bc.db"
    state = output_dir / "bc_geometry_research_state.db"
    if args.resume:
        if not database.exists() or not state.exists():
            raise FileNotFoundError("resume requires the cloned database and state")
        LOGGER.info("resuming the existing isolated SQLMesh materialization")
    else:
        output_dir.mkdir(parents=True)
        LOGGER.info("cloning production database and state with APFS copy-on-write")
        _ = subprocess.run(
            ["cp", "-c", str(repo_root / "bc.db"), str(database)], check=True
        )
        _ = subprocess.run(
            ["cp", "-c", str(repo_root / "bc/bc_state.db"), str(state)], check=True
        )
    LOGGER.info("loading isolated SQLMesh context without the publish catalog")
    context = Context(
        paths=[repo_root / "bc"],
        config=_config(
            database=database,
            state=state,
            start_season=args.start_season,
            end_season=args.end_season,
            snapshot=args.snapshot,
        ),
        concurrent_tasks=1,
    )
    try:
        LOGGER.info("planning and applying selected models to %s", args.environment)
        _ = context.plan(
            args.environment,
            create_from="prod",
            select_models=SELECTED_MODELS,
            skip_tests=True,
            no_prompts=True,
            auto_apply=True,
        )
    finally:
        context.close()
    schema = f"main_models__{args.environment}"
    LOGGER.info("validating materialized research schema %s", schema)
    counts = materialization_counts(database, schema)
    after = _source_identity(repo_root)
    if after != before:
        raise RuntimeError("production database, state, or publish surfaces changed")
    evidence = {
        "status": "complete",
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "environment": args.environment,
        "schema": schema,
        "source_snapshot_id": args.snapshot,
        "season_range": [args.start_season, args.end_season],
        "selected_models": sorted(SELECTED_MODELS),
        "connection_settings": CONNECTION_SETTINGS,
        "runtime": _runtime_identity(),
        "code_identity": _code_identity(repo_root),
        "database_path": str(database),
        "state_path": str(state),
        "production_identity_before": before,
        "production_identity_after": after,
        "production_unchanged": True,
        "materialization": counts,
    }
    evidence_path.parent.mkdir(parents=True, exist_ok=True)
    _ = evidence_path.write_text(
        json.dumps(evidence, indent=2) + "\n", encoding="utf-8"
    )
    LOGGER.info("wrote evidence to %s", evidence_path)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    main()
