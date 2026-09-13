from __future__ import annotations

import argparse
import json
import logging
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, TypedDict, cast

import duckdb

from python_models.imputation.artifacts import (
    ComponentArtifact,
    FileIdentity,
    export_component,
    file_identity,
    write_json,
    verify_component,
)
from python_models.imputation.context import (
    OUTPUT_SCHEMA as CONTEXT_SCHEMA,
    ContextCompletionConfig,
    build_context_completion_sql,
)
from python_models.imputation.completion_registry import (
    build_completion_registry_payload,
    query_source_population,
)
from python_models.imputation.coverage import (
    CoverageOptions,
    CoverageReport,
    audit_coverage,
)
from python_models.imputation.registry import default_field_registry
from python_models.imputation.validation import validate_component

_log = logging.getLogger(__name__)
VALUE_COMPONENTS = (
    "event_values",
    "park_factors",
    "run_expectancy",
    "state_transitions",
    "linear_weights",
)
PARTITIONED_COMPONENTS = {"geometry", "pitches", "fielding", "runners", "event_values"}


class ComponentParameters(TypedDict):
    start_season: int
    end_season: int
    sample_games: int | None


def component_modules(component: str) -> set[str]:
    modules = {
        "__init__.py",
        "__main__.py",
        "cli.py",
        "artifacts.py",
        "context.py",
        "coverage.py",
        "registry.py",
        "validation.py",
        "completion_registry.py",
        "officials.py",
        "geometry.py",
    }
    if component in VALUE_COMPONENTS:
        modules.update({"values.py", "values_validation.py"})
    elif component == "geometry":
        modules.update({"geometry_fast.py", "geometry_standardization.py"})
    else:
        modules.add(f"{component}.py")
    if component == "fielding":
        modules.update({"fielding_export.py", "fielding_allocation.py"})
    if component in PARTITIONED_COMPONENTS:
        modules.add("partitioned_export.py")
    return modules


def component_query(
    name: str, config: ContextCompletionConfig
) -> tuple[str, Mapping[str, str]]:
    parameters: ComponentParameters = {
        "start_season": config.start_season,
        "end_season": config.end_season,
        "sample_games": config.sample_games,
    }
    if name == "context":
        return build_context_completion_sql(config), CONTEXT_SCHEMA
    if name == "geometry":
        from python_models.imputation.geometry_fast import (
            OUTPUT_SCHEMA as GEOMETRY_SCHEMA,
            GeometryCompletionConfig,
            build_geometry_completion_sql,
        )
        from python_models.imputation.geometry_standardization import (
            build_geometry_standardization_query,
        )

        return build_geometry_standardization_query(
            build_geometry_completion_sql(GeometryCompletionConfig(**parameters))
        ), GEOMETRY_SCHEMA
    if name == "pitches":
        from python_models.imputation.pitches import (
            OUTPUT_SCHEMA as PITCH_SCHEMA,
            PitchCompletionConfig,
            build_pitch_completion_sql,
        )

        return build_pitch_completion_sql(
            PitchCompletionConfig(**parameters)
        ), PITCH_SCHEMA
    if name == "fielding":
        from python_models.imputation.fielding import (
            OUTPUT_SCHEMA as FIELDING_SCHEMA,
            FieldingCompletionConfig,
            build_fielding_completion_sql,
        )

        return build_fielding_completion_sql(
            FieldingCompletionConfig(**parameters)
        ), FIELDING_SCHEMA
    if name == "runners":
        from python_models.imputation.runners import (
            OUTPUT_SCHEMA as RUNNER_SCHEMA,
            RunnerCompletionConfig,
            build_runner_completion_sql,
        )

        return build_runner_completion_sql(
            RunnerCompletionConfig(**parameters)
        ), RUNNER_SCHEMA
    if name == "officials":
        from python_models.imputation.officials import (
            OUTPUT_SCHEMA as OFFICIAL_SCHEMA,
            build_officials_completion_sql,
        )

        return build_officials_completion_sql(config), OFFICIAL_SCHEMA
    if name in VALUE_COMPONENTS:
        from python_models.imputation.values import (
            OUTPUT_SCHEMAS,
            Config,
            build_values_component_sql,
        )

        return build_values_component_sql(name, Config(**parameters)), OUTPUT_SCHEMAS[
            name
        ]
    raise ValueError(f"Unknown component: {name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build additive PBP completion artifacts from a read-only database."
    )
    parser.add_argument("--database", type=Path, default=Path("bc.db"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--components",
        nargs="+",
        choices=(
            "coverage",
            "context",
            "geometry",
            "pitches",
            "fielding",
            "runners",
            "officials",
            *VALUE_COMPONENTS,
        ),
        default=[
            "coverage",
            "context",
            "geometry",
            "pitches",
            "fielding",
            "runners",
            "officials",
            *VALUE_COMPONENTS,
        ],
    )
    parser.add_argument("--start-season", type=int, default=1903)
    parser.add_argument("--end-season", type=int, default=2025)
    parser.add_argument("--sample-games", type=int, default=8)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--memory-limit", default="4GB")
    parser.add_argument("--threads", type=int, default=2)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    database = Path(args.database).resolve()
    output = Path(args.output).resolve()
    if output.exists() and not args.resume:
        raise FileExistsError(f"Choose a new output directory: {output}")
    if not database.is_file():
        raise FileNotFoundError(database)
    sample_games: int | None = None if args.full else args.sample_games
    config = ContextCompletionConfig(
        start_season=args.start_season,
        end_season=args.end_season,
        sample_games=sample_games,
    )
    output.mkdir(parents=True, exist_ok=args.resume)
    manifest_path = output / "manifest.json"
    manifest: dict[str, object] = (
        json.loads(manifest_path.read_text()) if args.resume else {}
    )
    if args.resume and any(
        manifest.get(key) != value
        for key, value in {
            "sample_games": sample_games,
            "start_season": config.start_season,
            "end_season": config.end_season,
            "scope": "existing_pbp_games_only",
        }.items()
    ):
        raise ValueError("Resume population does not match the existing build")
    package_root = Path(__file__).parent
    build_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    source_root = output / "code" / build_id
    source_root.mkdir(parents=True)
    code_identity: dict[str, object] = {}
    for path in sorted(package_root.glob("*.py")):
        destination = source_root / path.name
        shutil.copyfile(path, destination)
        code_identity[path.name] = file_identity(destination).model_dump()
    prior_artifacts = manifest.get("artifacts", {})
    prior_validation = manifest.get("validation", {})
    if not isinstance(prior_artifacts, dict) or not isinstance(prior_validation, dict):
        raise ValueError("Invalid prior build state")
    artifacts = dict(cast(dict[str, object], prior_artifacts))
    validation = dict(cast(dict[str, object], prior_validation))
    prior_source = manifest.get("source_database")
    manifest.update(
        {
            "created_at": manifest.get(
                "created_at", datetime.now(timezone.utc).isoformat()
            ),
            "status": "running",
            "scope": "existing_pbp_games_only",
            "project_complete": False,
            "sample_games": sample_games,
            "start_season": config.start_season,
            "end_season": config.end_season,
            "components_requested": list(args.components),
            "confidence_status": "exploratory",
            "latest_build_id": build_id,
            "latest_code": code_identity,
            "duckdb_version": duckdb.__version__,
            "artifacts": artifacts,
            "validation": validation,
        }
    )
    manifest.pop("error", None)
    write_json(manifest_path, manifest)
    try:
        with duckdb.connect(
            str(database),
            read_only=True,
            config={"threads": args.threads, "memory_limit": args.memory_limit},
        ) as connection:
            initial_stat = database.stat()
            source_identity = file_identity(database)
            if prior_source is not None:
                expected_source = FileIdentity.model_validate(prior_source)
                if (source_identity.sha256, source_identity.bytes) != (
                    expected_source.sha256,
                    expected_source.bytes,
                ):
                    raise ValueError("Resume source database content changed")
            manifest["source_database"] = source_identity.model_dump()
            manifest["source_population"] = query_source_population(
                connection
            ).model_dump()
            write_json(manifest_path, manifest)
            executed_modules = component_modules("coverage")
            for component in dict.fromkeys(args.components):
                if component in artifacts:
                    if component == "coverage":
                        previous = FileIdentity.model_validate(artifacts[component])
                        if file_identity(Path(previous.path)) != previous:
                            raise ValueError("Coverage content changed")
                    else:
                        verify_component(
                            ComponentArtifact.model_validate(artifacts[component])
                        )
                        if component not in validation:
                            raise ValueError(f"Missing prior validation: {component}")
                    _log.info("Reusing verified component %s", component)
                    continue
                executed_modules.update(component_modules(component))
                if component == "coverage":
                    registry = default_field_registry()
                    unclassified = registry.unclassified_columns(connection)
                    if unclassified:
                        raise ValueError(
                            f"Unclassified source fields: {sorted(unclassified)}"
                        )
                    (output / "field_registry.json").write_text(
                        registry.model_dump_json(indent=2) + "\n"
                    )
                    report = audit_coverage(
                        connection,
                        registry,
                        CoverageOptions(
                            season_start=config.start_season,
                            season_end=config.end_season,
                            sample_game_limit=config.sample_games,
                        ),
                    )
                    coverage_path = output / "coverage.json"
                    coverage_path.write_text(report.model_dump_json(indent=2) + "\n")
                    artifacts[component] = file_identity(coverage_path).model_dump()
                    _log.info(
                        "Coverage audit complete: %s games, %s fields",
                        report.sampled_games,
                        len(report.fields),
                    )
                else:
                    query, schema = component_query(component, config)
                    if sample_games is None and component in PARTITIONED_COMPONENTS:
                        from python_models.imputation.partitioned_export import (
                            export_partitioned_component,
                        )

                        result = export_partitioned_component(
                            connection,
                            output,
                            component,
                            config,
                            component_query,
                            tuple(
                                source_root / name
                                for name in sorted(component_modules(component))
                            ),
                        )
                    elif component == "fielding":
                        from python_models.imputation.fielding_export import (
                            export_fielding_component,
                        )

                        result = export_fielding_component(
                            connection, output, query, schema
                        )
                    else:
                        result = export_component(
                            connection,
                            output,
                            name=component,
                            query=query,
                            expected_columns=schema,
                        )
                    code_dependencies = tuple(
                        file_identity(source_root / name)
                        for name in sorted(component_modules(component))
                    )
                    result = result.model_copy(
                        update={
                            "dependencies": (*result.dependencies, *code_dependencies)
                        }
                    )
                    (output / f"{component}.json").write_text(
                        result.model_dump_json(indent=2) + "\n"
                    )
                    validation[component] = validate_component(
                        connection, result, config
                    )
                    artifacts[component] = result.model_dump()
                manifest["artifacts"] = artifacts
                manifest["validation"] = validation
                write_json(manifest_path, manifest)
            if "coverage" in artifacts:
                coverage_identity = FileIdentity.model_validate(artifacts["coverage"])
                coverage_path = Path(coverage_identity.path)
                if file_identity(coverage_path) != coverage_identity:
                    raise ValueError("Coverage content changed")
                report = CoverageReport.model_validate_json(coverage_path.read_text())
                registry_payload = build_completion_registry_payload(
                    report, source_identity, coverage_identity
                )
                registry_path = output / "completion_registry.json"
                registry_path.write_text(
                    registry_payload.model_dump_json(indent=2) + "\n"
                )
                manifest["completion_registry"] = file_identity(
                    registry_path
                ).model_dump()
            final_stat = database.stat()
            if (initial_stat.st_size, initial_stat.st_mtime_ns) != (
                final_stat.st_size,
                final_stat.st_mtime_ns,
            ):
                raise ValueError("Source database changed during the build")
        for name in executed_modules:
            path = package_root / name
            frozen = source_root / path.name
            if (
                not frozen.exists()
                or file_identity(path).sha256 != file_identity(frozen).sha256
            ):
                raise ValueError(
                    f"Implementation changed during the build: {path.name}"
                )
        manifest["status"] = "components_complete" if args.full else "smoke_complete"
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(manifest_path, manifest)
        _log.info(
            "Artifacts ready at %s; project completion still requires all field families",
            output,
        )
    except Exception as error:
        manifest["status"] = "failed"
        manifest["error"] = f"{type(error).__name__}: {error}"
        write_json(manifest_path, manifest)
        raise
