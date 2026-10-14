from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import shutil
import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import duckdb
import yaml
from pydantic import BaseModel, ConfigDict, TypeAdapter
from sqlmesh.core.snapshot import SnapshotTableInfo

from scripts.compare_retrosheet_sources import (
    digest,
    identifier,
    literal,
    stat_identity,
)

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[1]
RETROSHEET_SCHEMAS = frozenset({"event", "game", "box_score"})
SOURCE_SCHEMAS = ("event", "game", "box_score", "misc", "baseballdatabank", "biodata", "nlb_stats")
PROJECT_DIRECTORIES = ("models", "macros", "audits", "seeds", "python_models", "tests")
PROJECT_FILES = ("config.py", "external_models.yaml")
SNAPSHOT_SCRIPTS = ("preload_sources.py", "audit_report.py")
CHECKPOINT = "checkpoint.json"
Side = Literal["A", "B"]


class FrozenFile(BaseModel):
    model_config = ConfigDict(extra="ignore")
    sha256: str
    bytes: int


class SourceManifest(BaseModel):
    model_config = ConfigDict(extra="ignore")
    files: dict[str, FrozenFile]


class ExternalModel(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str


@dataclass(frozen=True)
class BuildOptions:
    project: Path
    prod_db: Path
    prod_state: Path
    release: Path
    release_manifest: Path
    output: Path
    memory_limit: str = "32GB"
    threads: int = 10
    tasks: int = 2
    always_build_a: bool = False

    @property
    def snapshot(self) -> Path:
        return self.output / "snapshot"

    @property
    def prod_sources(self) -> Path:
        return self.output / "sources" / "prod"

    def side_directory(self, side: Side) -> Path:
        return self.output / side


def json_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def schema_digest(files: Mapping[str, str], retrosheet: bool) -> str:
    return json_digest(
        {
            name: sha
            for name, sha in sorted(files.items())
            if (name.split(".")[0] in RETROSHEET_SCHEMAS) is retrosheet
        }
    )


def externals(project: Path) -> list[str]:
    document: object = yaml.safe_load((project / "external_models.yaml").read_text())
    return [
        model.name
        for model in TypeAdapter(list[ExternalModel]).validate_python(document)
    ]


def _overlaps(first: Path, second: Path) -> bool:
    return first == second or first in second.parents or second in first.parents


def validate_options(options: BuildOptions) -> None:
    output = options.output
    resolved = output.resolve()
    for protected in (
        options.project,
        options.prod_db,
        options.prod_state,
        options.release,
        options.release_manifest,
    ):
        if _overlaps(resolved, protected):
            raise ValueError(f"Output must not overlap protected input: {protected}")
    if output.is_symlink():
        raise ValueError("Output directory must not be a symlink")
    if output.exists():
        for path in output.rglob("*"):
            if path.is_symlink():
                raise ValueError(f"Output descendant must not be a symlink: {path}")
            if path.is_file() and path.stat().st_nlink > 1:
                raise ValueError(f"Output descendant must not be a hardlink: {path}")
        if any(output.iterdir()) and not (output / CHECKPOINT).is_file():
            raise ValueError("Output must be empty or a resumable candidate directory")
    for path in (options.prod_db, options.prod_state, options.release_manifest):
        if not path.is_file():
            raise ValueError(f"Required file does not exist: {path}")
    if not options.release.is_dir() or not (options.project / "config.py").is_file():
        raise ValueError("Release directory and SQLMesh project must exist")
    if options.threads < 1 or options.tasks < 1:
        raise ValueError("Threads and tasks must be positive")


def load_checkpoint(
    options: BuildOptions, inputs: dict[str, object]
) -> dict[str, object]:
    path = options.output / CHECKPOINT
    if not path.is_file():
        options.output.mkdir(parents=True, exist_ok=True)
        checkpoint: dict[str, object] = {"inputs": inputs, "steps": {}}
        path.write_text(json.dumps(checkpoint, indent=2) + "\n")
        return checkpoint
    loaded: dict[str, object] = json.loads(path.read_text())
    if loaded.get("inputs") != json.loads(json.dumps(inputs)):
        raise ValueError(
            "Inputs changed since this candidate started; use a fresh output"
        )
    return loaded


def run_step(
    options: BuildOptions,
    checkpoint: dict[str, object],
    name: str,
    action: Callable[[], object],
) -> object:
    steps = checkpoint["steps"]
    assert isinstance(steps, dict)
    if name in steps:
        LOGGER.info("Skipping completed step %s", name)
        return steps[name]
    LOGGER.info("Starting step %s", name)
    result = action()
    steps[name] = result
    (options.output / CHECKPOINT).write_text(json.dumps(checkpoint, indent=2) + "\n")
    LOGGER.info("Completed step %s", name)
    return result


def release_files(options: BuildOptions, names: list[str]) -> dict[str, Path]:
    return {
        name: options.release / f"{name.split('.')[1]}.parquet"
        for name in names
        if name.split(".")[0] in RETROSHEET_SCHEMAS
    }


def release_identities(
    options: BuildOptions, names: list[str]
) -> dict[str, dict[str, int] | None]:
    return {
        name: stat_identity(path) if path.is_file() else None
        for name, path in release_files(options, names).items()
    }


def verify_release(options: BuildOptions, names: list[str]) -> dict[str, str]:
    manifest = SourceManifest.model_validate_json(options.release_manifest.read_text())
    files: dict[str, str] = {}
    for name, path in release_files(options, names).items():
        expected = manifest.files.get(name)
        if expected is None:
            raise ValueError(f"Release manifest lacks source {name}")
        if (
            not path.is_file()
            or path.stat().st_size != expected.bytes
            or digest(path) != expected.sha256
        ):
            raise ValueError(f"Release file differs from its manifest: {path}")
        files[name] = expected.sha256
    return files


def freeze_prod_sources(options: BuildOptions, names: list[str]) -> dict[str, str]:
    before = stat_identity(options.prod_db)
    files: dict[str, str] = {}
    with duckdb.connect(str(options.prod_db), read_only=True) as connection:
        for name in names:
            schema, table = name.split(".")
            directory = options.prod_sources / schema
            directory.mkdir(parents=True, exist_ok=True)
            parquet = directory / f"{table}.parquet"
            LOGGER.info("Freezing prod source %s", name)
            connection.execute(
                f"COPY (SELECT * FROM {identifier(schema)}.{identifier(table)}) "
                + f"TO {literal(str(parquet))} (FORMAT PARQUET)"
            )
            files[name] = digest(parquet)
    if stat_identity(options.prod_db) != before:
        raise RuntimeError("Prod database changed while its sources were frozen")
    return files


def git_identity(project: Path) -> dict[str, str]:
    def git(*arguments: str) -> str:
        return subprocess.run(
            ["git", "-C", str(project), *arguments],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    return {
        "head": git("rev-parse", "HEAD").strip(),
        "status": git("status", "--porcelain"),
    }


def copy_project(options: BuildOptions) -> dict[str, object]:
    target = options.snapshot / "bc"
    target.mkdir(parents=True, exist_ok=True)
    for name in PROJECT_DIRECTORIES:
        source = options.project / name
        destination = target / name
        if destination.exists():
            shutil.rmtree(destination)
        if source.is_dir():
            _ = shutil.copytree(
                source,
                destination,
                ignore=shutil.ignore_patterns("__pycache__", ".cache"),
            )
    for name in PROJECT_FILES:
        _ = shutil.copy2(options.project / name, target / name)
    scripts = options.snapshot / "scripts"
    scripts.mkdir(exist_ok=True)
    for name in SNAPSHOT_SCRIPTS:
        _ = shutil.copy2(REPO_ROOT / "scripts" / name, scripts / name)
    files = {
        str(path.relative_to(options.snapshot)): digest(path)
        for path in sorted(options.snapshot.rglob("*"))
        if path.is_file() and "__pycache__" not in path.parts
    }
    return {"git": git_identity(options.project), "files_sha256": json_digest(files)}


def source_roots(options: BuildOptions, side: Side) -> dict[str, Path]:
    return {
        schema: options.release
        if side == "B" and schema in RETROSHEET_SCHEMAS
        else options.prod_sources / schema
        for schema in SOURCE_SCHEMAS
    }


def command_environment(
    options: BuildOptions, side: Side, inherited: Mapping[str, str] | None = None
) -> dict[str, str]:
    environment = {
        name: value
        for name, value in (os.environ if inherited is None else inherited).items()
        if not name.upper().startswith(("BC_", "SQLMESH__"))
    }
    directory = options.side_directory(side)
    environment.update(
        {
            "BC_DB_PATH": str(directory / "bc.db"),
            "BC_STATE_DB_PATH": str(directory / "bc_state.db"),
            "BC_DUCKDB_MEMORY_LIMIT": options.memory_limit,
            "BC_DUCKDB_THREADS": str(options.threads),
            "BC_CONCURRENT_TASKS": str(options.tasks),
        }
    )
    for schema, path in source_roots(options, side).items():
        environment[f"BC_SOURCE_ROOT_{schema.upper()}"] = path.as_uri()
    return environment


def run_logged(
    options: BuildOptions, side: Side, label: str, arguments: list[str], cwd: Path
) -> int:
    directory = options.side_directory(side)
    directory.mkdir(parents=True, exist_ok=True)
    log = directory / f"{label}.log"
    LOGGER.info("Running %s for %s; log %s", label, side, log)
    with log.open("w") as stream:
        result = subprocess.run(
            ["uv", "run", "--project", str(REPO_ROOT), "--group", "build", *arguments],
            cwd=cwd,
            env=command_environment(options, side),
            stdout=stream,
            stderr=subprocess.STDOUT,
        )
    return result.returncode


def build_side(options: BuildOptions, side: Side) -> dict[str, object]:
    directory = options.side_directory(side)
    steps: list[tuple[str, list[str]]] = [
        (
            "preload",
            ["python", str(options.snapshot / "scripts" / "preload_sources.py")],
        ),
        (
            "plan",
            [
                "sqlmesh",
                "--log-file-dir",
                str(directory / "sqlmesh-logs"),
                "plan",
                "--auto-apply",
                "--no-prompts",
            ],
        ),
    ]
    for label, arguments in steps:
        code = run_logged(options, side, label, arguments, options.snapshot / "bc")
        if code:
            message = (
                f"{side} {label} exited {code}; inspect {directory / f'{label}.log'}"
            )
            if label == "plan" and (directory / "bc.db").is_file():
                try:
                    _ = audit_side(options, side)
                    message += f" and {directory / 'audit-report.json'}"
                except Exception:
                    LOGGER.exception("Audit report after failed %s plan failed", side)
            raise RuntimeError(message)
    return {"db": str(directory / "bc.db"), "state": str(directory / "bc_state.db")}


def audit_side(options: BuildOptions, side: Side) -> dict[str, object]:
    directory = options.side_directory(side)
    report = directory / "audit-report.json"
    code = run_logged(
        options,
        side,
        "audit-report",
        [
            "python",
            str(options.snapshot / "scripts" / "audit_report.py"),
            "--project",
            str(options.snapshot / "bc"),
            "--db",
            str(directory / "bc.db"),
            "--output",
            str(report),
        ],
        options.snapshot / "bc",
    )
    if not report.is_file():
        raise RuntimeError(f"{side} audit report missing; inspect audit-report.log")
    document: dict[str, object] = json.loads(report.read_text())
    return {"report": str(report), "passed": code == 0, "summary": document["summary"]}


def fingerprints(state: Path) -> dict[str, dict[str, str]]:
    with duckdb.connect(str(state), read_only=True) as connection:
        record = connection.execute(
            "SELECT snapshots FROM sqlmesh._environments WHERE name='prod'"
        ).fetchone()
        if record is None:
            raise RuntimeError(f"Missing SQLMesh prod environment in {state}")
        references = TypeAdapter(list[SnapshotTableInfo]).validate_json(record[0])
        active = {(reference.name, reference.identifier) for reference in references}
        rows = connection.execute(
            "SELECT name,identifier,fingerprint FROM sqlmesh._snapshots"
        ).fetchall()
    return {
        str(name): TypeAdapter(dict[str, str]).validate_json(fingerprint)
        for name, identity, fingerprint in rows
        if (name, identity) in active
    }


def seed_digests(db: Path) -> dict[str, list[str]]:
    with duckdb.connect(str(db), read_only=True) as connection:
        names = [
            str(row[0])
            for row in connection.execute(
                "SELECT table_name FROM information_schema.tables "
                + "WHERE table_schema='main_seeds' ORDER BY table_name"
            ).fetchall()
        ]
        digests: dict[str, list[str]] = {}
        for name in names:
            row = connection.execute(
                "SELECT COUNT(*)::VARCHAR, "
                + "COALESCE(SUM(hash(row(*COLUMNS(*)))::HUGEINT), 0)::VARCHAR "
                + f"FROM main_seeds.{identifier(name)}"
            ).fetchone()
            assert row is not None
            columns = connection.execute(
                "SELECT column_name || ' ' || data_type FROM information_schema.columns "
                + "WHERE table_schema='main_seeds' AND table_name=? "
                + "ORDER BY ordinal_position",
                [name],
            ).fetchall()
            digests[name] = [
                str(row[0]),
                str(row[1]),
                *(str(column[0]) for column in columns),
            ]
    return digests


def side_record(
    db: Path, state: Path, retrosheet: str, other_sources: str
) -> dict[str, object]:
    return {
        "db": str(db),
        "state": str(state),
        "identity": stat_identity(db),
        "retrosheet_sources_sha256": retrosheet,
        "other_sources_sha256": other_sources,
        "fingerprints_sha256": json_digest(fingerprints(state)),
        "seeds_sha256": json_digest(seed_digests(db)),
    }


def same_code(first: Mapping[str, object], second: Mapping[str, object]) -> bool:
    return all(
        first[key] == second[key] for key in ("fingerprints_sha256", "seeds_sha256")
    )


def prod_inputs(options: BuildOptions) -> dict[str, object]:
    return {
        "prod_db": str(options.prod_db),
        "prod_identity": stat_identity(options.prod_db),
        "prod_state": str(options.prod_state),
        "prod_state_identity": stat_identity(options.prod_state),
    }


def require_unchanged_prod(options: BuildOptions, inputs: Mapping[str, object]) -> None:
    current = json.loads(json.dumps(prod_inputs(options)))
    if any(inputs[key] != value for key, value in current.items()):
        raise RuntimeError("Prod database or state changed during the candidate build")


def build(options: BuildOptions) -> dict[str, object]:
    validate_options(options)
    names = externals(options.project)
    inputs: dict[str, object] = {
        **prod_inputs(options),
        "release": str(options.release),
        "release_manifest_sha256": digest(options.release_manifest),
        "release_identities": release_identities(options, names),
        "externals": names,
    }
    checkpoint = load_checkpoint(options, inputs)
    verified = run_step(
        options, checkpoint, "verify_release", lambda: verify_release(options, names)
    )
    release = TypeAdapter(dict[str, str]).validate_python(verified)
    frozen = run_step(
        options,
        checkpoint,
        "freeze_prod_sources",
        lambda: freeze_prod_sources(options, names),
    )
    prod_files = TypeAdapter(dict[str, str]).validate_python(frozen)
    other_sources = schema_digest(prod_files, retrosheet=False)
    prod_retrosheet = schema_digest(prod_files, retrosheet=True)
    code = run_step(options, checkpoint, "snapshot_code", lambda: copy_project(options))
    _ = run_step(options, checkpoint, "build_B", lambda: build_side(options, "B"))
    _ = run_step(options, checkpoint, "audit_B", lambda: audit_side(options, "B"))
    require_unchanged_prod(options, inputs)
    prod = side_record(
        options.prod_db, options.prod_state, prod_retrosheet, other_sources
    )
    candidate = side_record(
        options.side_directory("B") / "bc.db",
        options.side_directory("B") / "bc_state.db",
        schema_digest(release, retrosheet=True),
        other_sources,
    )
    sides: dict[str, object] = {"prod": prod, "B": candidate}
    if not options.always_build_a and same_code(prod, candidate):
        LOGGER.info("Branch fingerprints and seeds equal prod's state; A is prod")
        sides["A"] = {"same_as": "prod"}
    else:
        _ = run_step(options, checkpoint, "build_A", lambda: build_side(options, "A"))
        _ = run_step(options, checkpoint, "audit_A", lambda: audit_side(options, "A"))
        sides["A"] = side_record(
            options.side_directory("A") / "bc.db",
            options.side_directory("A") / "bc_state.db",
            prod_retrosheet,
            other_sources,
        )
    require_unchanged_prod(options, inputs)
    steps = checkpoint["steps"]
    assert isinstance(steps, dict)
    candidates: dict[str, object] = {
        "release_manifest_sha256": inputs["release_manifest_sha256"],
        "code": code,
        "sides": sides,
        "audits": {side: steps.get(f"audit_{side}") for side in ("A", "B")},
    }
    path = options.output / "candidates.json"
    _ = path.write_text(json.dumps(candidates, indent=2) + "\n")
    LOGGER.info("Candidates written to %s", path)
    return candidates


def parse_args(argv: list[str] | None = None) -> BuildOptions:
    parser = argparse.ArgumentParser(
        description="Build isolated A (current sources) and B (release) candidates."
    )
    parser.add_argument("--project", type=Path, default=REPO_ROOT / "bc")
    parser.add_argument("--prod-db", type=Path, default=REPO_ROOT / "bc.db")
    parser.add_argument(
        "--prod-state", type=Path, default=REPO_ROOT / "bc" / "bc_state.db"
    )
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--release-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--memory-limit", default="32GB")
    parser.add_argument("--threads", type=int, default=10)
    parser.add_argument("--tasks", type=int, default=2)
    parser.add_argument("--always-build-a", action="store_true")
    arguments = parser.parse_args(argv)
    return BuildOptions(
        project=arguments.project.resolve(),
        prod_db=arguments.prod_db.resolve(),
        prod_state=arguments.prod_state.resolve(),
        release=arguments.release.resolve(),
        release_manifest=arguments.release_manifest.resolve(),
        output=arguments.output.absolute(),
        memory_limit=arguments.memory_limit,
        threads=arguments.threads,
        tasks=arguments.tasks,
        always_build_a=arguments.always_build_a,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    _ = build(parse_args())
