from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import duckdb
import pytest
from sqlmesh.core.snapshot import SnapshotTableInfo

from scripts.build_retrosheet_candidate import (
    RETROSHEET_SCHEMAS,
    SOURCE_SCHEMAS,
    BuildOptions,
    command_environment,
    copy_project,
    fingerprints,
    freeze_prod_sources,
    load_checkpoint,
    prod_inputs,
    release_identities,
    require_unchanged_prod,
    run_step,
    same_code,
    seed_digests,
    side_record,
    validate_options,
    verify_release,
)

FINGERPRINT = {
    "data_hash": "11",
    "metadata_hash": "12",
    "parent_data_hash": "13",
    "parent_metadata_hash": "14",
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def options(tmp_path: Path, output: Path | None = None) -> BuildOptions:
    project = tmp_path / "repo" / "bc"
    (project / "models").mkdir(parents=True, exist_ok=True)
    (project / "seeds").mkdir(exist_ok=True)
    (project / "config.py").write_text("config = None\n")
    (project / "external_models.yaml").write_text(
        "- name: event.events\n- name: misc.parks\n"
    )
    release = tmp_path / "release"
    release.mkdir(exist_ok=True)
    events = release / "events.parquet"
    events.write_bytes(b"release events")
    manifest = tmp_path / "release-manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "files": {
                    "event.events": {
                        "sha256": sha256(events),
                        "bytes": events.stat().st_size,
                    }
                }
            }
        )
    )
    prod_db = tmp_path / "prod" / "bc.db"
    prod_db.parent.mkdir(exist_ok=True)
    if not prod_db.exists():
        with duckdb.connect(str(prod_db)) as connection:
            connection.execute("CREATE SCHEMA event")
            connection.execute("CREATE SCHEMA misc")
            connection.execute("CREATE TABLE event.events AS SELECT 1 AS event_key")
            connection.execute("CREATE TABLE misc.parks AS SELECT 'P' AS park_id")
    prod_state = tmp_path / "prod" / "bc_state.db"
    prod_state.touch()
    return BuildOptions(
        project=project,
        prod_db=prod_db,
        prod_state=prod_state,
        release=release,
        release_manifest=manifest,
        output=output or tmp_path / "out",
    )


def state_with_prod(path: Path, fingerprint: dict[str, str]) -> None:
    reference = SnapshotTableInfo.model_validate(
        {
            "name": '"bc"."main_models"."fixture"',
            "fingerprint": fingerprint,
            "dev_table_suffix": "dev",
            "version": "v1",
            "physical_schema": "sqlmesh__main_models",
            "parents": [],
            "kind_name": "FULL",
        }
    )
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA sqlmesh")
        connection.execute(
            "CREATE TABLE sqlmesh._environments(name VARCHAR,snapshots VARCHAR)"
        )
        connection.execute(
            "CREATE TABLE sqlmesh._snapshots(name VARCHAR,identifier VARCHAR,fingerprint VARCHAR)"
        )
        connection.execute(
            "INSERT INTO sqlmesh._environments VALUES ('prod', ?)",
            [json.dumps([reference.model_dump(mode="json", by_alias=True)])],
        )
        connection.execute(
            "INSERT INTO sqlmesh._snapshots VALUES (?,?,?)",
            [reference.name, reference.identifier, json.dumps(fingerprint)],
        )
        connection.execute(
            "INSERT INTO sqlmesh._snapshots VALUES (?,'stale',?)",
            [reference.name, json.dumps({key: "stale" for key in fingerprint})],
        )


def db_with_seed(path: Path, value: str) -> None:
    with duckdb.connect(str(path)) as connection:
        connection.execute("CREATE SCHEMA main_seeds")
        connection.execute(
            "CREATE TABLE main_seeds.seed_fixture AS "
            + "SELECT * FROM (VALUES (1, ?), (2, 'b')) t(id, label)",
            [value],
        )


@pytest.mark.parametrize("protected", ["prod", "release", "project"])
def test_output_may_not_overlap_protected_inputs(
    tmp_path: Path, protected: str
) -> None:
    base = options(tmp_path)
    inside = {
        "prod": base.prod_db.parent,
        "release": base.release / "out",
        "project": base.project / "out",
    }[protected]
    with pytest.raises(ValueError, match="overlap protected input"):
        validate_options(options(tmp_path, inside))


def test_nonempty_output_without_checkpoint_is_refused(tmp_path: Path) -> None:
    build = options(tmp_path)
    build.output.mkdir()
    (build.output / "stray.txt").write_text("x")
    with pytest.raises(ValueError, match="empty or a resumable"):
        validate_options(build)


def test_changed_release_file_is_refused(tmp_path: Path) -> None:
    build = options(tmp_path)
    (build.release / "events.parquet").write_bytes(b"tampered events")
    with pytest.raises(ValueError, match="differs from its manifest"):
        verify_release(build, ["event.events", "misc.parks"])


def test_release_routes_only_retrosheet_schemas_on_side_b(tmp_path: Path) -> None:
    build = options(tmp_path)
    inherited = {"BC_DB_PATH": "production.db", "SQLMESH__X": "1", "PATH": "/bin"}
    environments = {
        side: command_environment(build, side, inherited) for side in ("A", "B")
    }
    for side, environment in environments.items():
        assert environment["PATH"] == "/bin"
        assert "SQLMESH__X" not in environment
        assert environment["BC_DB_PATH"] == str(build.output / side / "bc.db")
        for schema in SOURCE_SCHEMAS:
            expected = (
                build.release
                if side == "B" and schema in RETROSHEET_SCHEMAS
                else build.prod_sources / schema
            )
            assert environment[f"BC_SOURCE_ROOT_{schema.upper()}"] == expected.as_uri()
    assert inherited["BC_DB_PATH"] == "production.db"


def test_prod_sources_freeze_every_external_and_leave_prod_untouched(
    tmp_path: Path,
) -> None:
    build = options(tmp_path)
    before = build.prod_db.stat().st_mtime_ns
    files = freeze_prod_sources(build, ["event.events", "misc.parks"])
    assert set(files) == {"event.events", "misc.parks"}
    for name, digest in files.items():
        schema, table = name.split(".")
        parquet = build.prod_sources / schema / f"{table}.parquet"
        assert sha256(parquet) == digest
    assert build.prod_db.stat().st_mtime_ns == before


def test_snapshot_keeps_branch_seeds_and_drops_deleted_models(tmp_path: Path) -> None:
    build = options(tmp_path)
    repository = str(build.project.parent)
    subprocess.run(["git", "init", "-q", repository], check=True)
    subprocess.run(
        [
            "git",
            "-C",
            repository,
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "commit",
            "-q",
            "--allow-empty",
            "-m",
            "fixture",
        ],
        check=True,
    )
    seed = build.project / "seeds" / "seed_fixture.csv"
    seed.write_text("id,label\n1,branch\n")
    stale = build.project / "models" / "stale.sql"
    stale.write_text("SELECT 1")
    copy_project(build)
    stale.unlink()
    identity = copy_project(build)
    snapshot = build.snapshot / "bc"
    assert (snapshot / "seeds" / "seed_fixture.csv").read_text() == seed.read_text()
    assert not (snapshot / "models" / "stale.sql").exists()
    assert (build.snapshot / "scripts" / "preload_sources.py").is_file()
    git = identity["git"]
    assert isinstance(git, dict) and len(str(git.get("head"))) == 40


def test_fingerprints_read_only_the_active_prod_snapshot(tmp_path: Path) -> None:
    state = tmp_path / "state.db"
    state_with_prod(state, FINGERPRINT)
    assert fingerprints(state) == {'"bc"."main_models"."fixture"': FINGERPRINT}


@pytest.mark.parametrize(
    ("other_fingerprint", "other_seed", "expected"),
    [
        (FINGERPRINT, "a", True),
        (FINGERPRINT, "changed", False),
        ({**FINGERPRINT, "data_hash": "99"}, "a", False),
    ],
)
def test_a_is_prod_only_when_fingerprints_and_seeds_match(
    tmp_path: Path, other_fingerprint: dict[str, str], other_seed: str, expected: bool
) -> None:
    records = []
    for name, fingerprint, seed in (
        ("prod", FINGERPRINT, "a"),
        ("branch", other_fingerprint, other_seed),
    ):
        state = tmp_path / f"{name}-state.db"
        db = tmp_path / f"{name}.db"
        state_with_prod(state, fingerprint)
        db_with_seed(db, seed)
        records.append(side_record(db, state, "sources", "other"))
    assert same_code(records[0], records[1]) is expected


def test_resume_skips_completed_steps_and_refuses_changed_inputs(
    tmp_path: Path,
) -> None:
    build = options(tmp_path)
    calls: list[str] = []
    checkpoint = load_checkpoint(build, {"release": "one"})
    run_step(build, checkpoint, "first", lambda: calls.append("first") or {"n": 1})
    resumed = load_checkpoint(build, {"release": "one"})
    result = run_step(build, resumed, "first", lambda: calls.append("again"))
    assert calls == ["first"]
    assert result == {"n": 1}
    with pytest.raises(ValueError, match="Inputs changed"):
        load_checkpoint(build, {"release": "two"})


def test_seed_digest_tracks_content_and_columns_not_row_order(tmp_path: Path) -> None:
    def digest_of(name: str, rows: str, label: str = "t") -> list[str]:
        db = tmp_path / f"{name}.db"
        with duckdb.connect(str(db)) as connection:
            connection.execute("CREATE SCHEMA main_seeds")
            connection.execute(
                "CREATE TABLE main_seeds.seed_fixture AS "
                + f"SELECT * FROM (VALUES {rows}) v(id, {label})"
            )
        return seed_digests(db)["seed_fixture"]

    base = digest_of("base", "(1, 'a'), (2, 'b')")
    assert digest_of("reordered", "(2, 'b'), (1, 'a')") == base
    assert digest_of("edited", "(1, 'a'), (2, 'z')") != base
    assert digest_of("renamed", "(1, 'a'), (2, 'b')", "label") != base


def test_prod_change_during_build_is_refused(tmp_path: Path) -> None:
    build = options(tmp_path)
    inputs = json.loads(json.dumps(prod_inputs(build)))
    require_unchanged_prod(build, inputs)
    with duckdb.connect(str(build.prod_db)) as connection:
        connection.execute("CREATE TABLE misc.extra AS SELECT 1 AS x")
    with pytest.raises(RuntimeError, match="changed during the candidate build"):
        require_unchanged_prod(build, inputs)


def test_resume_refuses_a_rewritten_release_file(tmp_path: Path) -> None:
    build = options(tmp_path)
    names = ["event.events", "misc.parks"]
    _ = load_checkpoint(build, {"release": release_identities(build, names)})
    events = build.release / "events.parquet"
    events.write_bytes(b"other events!!")
    with pytest.raises(ValueError, match="Inputs changed"):
        load_checkpoint(build, {"release": release_identities(build, names)})
