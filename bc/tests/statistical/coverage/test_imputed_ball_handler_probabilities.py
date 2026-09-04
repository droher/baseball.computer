"""Tests for the bayes ball-handler manifest-ingest helper (Model D).

The SQLMesh ``@model`` file imports the project dialect (``UINTEGER`` etc.)
which stock sqlglot can't parse without a SQLMesh context, so we exercise
the underlying ``aggregate_ball_handler_frames`` helper directly, mirroring
``test_imputed_fielding_credit``.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import numpy as np
import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]
from python_models.statistical.bayes.manifest_ingest import (
    METHOD_HIERARCHICAL_BAYES_SOFTMAX,
    BALL_HANDLER_SCHEMA,
    aggregate_ball_handler_frames,
    iterate_published_credit_frames,
    query_ball_handler_personnel,
    stamp_ball_handler_personnel,
)

MODEL_NAME = "ball_handler_imputation"
N_POSITIONS = 9


def _write_ball_handler_artifact_with_pointer(
    *,
    tmp_path: Path,
    artifact_id: str,
    events: int,
    share_scale: float = 1.0,
) -> Path:
    from python_models.statistical.bayes.artifacts import bayes_artifact_dir
    from python_models.statistical.manifests import (
        write_manifest,
        write_published_pointer,
    )
    from python_models.statistical.schemas import (
        ArtifactManifest,
        BayesArtifactExtras,
        BayesDiagnosticsSummary,
        BayesPriorConfig,
        BayesSamplerConfig,
        PublishedPointer,
    )

    bayes_root = tmp_path / "bayes"
    published_root = tmp_path / "published"
    artifact_dir = bayes_artifact_dir(MODEL_NAME, artifact_id, root=bayes_root)
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    event_keys = np.repeat(np.arange(events, dtype=np.int64), N_POSITIONS)
    positions = np.tile(np.arange(1, N_POSITIONS + 1, dtype=np.int8), events)
    rng = np.random.default_rng(0)
    raw = rng.uniform(0.0, 1.0, size=(events, N_POSITIONS))
    shares = (
        (share_scale * raw / raw.sum(axis=1, keepdims=True))
        .reshape(-1)
        .astype(np.float64)
    )
    pl.DataFrame(
        {
            "event_key": event_keys,
            "fielding_position": positions,
            "expected_share": shares,
        }
    ).write_parquet(exports_dir / "ball_handler_probabilities.parquet")

    extras = BayesArtifactExtras(
        model_name=MODEL_NAME,
        model_version="0.3.0",
        dimension="ball_handler_position",
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=10,
            tune=10,
            chains=1,
            target_accept=0.8,
            random_seed=0,
            backend="numpyro",
        ),
        diagnostics_summary=BayesDiagnosticsSummary(
            rhat_max=1.01,
            ess_bulk_min=500.0,
            ess_tail_min=400.0,
            divergences=0,
            total_draws=10,
        ),
        source_effect_active=True,
        event_row_count=events,
    )
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=MODEL_NAME,
        version="0.3.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={
            "ball_handler": exports_dir / "ball_handler_probabilities.parquet"
        },
        package_versions={},
        bayes_extras=extras,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    _ = write_published_pointer(
        PublishedPointer(
            model_name=MODEL_NAME,
            artifact_id=artifact_id,
            published_at=dt.datetime.now(tz=dt.timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
    )
    return published_root


def test_yields_empty_typed_frame_when_no_targets_published(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "empty"))
    frames = list(aggregate_ball_handler_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == 0
    assert dict(frame.schema) == BALL_HANDLER_SCHEMA


def test_yields_per_target_frame_with_artifact_id(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = 5
    published_root = _write_ball_handler_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="bh-1", events=events
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    frames = list(aggregate_ball_handler_frames())
    assert len(frames) == 1
    frame = frames[0]
    assert frame.height == events * N_POSITIONS
    assert dict(frame.schema) == BALL_HANDLER_SCHEMA
    assert set(frame.get_column("artifact_id").unique().to_list()) == {"bh-1"}
    for _contract_col in (
        "model_name",
        "model_version",
        "source_snapshot_id",
        "method",
        "observed_status",
        "confidence_status",
        "weak_identification_flag",
    ):
        assert frame.get_column(_contract_col).null_count() == 0
    assert set(frame.get_column("observed_status").unique().to_list()) == {"estimated"}
    assert set(frame.get_column("method").unique().to_list()) == {METHOD_HIERARCHICAL_BAYES_SOFTMAX}
    per_event = frame.group_by("event_key").agg(
        pl.col("expected_share").sum().alias("total")
    )
    np.testing.assert_allclose(per_event.get_column("total").to_numpy(), 1.0, atol=1e-9)


def test_credit_iterator_excludes_ball_handler_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_ball_handler_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="bh-2", events=3
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    credit_frames = list(iterate_published_credit_frames())
    assert credit_frames == [], (
        "the credit iterator must not slurp the published ball_handler export"
    )


def test_export_whose_events_do_not_sum_to_one_raises(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    published_root = _write_ball_handler_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="bh-bad", events=3, share_scale=1.2
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    with pytest.raises(ValueError, match="do not sum to 1"):
        _ = list(aggregate_ball_handler_frames())


def _personnel_db(*, events: int, missing: dict[int, int]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    epl_rows = [(ek, f"G{ek // 2:03d}", f"PFK{ek}") for ek in range(events)]
    pfs_rows = [
        (f"G{ek // 2:03d}", f"PFK{ek}", pos, f"P{ek}_{pos}")
        for ek in range(events)
        for pos in range(1, N_POSITIONS + 1)
        if missing.get(ek) != pos
    ]
    con.execute(
        "CREATE TABLE epl (event_key UINTEGER, game_id VARCHAR, "
        "personnel_fielding_key VARCHAR)"
    )
    con.executemany("INSERT INTO epl VALUES (?, ?, ?)", epl_rows)
    con.execute(
        "CREATE TABLE pfs (game_id VARCHAR, personnel_fielding_key VARCHAR, "
        "fielding_position UTINYINT, player_id VARCHAR)"
    )
    con.executemany("INSERT INTO pfs VALUES (?, ?, ?, ?)", pfs_rows)
    return con


def test_personnel_join_never_publishes_a_partial_simplex(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = 4
    published_root = _write_ball_handler_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="bh-join", events=events
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    handler_frame = next(iter(aggregate_ball_handler_frames()))
    con = _personnel_db(events=events, missing={2: 6})
    try:
        personnel = query_ball_handler_personnel(
            con,
            epl_table="epl",
            pfs_table="pfs",
            event_keys=handler_frame.get_column("event_key").unique().to_frame(),
        )
    finally:
        con.close()
    assert personnel.height == events * N_POSITIONS - 1

    joined = stamp_ball_handler_personnel(handler_frame, personnel)
    assert joined.get_column("player_id").null_count() == 0
    assert 2 not in joined.get_column("event_key").to_list()
    assert set(joined.get_column("event_key").to_list()) == {0, 1, 3}
    per_event = joined.group_by("event_key").agg(
        pl.len().alias("n"), pl.col("expected_share").sum().alias("total")
    )
    assert per_event.get_column("n").unique().to_list() == [N_POSITIONS]
    np.testing.assert_allclose(per_event.get_column("total").to_numpy(), 1.0, atol=1e-9)
    assert joined.select("event_key", "player_id", "fielding_position").n_unique() == joined.height


def test_personnel_join_keeps_every_event_when_all_positions_resolve(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = 3
    published_root = _write_ball_handler_artifact_with_pointer(
        tmp_path=tmp_path, artifact_id="bh-join-full", events=events
    )
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    handler_frame = next(iter(aggregate_ball_handler_frames()))
    con = _personnel_db(events=events, missing={})
    try:
        personnel = query_ball_handler_personnel(
            con,
            epl_table="epl",
            pfs_table="pfs",
            event_keys=handler_frame.get_column("event_key").unique().to_frame(),
        )
    finally:
        con.close()
    joined = stamp_ball_handler_personnel(handler_frame, personnel)
    assert joined.height == handler_frame.height
    assert joined.get_column("player_id").to_list() == [
        f"P{ek}_{pos}"
        for ek, pos in zip(
            joined.get_column("event_key").to_list(),
            joined.get_column("fielding_position").to_list(),
        )
    ]
