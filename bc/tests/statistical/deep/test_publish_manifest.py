"""publish-manifest CLI handler: discover artifact, write PublishedPointer."""

from __future__ import annotations

import argparse
import importlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from python_models.statistical import config as cfg
from python_models.statistical.cli import _run_publish_manifest
from python_models.statistical.deep import registry as deep_registry
from python_models.statistical.manifests import (
    find_published_manifest,
    package_versions,
    write_manifest,
)
from python_models.statistical.schemas import (
    ArtifactKind,
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)

importlib.import_module("python_models.statistical.deep.targets")


@pytest.fixture
def roots(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path]:
    layout = {
        "DEEP_ROOT": tmp_path / "deep",
        "BAYES_ROOT": tmp_path / "bayes",
        "DATASETS_ROOT": tmp_path / "datasets",
        "EDA_ROOT": tmp_path / "eda",
    }
    for attr, path in layout.items():
        monkeypatch.setattr(cfg, attr, path)
    published_root = tmp_path / "published"
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
    return {**layout, "published": published_root}


def _bayes_extras(model_name: str) -> BayesArtifactExtras:
    return BayesArtifactExtras(
        model_name=model_name,
        model_version="0.1.0",
        prior_config=BayesPriorConfig(),
        sampler_config=BayesSamplerConfig(
            draws=10,
            tune=10,
            chains=1,
            target_accept=0.8,
            random_seed=0,
        ),
        diagnostics_summary=BayesDiagnosticsSummary(
            rhat_max=1.01,
            ess_bulk_min=500.0,
            ess_tail_min=400.0,
            divergences=0,
            total_draws=10,
        ),
    )


def _fake_manifest(
    root: Path, model_name: str, artifact_id: str, *, kind: ArtifactKind
) -> Path:
    artifact_dir = root / model_name / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind=kind,
        name=model_name,
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id="src-0001",
        query_hash="dead0001",
        dataset_artifact_id="ds-0001",
        output_paths={"artifact_dir": artifact_dir},
        package_versions=package_versions(),
        bayes_extras=_bayes_extras(model_name) if kind == "bayes" else None,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    return manifest_path


def _publish(model_name: str, artifact_id: str) -> int:
    args = argparse.Namespace(model=model_name, artifact_id=artifact_id)
    return _run_publish_manifest(args)


def _registered_deep_target() -> tuple[str, str]:
    names = deep_registry.all_target_names()
    assert names
    target_name = names[0]
    pointer_name = deep_registry.get_target(target_name).published_manifest_name()
    assert pointer_name != target_name
    return target_name, pointer_name


def test_publish_manifest_deep_uses_published_manifest_name(
    roots: dict[str, Path],
) -> None:
    target_name, pointer_name = _registered_deep_target()
    artifact_id = "deep-fit-0001"
    _ = _fake_manifest(roots["DEEP_ROOT"], target_name, artifact_id, kind="deep")

    rc = _publish(target_name, artifact_id)
    assert rc == 0

    pointer_path = roots["published"] / f"{pointer_name}.json"
    assert pointer_path.exists()
    assert not (roots["published"] / f"{target_name}.json").exists()
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert payload["model_name"] == pointer_name
    assert payload["artifact_id"] == artifact_id

    resolved = find_published_manifest(pointer_name)
    assert resolved == pointer_path
    assert find_published_manifest(target_name) is None


def test_publish_manifest_bayes_keeps_raw_model_name(
    roots: dict[str, Path],
) -> None:
    target_name, _ = _registered_deep_target()
    artifact_id = "bayes-fit-0001"
    _ = _fake_manifest(roots["BAYES_ROOT"], target_name, artifact_id, kind="bayes")

    rc = _publish(target_name, artifact_id)
    assert rc == 0

    pointer_path = roots["published"] / f"{target_name}.json"
    assert pointer_path.exists()
    payload = json.loads(pointer_path.read_text(encoding="utf-8"))
    assert payload["model_name"] == target_name
    assert payload["artifact_id"] == artifact_id


def test_publish_manifest_deep_and_bayes_pointers_do_not_collide(
    roots: dict[str, Path],
) -> None:
    target_name, pointer_name = _registered_deep_target()
    deep_artifact_id = "deep-fit-0002"
    bayes_artifact_id = "bayes-fit-0002"
    _ = _fake_manifest(roots["DEEP_ROOT"], target_name, deep_artifact_id, kind="deep")
    _ = _fake_manifest(
        roots["BAYES_ROOT"], target_name, bayes_artifact_id, kind="bayes"
    )

    assert _publish(target_name, bayes_artifact_id) == 0
    assert _publish(target_name, deep_artifact_id) == 0

    bayes_payload = json.loads(
        (roots["published"] / f"{target_name}.json").read_text(encoding="utf-8")
    )
    deep_payload = json.loads(
        (roots["published"] / f"{pointer_name}.json").read_text(encoding="utf-8")
    )
    assert bayes_payload["artifact_id"] == bayes_artifact_id
    assert deep_payload["artifact_id"] == deep_artifact_id


def test_publish_manifest_deep_unregistered_target_raises(
    roots: dict[str, Path],
) -> None:
    artifact_id = "deep-fit-0003"
    _ = _fake_manifest(
        roots["DEEP_ROOT"], "not_a_registered_target", artifact_id, kind="deep"
    )

    with pytest.raises(ValueError, match="not a registered deep target"):
        _ = _publish("not_a_registered_target", artifact_id)
    assert not (roots["published"] / "not_a_registered_target.json").exists()


def test_publish_manifest_raises_when_artifact_absent(
    roots: dict[str, Path],
) -> None:
    with pytest.raises(FileNotFoundError):
        _ = _publish("nope", "missing")
