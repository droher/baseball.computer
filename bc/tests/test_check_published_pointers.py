"""The CI pointer check fails fast on an unset root or unreachable manifests."""

from __future__ import annotations

import importlib.util
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from python_models.statistical import config as cfg

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "check_published_pointers.py"


@pytest.fixture(scope="module")
def script() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"check_published_pointers_test_{uuid.uuid4().hex}", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_pointer(published_root: Path, name: str, manifest_path: Path) -> Path:
    published_root.mkdir(parents=True, exist_ok=True)
    target = published_root / f"{name}.json"
    target.write_text(
        json.dumps(
            {
                "model_name": name,
                "artifact_id": f"{name}-v1",
                "published_at": datetime.now(tz=timezone.utc).isoformat(),
                "manifest_path": str(manifest_path),
                "notes": None,
            }
        ),
        encoding="utf-8",
    )
    return target


def test_unset_root_fails(script: Any) -> None:
    with pytest.raises(SystemExit, match=cfg.ENV_ARTIFACTS_ROOT):
        script.artifacts_root_from_env({})
    with pytest.raises(SystemExit, match="unset"):
        script.artifacts_root_from_env({cfg.ENV_ARTIFACTS_ROOT: "  "})


def test_relative_or_missing_root_fails(script: Any, tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="absolute"):
        script.artifacts_root_from_env(
            {cfg.ENV_ARTIFACTS_ROOT: "artifacts/statistical"}
        )
    with pytest.raises(SystemExit, match="not a directory"):
        script.artifacts_root_from_env({cfg.ENV_ARTIFACTS_ROOT: str(tmp_path / "nope")})
    assert (
        script.artifacts_root_from_env({cfg.ENV_ARTIFACTS_ROOT: str(tmp_path)})
        == tmp_path
    )


def test_no_pointers_fails(script: Any, tmp_path: Path) -> None:
    published = tmp_path / "published"
    published.mkdir()
    with pytest.raises(SystemExit, match="no published pointers"):
        script.check_pointers(published)


def test_missing_manifest_fails(script: Any, tmp_path: Path) -> None:
    published = tmp_path / "published"
    _write_pointer(published, "gone", tmp_path / "bayes" / "gone" / "manifest.json")
    with pytest.raises(SystemExit, match=r"gone\.json"):
        script.check_pointers(published)


def test_resolvable_pointers_pass(
    script: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = tmp_path / "canonical"
    absolute_manifest = root / "bayes" / "abs" / "manifest.json"
    relative_manifest = root / "bayes" / "rel" / "manifest.json"
    for manifest in (absolute_manifest, relative_manifest):
        manifest.parent.mkdir(parents=True)
        manifest.write_text("{}", encoding="utf-8")
    published = root / "published"
    _write_pointer(published, "abs", absolute_manifest)
    _write_pointer(published, "rel", Path("bayes/rel/manifest.json"))
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(root))

    assert script.check_pointers(published, verify_evidence=False) == [
        absolute_manifest,
        relative_manifest,
    ]
