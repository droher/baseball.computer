"""The assist model's putout-posterior lookup follows either pointer form."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.bayes.training import (
    PUTOUT_MARGINALIZATION_MODEL,
    _resolve_published_putout_idata,
)
from python_models.statistical.manifests import write_published_pointer
from python_models.statistical.schemas import PublishedPointer


def _publish_putout_fit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    relative_pointer: bool,
    with_posterior: bool,
) -> Path:
    import arviz as az

    artifacts_root = tmp_path / "canonical"
    artifact_dir = artifacts_root / "bayes" / PUTOUT_MARGINALIZATION_MODEL / "po-1"
    artifact_dir.mkdir(parents=True)
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(json.dumps({"artifact_id": "po-1"}), encoding="utf-8")
    posterior_path = artifact_dir / "inference" / "posterior.nc"
    if with_posterior:
        posterior_path.parent.mkdir(parents=True)
        rng = np.random.default_rng(0)
        idata = az.from_dict(posterior={"alpha_position": rng.normal(size=(2, 5, 9))})
        idata.to_netcdf(str(posterior_path))
    monkeypatch.setenv(cfg.ENV_ARTIFACTS_ROOT, str(artifacts_root))
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    _ = write_published_pointer(
        PublishedPointer(
            model_name=PUTOUT_MARGINALIZATION_MODEL,
            artifact_id="po-1",
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=manifest_path,
        ),
        root=tmp_path / "published",
        relative=relative_pointer,
    )
    return posterior_path


@pytest.mark.parametrize("relative_pointer", [True, False])
def test_putout_posterior_resolves_under_either_pointer_form(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative_pointer: bool
) -> None:
    _ = _publish_putout_fit(
        tmp_path, monkeypatch, relative_pointer=relative_pointer, with_posterior=True
    )
    stored = json.loads(
        (tmp_path / "published" / f"{PUTOUT_MARGINALIZATION_MODEL}.json").read_text(
            encoding="utf-8"
        )
    )
    assert Path(stored["manifest_path"]).is_absolute() is not relative_pointer

    idata = _resolve_published_putout_idata()

    assert idata is not None
    posterior = idata["posterior"]
    assert "alpha_position" in posterior.data_vars
    assert posterior["alpha_position"].shape == (2, 5, 9)


def test_missing_posterior_behind_a_relative_pointer_returns_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    posterior_path = _publish_putout_fit(
        tmp_path, monkeypatch, relative_pointer=True, with_posterior=False
    )
    assert not posterior_path.exists()
    assert _resolve_published_putout_idata() is None


def test_no_pointer_returns_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    assert _resolve_published_putout_idata() is None
