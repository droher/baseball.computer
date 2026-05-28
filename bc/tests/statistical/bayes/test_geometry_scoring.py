"""Scoring-primitive tests for the geometry model (Model E)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

from pathlib import Path

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.training import (
    _export_geometry_probabilities,
    _posterior_event_softmax,
)
from python_models.statistical.models._credit_data import FixedEffectDesign
from python_models.statistical.models._geometry_data import GeometryProductionFrame

N_CLASSES = 4
N_EVENTS = 6


def _geometry_carrier(dl_logit_per_class: np.ndarray) -> GeometryProductionFrame:
    rng = np.random.default_rng(7)
    rf_codes = rng.integers(0, 3, size=N_EVENTS).astype(np.int64)
    return GeometryProductionFrame(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
        dl_logit_per_class=dl_logit_per_class,
    )


def _posterior(*, with_gamma_dl: bool, gamma_value: float = 1.5) -> az.InferenceData:
    rng = np.random.default_rng(3)
    n_chain, n_draw = 2, 5
    posterior: dict[str, np.ndarray] = {
        "alpha_class": rng.normal(size=(n_chain, n_draw, N_CLASSES)),
        "delta_result_family": rng.normal(size=(n_chain, n_draw, 3, N_CLASSES)),
    }
    if with_gamma_dl:
        posterior["gamma_dl"] = np.full((n_chain, n_draw), gamma_value)
    return az.from_dict(posterior=posterior)


def test_dl_term_shifts_per_event_shares() -> None:
    dl = np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64)
    dl[:, 0] = 5.0
    carrier = _geometry_carrier(dl)
    idata = _posterior(with_gamma_dl=True, gamma_value=2.0)

    without_dl = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        dl_logit_per_class=None,
    )
    with_dl = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        dl_logit_per_class=dl,
    )

    np.testing.assert_allclose(without_dl.sum(axis=1), 1.0, atol=1e-9)
    np.testing.assert_allclose(with_dl.sum(axis=1), 1.0, atol=1e-9)
    assert not np.allclose(without_dl, with_dl)

    diff = with_dl - without_dl
    assert not np.allclose(diff, diff[0], atol=1e-6)
    assert (np.argmax(with_dl, axis=1) == 0).all()
    assert not (np.argmax(without_dl, axis=1) == 0).all()


def test_dl_term_inert_when_gamma_dl_absent() -> None:
    dl = np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64)
    dl[:, 0] = 5.0
    carrier = _geometry_carrier(dl)
    idata = _posterior(with_gamma_dl=False)

    passed_dl = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        dl_logit_per_class=dl,
    )
    no_dl = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        dl_logit_per_class=None,
    )
    np.testing.assert_allclose(passed_dl, no_dl, atol=1e-12)


def _credit_carrier(rf_codes: np.ndarray) -> GeometryProductionFrame:
    return GeometryProductionFrame(
        event_keys=np.arange(rf_codes.shape[0], dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
        dl_logit_per_class=np.zeros((rf_codes.shape[0], 9), dtype=np.float64),
    )


def _direct_softmax(eta: np.ndarray) -> np.ndarray:
    eta = eta - eta.max(axis=-1, keepdims=True)
    exp = np.exp(eta)
    return (exp / exp.sum(axis=-1, keepdims=True)).mean(axis=(0, 1))


def test_backward_compat_default_intercept_no_dl() -> None:
    rng = np.random.default_rng(11)
    n_chain, n_draw, k = 2, 5, 9
    n = 7
    rf_codes = rng.integers(0, 3, size=n).astype(np.int64)
    alpha = rng.normal(size=(n_chain, n_draw, k))
    delta = rng.normal(size=(n_chain, n_draw, 3, k))
    idata = az.from_dict(
        posterior={"alpha_position": alpha, "delta_result_family": delta}
    )
    carrier = _credit_carrier(rf_codes)

    got = _posterior_event_softmax(idata, carrier, n_positions=k)

    eta = np.broadcast_to(alpha[:, :, None, :], (n_chain, n_draw, n, k)).copy()
    eta += delta[:, :, rf_codes, :]
    expected = _direct_softmax(eta)
    np.testing.assert_allclose(got, expected, atol=1e-12)


def test_export_geometry_probabilities_schema_and_sum(tmp_path: Path) -> None:
    rng = np.random.default_rng(5)
    raw = rng.random(size=(N_EVENTS, N_CLASSES))
    means = raw / raw.sum(axis=1, keepdims=True)
    event_keys = np.arange(100, 100 + N_EVENTS, dtype=np.int64)
    class_labels = [f"class_{k}" for k in range(N_CLASSES)]
    target = tmp_path / "geometry_probabilities.parquet"

    df = _export_geometry_probabilities(
        means,
        event_keys=event_keys,
        class_labels=class_labels,
        target_path=target,
    )

    assert df.schema["event_key"] == pl.Int64
    assert df.schema["class_index"] == pl.Int8
    assert df.schema["class_label"] == pl.Utf8
    assert df.schema["expected_share"] == pl.Float64
    assert df.height == N_EVENTS * N_CLASSES

    per_event = df.group_by("event_key").agg(pl.col("expected_share").sum())
    np.testing.assert_allclose(
        per_event.get_column("expected_share").to_numpy(), 1.0, atol=1e-9
    )

    first = df.filter(pl.col("event_key") == 100).sort("class_index")
    assert first.get_column("class_index").to_list() == list(range(N_CLASSES))
    assert first.get_column("class_label").to_list() == class_labels

    assert target.exists()
    reread = pl.read_parquet(target)
    assert reread.height == N_EVENTS * N_CLASSES
