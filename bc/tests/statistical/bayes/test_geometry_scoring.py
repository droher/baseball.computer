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
    _posterior_held_out_softmax,
)
from python_models.statistical.models._credit_data import FixedEffectDesign, HeldOutSet
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


def _posterior(
    *,
    with_gamma_dl: bool,
    gamma_value: float = 1.5,
    with_gamma_propensity: bool = False,
    with_gamma_handler: bool = False,
) -> az.InferenceData:
    rng = np.random.default_rng(3)
    n_chain, n_draw = 2, 5
    posterior: dict[str, np.ndarray] = {
        "alpha_class": rng.normal(size=(n_chain, n_draw, N_CLASSES)),
        "delta_result_family": rng.normal(size=(n_chain, n_draw, 3, N_CLASSES)),
    }
    if with_gamma_dl:
        posterior["gamma_dl"] = np.full((n_chain, n_draw), gamma_value)
    if with_gamma_propensity:
        raw = rng.normal(size=(n_chain, n_draw, N_CLASSES))
        posterior["gamma_propensity"] = raw - raw.mean(axis=-1, keepdims=True)
    if with_gamma_handler:
        raw = rng.normal(size=(n_chain, n_draw, N_CLASSES))
        posterior["gamma_handler"] = raw - raw.mean(axis=-1, keepdims=True)
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


def _posterior_arrays(
    idata: az.InferenceData,
) -> tuple[np.ndarray, np.ndarray, np.ndarray | None]:
    posterior = idata.posterior
    alpha = np.asarray(posterior["alpha_class"].values)
    delta = np.asarray(posterior["delta_result_family"].values)
    gamma_propensity = (
        np.asarray(posterior["gamma_propensity"].values)
        if "gamma_propensity" in posterior.data_vars
        else None
    )
    return alpha, delta, gamma_propensity


def _hand_rolled_expected(
    idata: az.InferenceData,
    *,
    rf_codes: np.ndarray,
    propensity_z: np.ndarray | None,
) -> np.ndarray:
    alpha, delta, gamma_propensity = _posterior_arrays(idata)
    n_chain, n_draw, k = alpha.shape
    n = rf_codes.shape[0]
    eta = np.broadcast_to(alpha[:, :, None, :], (n_chain, n_draw, n, k)).copy()
    valid = rf_codes >= 0
    safe = np.where(valid, rf_codes, 0)
    eta += delta[:, :, safe, :] * valid[None, None, :, None]
    if gamma_propensity is not None and propensity_z is not None:
        eta += propensity_z[None, None, :, None] * gamma_propensity[:, :, None, :]
    return _direct_softmax(eta)


def _hand_rolled_expected_handler(
    idata: az.InferenceData,
    *,
    rf_codes: np.ndarray,
    handler_z: np.ndarray | None,
) -> np.ndarray:
    posterior = idata.posterior
    alpha = np.asarray(posterior["alpha_class"].values)
    delta = np.asarray(posterior["delta_result_family"].values)
    gamma_handler = (
        np.asarray(posterior["gamma_handler"].values)
        if "gamma_handler" in posterior.data_vars
        else None
    )
    n_chain, n_draw, k = alpha.shape
    n = rf_codes.shape[0]
    eta = np.broadcast_to(alpha[:, :, None, :], (n_chain, n_draw, n, k)).copy()
    valid = rf_codes >= 0
    safe = np.where(valid, rf_codes, 0)
    eta += delta[:, :, safe, :] * valid[None, None, :, None]
    if gamma_handler is not None and handler_z is not None:
        eta += handler_z[None, None, :, None] * gamma_handler[:, :, None, :]
    return _direct_softmax(eta)


def test_handler_term_matches_hand_rolled_softmax() -> None:
    rng = np.random.default_rng(29)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    carrier = GeometryProductionFrame(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(
                levels=("a", "b", "c"),
                codes=rng.integers(0, 3, size=N_EVENTS).astype(np.int64),
            )
        },
        dl_logit_per_class=np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64),
        handler_z=z,
    )
    idata = _posterior(with_gamma_dl=False, with_gamma_handler=True)

    got = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=z,
    )
    expected = _hand_rolled_expected_handler(
        idata,
        rf_codes=carrier.fixed_effects["result_family"].codes,
        handler_z=z,
    )
    np.testing.assert_allclose(got, expected, atol=1e-12)
    np.testing.assert_allclose(got.sum(axis=1), 1.0, atol=1e-9)

    without_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=None,
    )
    assert not np.allclose(got, without_z)


def test_handler_term_inert_when_gamma_handler_absent() -> None:
    rng = np.random.default_rng(31)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    carrier = GeometryProductionFrame(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(
                levels=("a", "b", "c"),
                codes=rng.integers(0, 3, size=N_EVENTS).astype(np.int64),
            )
        },
        dl_logit_per_class=np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64),
        handler_z=z,
    )
    idata = _posterior(with_gamma_dl=False, with_gamma_handler=False)

    passed_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=z,
    )
    no_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=None,
    )
    np.testing.assert_allclose(passed_z, no_z, atol=1e-12)


def test_handler_term_composes_with_dl_and_propensity() -> None:
    rng = np.random.default_rng(37)
    handler_z = rng.normal(size=N_EVENTS).astype(np.float64)
    prop_z = rng.normal(size=N_EVENTS).astype(np.float64)
    dl = rng.normal(size=(N_EVENTS, N_CLASSES)).astype(np.float64)
    rf_codes = rng.integers(0, 3, size=N_EVENTS).astype(np.int64)
    carrier = GeometryProductionFrame(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
        dl_logit_per_class=dl,
        propensity_z=prop_z,
        handler_z=handler_z,
    )
    n_chain, n_draw = 2, 5
    base = _posterior(with_gamma_dl=True, gamma_value=1.3, with_gamma_propensity=True)
    posterior = {k: np.asarray(v.values) for k, v in base.posterior.data_vars.items()}
    raw = rng.normal(size=(n_chain, n_draw, N_CLASSES))
    posterior["gamma_handler"] = raw - raw.mean(axis=-1, keepdims=True)
    idata = az.from_dict(posterior=posterior)

    got = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        dl_logit_per_class=dl,
        propensity_z=prop_z,
        handler_z=handler_z,
    )

    alpha = posterior["alpha_class"]
    delta = posterior["delta_result_family"]
    eta = np.broadcast_to(
        alpha[:, :, None, :], (n_chain, n_draw, N_EVENTS, N_CLASSES)
    ).copy()
    eta += delta[:, :, rf_codes, :]
    eta += posterior["gamma_dl"][:, :, None, None] * dl[None, None, :, :]
    eta += prop_z[None, None, :, None] * posterior["gamma_propensity"][:, :, None, :]
    eta += handler_z[None, None, :, None] * posterior["gamma_handler"][:, :, None, :]
    expected = _direct_softmax(eta)
    np.testing.assert_allclose(got, expected, atol=1e-12)
    np.testing.assert_allclose(got.sum(axis=1), 1.0, atol=1e-9)


def test_held_out_softmax_handler_parity() -> None:
    rng = np.random.default_rng(41)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    rf_codes = rng.integers(0, 3, size=N_EVENTS).astype(np.int64)
    rf_codes[2] = -1
    held = HeldOutSet(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        true_position=rng.integers(0, N_CLASSES, size=N_EVENTS).astype(np.int64),
        U=np.ones(N_EVENTS, dtype=np.int64),
        season_idx=np.zeros(N_EVENTS, dtype=np.int64),
        scorer_idx=np.zeros(N_EVENTS, dtype=np.int64),
        park_idx=np.zeros(N_EVENTS, dtype=np.int64),
        source_idx=np.zeros(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
    )
    idata = _posterior(with_gamma_dl=False, with_gamma_handler=True)

    got = _posterior_held_out_softmax(
        idata,
        held,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=z,
    )
    expected = _hand_rolled_expected_handler(idata, rf_codes=rf_codes, handler_z=z)
    np.testing.assert_allclose(got, expected, atol=1e-12)

    without_z = _posterior_held_out_softmax(
        idata,
        held,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        handler_z=None,
    )
    assert not np.allclose(got, without_z)


def test_propensity_term_matches_hand_rolled_softmax() -> None:
    rng = np.random.default_rng(13)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    carrier = _geometry_carrier(np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64))
    idata = _posterior(with_gamma_dl=False, with_gamma_propensity=True)

    got = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=z,
    )
    expected = _hand_rolled_expected(
        idata,
        rf_codes=carrier.fixed_effects["result_family"].codes,
        propensity_z=z,
    )
    np.testing.assert_allclose(got, expected, atol=1e-12)
    np.testing.assert_allclose(got.sum(axis=1), 1.0, atol=1e-9)

    without_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=None,
    )
    assert not np.allclose(got, without_z)


def test_propensity_term_inert_when_gamma_propensity_absent() -> None:
    rng = np.random.default_rng(17)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    carrier = _geometry_carrier(np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64))
    idata = _posterior(with_gamma_dl=False, with_gamma_propensity=False)

    passed_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=z,
    )
    no_z = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=None,
    )
    np.testing.assert_allclose(passed_z, no_z, atol=1e-12)


def test_propensity_term_composes_with_unseen_level_mask() -> None:
    rng = np.random.default_rng(19)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    rf_codes = rng.integers(0, 3, size=N_EVENTS).astype(np.int64)
    rf_codes[0] = -1
    rf_codes[3] = -1
    carrier = GeometryProductionFrame(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
        dl_logit_per_class=np.zeros((N_EVENTS, N_CLASSES), dtype=np.float64),
        propensity_z=z,
    )
    idata = _posterior(with_gamma_dl=False, with_gamma_propensity=True)

    got = _posterior_event_softmax(
        idata,
        carrier,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=z,
    )
    expected = _hand_rolled_expected(idata, rf_codes=rf_codes, propensity_z=z)
    np.testing.assert_allclose(got, expected, atol=1e-12)

    masked_only = _hand_rolled_expected(
        idata, rf_codes=np.full(N_EVENTS, -1, dtype=np.int64), propensity_z=z
    )
    np.testing.assert_allclose(got[[0, 3]], masked_only[[0, 3]], atol=1e-12)
    no_z_masked = _hand_rolled_expected(
        idata, rf_codes=np.full(N_EVENTS, -1, dtype=np.int64), propensity_z=None
    )
    assert not np.allclose(got[[0, 3]], no_z_masked[[0, 3]]), (
        "the propensity term must stay active on rows whose FE codes are masked"
    )


def test_held_out_softmax_propensity_parity() -> None:
    rng = np.random.default_rng(23)
    z = rng.normal(size=N_EVENTS).astype(np.float64)
    rf_codes = rng.integers(0, 3, size=N_EVENTS).astype(np.int64)
    rf_codes[1] = -1
    held = HeldOutSet(
        event_keys=np.arange(N_EVENTS, dtype=np.int64),
        true_position=rng.integers(0, N_CLASSES, size=N_EVENTS).astype(np.int64),
        U=np.ones(N_EVENTS, dtype=np.int64),
        season_idx=np.zeros(N_EVENTS, dtype=np.int64),
        scorer_idx=np.zeros(N_EVENTS, dtype=np.int64),
        park_idx=np.zeros(N_EVENTS, dtype=np.int64),
        source_idx=np.zeros(N_EVENTS, dtype=np.int64),
        fixed_effects={
            "result_family": FixedEffectDesign(levels=("a", "b", "c"), codes=rf_codes)
        },
    )
    idata = _posterior(with_gamma_dl=False, with_gamma_propensity=True)

    got = _posterior_held_out_softmax(
        idata,
        held,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=z,
    )
    expected = _hand_rolled_expected(idata, rf_codes=rf_codes, propensity_z=z)
    np.testing.assert_allclose(got, expected, atol=1e-12)

    without_z = _posterior_held_out_softmax(
        idata,
        held,
        n_positions=N_CLASSES,
        intercept_name="alpha_class",
        propensity_z=None,
    )
    assert not np.allclose(got, without_z)


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
        geometry_dimension="trajectory",
        target_path=target,
    )

    assert df.schema["event_key"] == pl.Int64
    assert df.schema["geometry_dimension"] == pl.Utf8
    assert df.schema["class_index"] == pl.Int8
    assert df.schema["class_label"] == pl.Utf8
    assert df.schema["expected_share"] == pl.Float64
    assert df.height == N_EVENTS * N_CLASSES
    assert df.get_column("geometry_dimension").unique().to_list() == ["trajectory"]

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
