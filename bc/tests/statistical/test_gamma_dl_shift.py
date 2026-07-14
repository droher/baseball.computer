"""gamma_dl publication-tier posterior-shift diagnostic on synthetic posteriors."""

from __future__ import annotations

import numpy as np
import pytest
import xarray as xr

from python_models.statistical.gamma_dl_shift import (
    compute_shift_report,
    is_publication_tier_block,
    normalized_abs_shift,
)


def _block(
    name: str,
    values: np.ndarray,
    extra_dim: str,
) -> xr.DataArray:
    n_chain, n_draw, n_cell = values.shape
    return xr.DataArray(
        values,
        dims=("chain", "draw", extra_dim),
        coords={
            "chain": np.arange(n_chain),
            "draw": np.arange(n_draw),
            extra_dim: [f"{extra_dim}{i}" for i in range(n_cell)],
        },
        name=name,
    )


def _constant_sd_samples(
    means: np.ndarray, sd: float, n_chain: int = 2, n_draw: int = 500
) -> np.ndarray:
    """Draw samples whose per-cell posterior SD equals ``sd`` and mean equals ``means``."""
    rng = np.random.default_rng(0)
    n_cell = means.size
    raw = rng.standard_normal((n_chain, n_draw, n_cell))
    raw = (raw - raw.mean(axis=(0, 1))) / raw.std(axis=(0, 1))
    return raw * sd + means[None, None, :]


def test_normalized_abs_shift_matches_hand_computation() -> None:
    mean_zero = np.array([0.0, 1.0, 2.0])
    mean_shrunk = np.array([0.5, 1.0, 0.0])
    sd_zero = np.array([1.0, 2.0, 1.0])
    sd_shrunk = np.array([1.0, 2.0, 1.0])
    out = normalized_abs_shift(mean_zero, sd_zero, mean_shrunk, sd_shrunk)
    np.testing.assert_allclose(out, [0.5, 0.0, 2.0])


def test_normalized_abs_shift_uses_pooled_sd() -> None:
    out = normalized_abs_shift(
        np.array([0.0]), np.array([3.0]), np.array([2.0]), np.array([1.0])
    )
    pooled = np.sqrt((9.0 + 1.0) / 2.0)
    np.testing.assert_allclose(out, [2.0 / pooled])


def test_is_publication_tier_block_excludes_gamma() -> None:
    assert is_publication_tier_block("alpha_class")
    assert is_publication_tier_block("delta_result_family")
    assert not is_publication_tier_block("gamma_dl")
    assert not is_publication_tier_block("gamma_propensity")


def test_report_recovers_known_shift_fraction() -> None:
    sd = 1.0
    zero_means = np.zeros(10)
    shrunk_means = np.array([0.3] * 6 + [0.1] * 4) * sd
    zero = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(zero_means, sd), "class")}
    )
    shrunk = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(shrunk_means, sd), "class")}
    )
    report = compute_shift_report(
        zero, shrunk, zero_artifact="z", shrunk_artifact="s", threshold_sd=0.25
    )
    assert report.overall_n_cells == 10
    block = report.blocks[0]
    assert block.share_over_threshold == pytest.approx(0.6, abs=1e-6)
    assert report.overall_share_over_threshold == pytest.approx(0.6, abs=1e-6)
    assert report.most_cells_over_threshold is True
    np.testing.assert_allclose(block.max_abs_shift_sd, 0.3, atol=1e-6)


def test_report_below_threshold_does_not_flag() -> None:
    sd = 2.0
    zero_means = np.zeros(8)
    shrunk_means = np.full(8, 0.1) * sd
    zero = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(zero_means, sd), "class")}
    )
    shrunk = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(shrunk_means, sd), "class")}
    )
    report = compute_shift_report(
        zero, shrunk, zero_artifact="z", shrunk_artifact="s", threshold_sd=0.25
    )
    assert report.overall_share_over_threshold == pytest.approx(0.0)
    assert report.most_cells_over_threshold is False
    np.testing.assert_allclose(report.overall_max_abs_shift_sd, 0.1, atol=1e-6)


def test_report_excludes_gamma_and_unshared_blocks() -> None:
    sd = 1.0
    means = np.zeros(4)
    zero = xr.Dataset(
        {
            "alpha_class": _block("alpha_class", _constant_sd_samples(means, sd), "class"),
        }
    )
    shrunk = xr.Dataset(
        {
            "alpha_class": _block("alpha_class", _constant_sd_samples(means, sd), "class"),
            "gamma_dl": xr.DataArray(
                np.zeros((2, 500)),
                dims=("chain", "draw"),
                coords={"chain": np.arange(2), "draw": np.arange(500)},
            ),
            "delta_only_in_shrunk": _block(
                "delta_only_in_shrunk", _constant_sd_samples(means, sd), "class"
            ),
        }
    )
    report = compute_shift_report(
        zero, shrunk, zero_artifact="z", shrunk_artifact="s"
    )
    assert [b.block for b in report.blocks] == ["alpha_class"]


def test_report_multiple_blocks_pools_cells() -> None:
    sd = 1.0
    a_shift = np.array([0.3, 0.3, 0.3])
    d_shift = np.array([0.0, 0.0, 0.0, 0.0, 0.0])
    zero = xr.Dataset(
        {
            "alpha_class": _block(
                "alpha_class", _constant_sd_samples(np.zeros(3), sd), "class"
            ),
            "delta_x": _block("delta_x", _constant_sd_samples(np.zeros(5), sd), "level"),
        }
    )
    shrunk = xr.Dataset(
        {
            "alpha_class": _block(
                "alpha_class", _constant_sd_samples(a_shift * sd, sd), "class"
            ),
            "delta_x": _block("delta_x", _constant_sd_samples(d_shift * sd, sd), "level"),
        }
    )
    report = compute_shift_report(
        zero, shrunk, zero_artifact="z", shrunk_artifact="s", threshold_sd=0.25
    )
    assert report.overall_n_cells == 8
    assert report.overall_share_over_threshold == pytest.approx(3 / 8, abs=1e-6)
    assert report.most_cells_over_threshold is False


def test_shape_mismatch_raises() -> None:
    zero = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(np.zeros(3), 1.0), "class")}
    )
    shrunk = xr.Dataset(
        {"alpha_class": _block("alpha_class", _constant_sd_samples(np.zeros(4), 1.0), "class")}
    )
    with pytest.raises(ValueError, match="shape mismatch"):
        _ = compute_shift_report(zero, shrunk, zero_artifact="z", shrunk_artifact="s")


def test_no_shared_blocks_raises() -> None:
    zero = xr.Dataset(
        {"gamma_dl": xr.DataArray(np.zeros((2, 10)), dims=("chain", "draw"))}
    )
    shrunk = xr.Dataset(
        {"gamma_dl": xr.DataArray(np.zeros((2, 10)), dims=("chain", "draw"))}
    )
    with pytest.raises(ValueError, match="no shared publication-tier blocks"):
        _ = compute_shift_report(zero, shrunk, zero_artifact="z", shrunk_artifact="s")
