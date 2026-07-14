"""Publication-tier posterior-shift diagnostic for the gamma_dl ablation.

Compares two geometry fits of the same dimension and dataset — a
``gamma_dl_zero`` counterfactual and its published ``gamma_dl_shrunk``
sibling — and reports, per publication-tier structured-effect block, the
distribution of ``|mean_shrunk - mean_zero| / posterior_sd`` across the
block's cells plus the share of cells whose shift exceeds a threshold
(default 0.25 SD).

After the canceling-random-effect removal the geometry model
(``models/geometry.py``) carries no season / scorer / park random-effect
blocks: scalar-per-event terms enter every class logit equally and cancel
inside the per-event softmax, so they were dropped. The effect blocks that
survive and drive the published imputation distribution are the per-class
intercept ``alpha_class`` and the per-fixed-effect interaction
``delta_<fe>`` tensors. Those are the publication-tier blocks this
diagnostic compares. The ``gamma_*`` covariate coefficients are excluded:
``gamma_dl`` exists only in the shrunk fit (it is the covariate under
ablation, not a shared effect block), and the other gamma hooks are inert
at the published operating point.

The normalizing denominator is the pooled posterior SD
``sqrt((sd_zero**2 + sd_shrunk**2) / 2)`` — symmetric in the two fits and
on the same scale as each fit's own posterior uncertainty, so a normalized
shift of 1.0 means the posterior mean moved by one posterior standard
deviation.
"""

from __future__ import annotations

import logging
from pathlib import Path

import arviz as az
import numpy as np
import numpy.typing as npt
import xarray as xr
from pydantic import BaseModel

_log = logging.getLogger(__name__)

DEFAULT_SHIFT_THRESHOLD_SD: float = 0.25
_SAMPLE_DIMS: tuple[str, str] = ("chain", "draw")


class BlockShift(BaseModel):
    """Per-block summary of normalized posterior-mean shifts across cells."""

    block: str
    n_cells: int
    mean_abs_shift_sd: float
    median_abs_shift_sd: float
    p90_abs_shift_sd: float
    max_abs_shift_sd: float
    share_over_threshold: float


class ShiftReport(BaseModel):
    """Dimension-level gamma_dl posterior-shift report."""

    zero_artifact: str
    shrunk_artifact: str
    threshold_sd: float
    blocks: list[BlockShift]
    overall_n_cells: int
    overall_share_over_threshold: float
    overall_mean_abs_shift_sd: float
    overall_max_abs_shift_sd: float
    most_cells_over_threshold: bool


def is_publication_tier_block(name: str) -> bool:
    """A posterior variable is a publication-tier effect block unless it is a covariate coefficient."""
    return not name.startswith("gamma_")


def normalized_abs_shift(
    mean_zero: npt.NDArray[np.float64],
    sd_zero: npt.NDArray[np.float64],
    mean_shrunk: npt.NDArray[np.float64],
    sd_shrunk: npt.NDArray[np.float64],
) -> npt.NDArray[np.float64]:
    """Elementwise ``|mean_shrunk - mean_zero| / pooled_sd`` with a pooled posterior SD."""
    pooled_sd = np.sqrt((np.square(sd_zero) + np.square(sd_shrunk)) / 2.0)
    abs_shift = np.abs(mean_shrunk - mean_zero)
    return np.asarray(abs_shift / pooled_sd, dtype=np.float64)


def _summarize_block(
    block: str,
    normalized: npt.NDArray[np.float64],
    threshold_sd: float,
) -> BlockShift:
    flat = normalized.ravel()
    return BlockShift(
        block=block,
        n_cells=int(flat.size),
        mean_abs_shift_sd=float(np.mean(flat)),
        median_abs_shift_sd=float(np.median(flat)),
        p90_abs_shift_sd=float(np.quantile(flat, 0.9)),
        max_abs_shift_sd=float(np.max(flat)),
        share_over_threshold=float(np.mean(flat > threshold_sd)),
    )


def _shared_blocks(zero: xr.Dataset, shrunk: xr.Dataset) -> list[str]:
    shared = {str(name) for name in zero.data_vars} & {
        str(name) for name in shrunk.data_vars
    }
    return sorted(name for name in shared if is_publication_tier_block(name))


def compute_shift_report(
    zero_posterior: xr.Dataset,
    shrunk_posterior: xr.Dataset,
    *,
    zero_artifact: str,
    shrunk_artifact: str,
    threshold_sd: float = DEFAULT_SHIFT_THRESHOLD_SD,
) -> ShiftReport:
    """Build a :class:`ShiftReport` from two posterior ``xarray`` datasets."""
    blocks_summ: list[BlockShift] = []
    all_normalized: list[npt.NDArray[np.float64]] = []
    for block in _shared_blocks(zero_posterior, shrunk_posterior):
        z = zero_posterior[block]
        s = shrunk_posterior[block]
        if z.shape != s.shape:
            raise ValueError(
                f"block {block} shape mismatch: zero={z.shape} shrunk={s.shape}"
            )
        mean_zero = z.mean(dim=_SAMPLE_DIMS).to_numpy()
        sd_zero = z.std(dim=_SAMPLE_DIMS).to_numpy()
        mean_shrunk = s.mean(dim=_SAMPLE_DIMS).to_numpy()
        sd_shrunk = s.std(dim=_SAMPLE_DIMS).to_numpy()
        normalized = normalized_abs_shift(mean_zero, sd_zero, mean_shrunk, sd_shrunk)
        all_normalized.append(normalized.ravel())
        blocks_summ.append(_summarize_block(str(block), normalized, threshold_sd))

    if not all_normalized:
        raise ValueError("no shared publication-tier blocks between the two posteriors")

    pooled = np.concatenate(all_normalized)
    overall_share = float(np.mean(pooled > threshold_sd))
    return ShiftReport(
        zero_artifact=zero_artifact,
        shrunk_artifact=shrunk_artifact,
        threshold_sd=threshold_sd,
        blocks=blocks_summ,
        overall_n_cells=int(pooled.size),
        overall_share_over_threshold=overall_share,
        overall_mean_abs_shift_sd=float(np.mean(pooled)),
        overall_max_abs_shift_sd=float(np.max(pooled)),
        most_cells_over_threshold=overall_share > 0.5,
    )


def load_posterior(artifact_dir: Path) -> xr.Dataset:
    """Load the ``posterior`` group of a bayes artifact's ``inference/posterior.nc``."""
    nc_path = artifact_dir / "inference" / "posterior.nc"
    if not nc_path.exists():
        raise FileNotFoundError(f"posterior netCDF missing at {nc_path}")
    idata = az.from_netcdf(nc_path)
    return idata["posterior"]


def report_from_artifact_dirs(
    zero_dir: Path,
    shrunk_dir: Path,
    *,
    threshold_sd: float = DEFAULT_SHIFT_THRESHOLD_SD,
) -> ShiftReport:
    """Load both artifacts' posteriors from disk and build the shift report."""
    zero_post = load_posterior(zero_dir)
    shrunk_post = load_posterior(shrunk_dir)
    report = compute_shift_report(
        zero_post,
        shrunk_post,
        zero_artifact=zero_dir.name,
        shrunk_artifact=shrunk_dir.name,
        threshold_sd=threshold_sd,
    )
    _log.info(
        "gamma_dl shift zero=%s shrunk=%s cells=%d share>%.2fSD=%.4f max=%.3f most_cells=%s",
        report.zero_artifact,
        report.shrunk_artifact,
        report.overall_n_cells,
        report.threshold_sd,
        report.overall_share_over_threshold,
        report.overall_max_abs_shift_sd,
        report.most_cells_over_threshold,
    )
    return report
