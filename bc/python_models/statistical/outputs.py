"""Atomic Parquet writes + NetCDF/Zarr export wrappers."""

from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    import arviz as az

_log = logging.getLogger(__name__)


def write_parquet_atomic(df: pl.DataFrame, path: Path) -> None:
    """Write a Polars DataFrame to Parquet via temp-file rename.

    Same directory as the target so the rename stays on one filesystem.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.stem}.", suffix=".parquet"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        df.write_parquet(tmp_path)
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def write_netcdf(idata: "az.InferenceData", path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.stem}.", suffix=".nc"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        idata.to_netcdf(str(tmp_path))
        os.replace(tmp_path, path)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def write_zarr(idata: "az.InferenceData", path: Path) -> None:
    raise NotImplementedError("zarr export wiring lands with the first PyMC fit")
