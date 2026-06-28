"""Read-only DuckDB connections and Arrow batch helpers."""

from __future__ import annotations

import logging
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl
import pyarrow as pa

from python_models.statistical.config import resolve_db_path

_log = logging.getLogger(__name__)


@contextmanager
def open_bc_db(
    db_path: str | Path | None = None,
    *,
    read_only: bool = True,
    attach_as: str | None = "bc",
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open the baseball.computer DuckDB.

    Per-branch env views (``main_models__<slug>.*``) and modeling
    dataset views fully-qualify joins against the ``bc`` catalog, so by
    default we ATTACH the database as ``bc`` from an in-memory
    connection. Pass ``attach_as=None`` to skip the indirection when a
    bare connection is enough.
    """
    path = Path(db_path) if db_path is not None else resolve_db_path()
    if attach_as is None:
        con = duckdb.connect(str(path), read_only=read_only)
        try:
            yield con
        finally:
            con.close()
        return
    con = duckdb.connect(":memory:")
    try:
        suffix = " (READ_ONLY)" if read_only else ""
        con.execute(f"ATTACH '{path}' AS {attach_as}{suffix}")
        con.execute(f"USE {attach_as}")
        yield con
    finally:
        con.close()


def stream_query(
    con: duckdb.DuckDBPyConnection,
    query: str,
    *,
    rows_per_batch: int = 500_000,
) -> Iterator[pl.DataFrame]:
    cursor = con.cursor()
    try:
        reader: pa.RecordBatchReader = cursor.execute(query).fetch_record_batch(
            rows_per_batch=rows_per_batch
        )
        for idx, record_batch in enumerate(reader):
            df_or_series = pl.from_arrow(record_batch)
            assert isinstance(df_or_series, pl.DataFrame)
            _log.debug("stream_query batch %d (%d rows)", idx, df_or_series.height)
            yield df_or_series
    finally:
        cursor.close()


def scalar(con: duckdb.DuckDBPyConnection, query: str) -> object:
    row = con.execute(query).fetchone()
    if row is None:
        return None
    return row[0]
