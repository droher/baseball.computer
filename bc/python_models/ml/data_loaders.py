"""DuckDB streaming generators for Hamilton ML DAGs.

Streams `ml_features` rows in Arrow record batches so peak Python
memory stays at one batch's worth of rows. Mirrors the chunking idiom
from the deleted `keras_sandbox.ipynb` (commit ebe9386), now keyed off
DuckDB's native `fetch_record_batch` rather than building a `tf.data`
pipeline on top.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from collections.abc import Generator

import duckdb
import polars as pl
import pyarrow as pa

_log = logging.getLogger(__name__)


@contextmanager
def open_bc_db(
    db_path: str | Path = "bc.db", *, read_only: bool = True
) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    con = duckdb.connect(str(db_path), read_only=read_only)
    try:
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
