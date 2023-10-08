import duckdb
import polars as pl
import pyarrow as pa
from pyarrow import RecordBatchReader

def model(dbt, session):
    print("Starting")
    o: duckdb.DuckDBPyRelation = dbt.ref("ml_features")
    batch_reader: pa.RecordBatchReader = o.record_batch(10000)
    def gen():
        for batch in batch_reader:
            print("batch")
            df = pl.DataFrame.from_arrow(batch)
            # Do some operations...
            # ... then convert back to arrow
            yield df.to_arrow() 
    new_reader = RecordBatchReader.from_batches(batch_reader.schema, [])
    return new_reader