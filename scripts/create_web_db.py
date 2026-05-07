import duckdb
import boto3
import os
import time

def export_table_to_parquet(conn, schema_name, table_name, file_name):
    row_group_size = 1966080 if table_name != "event_states_full" else 262144
    compression = "ZSTD" if table_name != "event_states_full" else "GZIP"
    query = f"COPY (SELECT * FROM {schema_name}.{table_name}) TO '{file_name}' (FORMAT 'parquet', COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group_size})"
    conn.sql(query)
    print(f"Exported {schema_name}.{table_name} to {file_name}")

def get_url(file_name, prefix, cache_bust=None):
    name_only = file_name.split("/")[-1]
    url = f"https://data.baseball.computer/{prefix}/{name_only}"
    if cache_bust:
        url = f"{url}?v={cache_bust}"
    return url

def upload_to_r2(file_name, bucket_name, prefix, cache_bust=None):
    print(f"Uploading {file_name} to R2 bucket {bucket_name}")
    account_id = os.environ["R2_ACCOUNT_ID"]
    access_key_id = os.environ["R2_ACCESS_KEY_ID"]
    secret_access_key = os.environ["R2_SECRET_ACCESS_KEY"]
    s3 = boto3.resource(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
    )
    name_only = file_name.split("/")[-1]
    s3.meta.client.upload_file(file_name, bucket_name, f"{prefix}/{name_only}")
    return get_url(file_name, prefix, cache_bust=cache_bust)


def create_view_with_url(new_conn, schema_name, view_name, url):
    new_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    query = f"CREATE VIEW {schema_name}.{view_name} AS SELECT * FROM '{url}'"
    print(query)
    new_conn.execute(query)


REQUIRED_SCHEMAS = {"main_models", "main_seeds"}


def main():
    original_db_path = "bc.db"
    new_db_path = "bc_remote.db"
    if os.path.exists(new_db_path):
        os.remove(new_db_path)
    bucket_name = "timeball"
    prefix = "dbt"
    cache_bust = str(int(time.time()))

    conn = duckdb.connect(original_db_path)
    conn.execute("SET memory_limit='25GB'")

    schema_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('main', 'pg_catalog', 'information_schema')"
    schemas = [row[0] for row in conn.execute(schema_query).fetchall()]

    missing = REQUIRED_SCHEMAS - set(schemas)
    if missing:
        raise RuntimeError(
            f"refusing to publish: {original_db_path} (cwd={os.getcwd()}, "
            f"size={os.path.getsize(original_db_path)} bytes) is missing required "
            f"schemas {sorted(missing)}; found only {sorted(schemas)}"
        )

    schema_tables: dict[str, list[str]] = {}
    for schema_name in schemas:
        tables_query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        tables = [row[0] for row in conn.execute(tables_query).fetchall()]
        if not tables:
            raise RuntimeError(f"refusing to publish: schema {schema_name} has no tables")
        schema_tables[schema_name] = tables

    for schema_name, tables in schema_tables.items():
        for table_name in tables:
            parquet_file = f"/tmp/{schema_name}_{table_name}.parquet"
            export_table_to_parquet(conn, schema_name, table_name, parquet_file)

    new_conn = duckdb.connect(new_db_path)
    for schema_name, tables in schema_tables.items():
        for table_name in tables:
            parquet_file = f"/tmp/{schema_name}_{table_name}.parquet"
            url = upload_to_r2(parquet_file, bucket_name, prefix, cache_bust=cache_bust)
            print(f"URL: {url}")
            create_view_with_url(new_conn, schema_name, table_name, url)
            os.remove(parquet_file)

    conn.close()
    new_conn.close()
    url = upload_to_r2(new_db_path, bucket_name, prefix)
    print(f"URL: {url}")


if __name__ == "__main__":
    main()
