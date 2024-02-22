import duckdb
import boto3
import os

upcast_map = {
    ""
}

def export_table_to_parquet(conn, schema_name, table_name, file_name):
    query = f"COPY (SELECT * FROM {schema_name}.{table_name}) TO '{file_name}' (FORMAT 'parquet', COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1966080)"
    conn.sql(query)
    print(f"Exported {schema_name}.{table_name} to {file_name}")


def upload_to_r2(file_name, bucket_name, prefix):
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
    url = f"https://data.baseball.computer/{prefix}/{name_only}"
    return url


def create_view_with_url(new_conn, schema_name, view_name, url):
    new_conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    query = f"CREATE VIEW {schema_name}.{view_name} AS SELECT * FROM '{url}'"
    new_conn.execute(query)


def main():
    original_db_path = "bc.db"
    new_db_path = "bc_remote.db"
    bucket_name = "timeball"
    prefix = "dbt"

    conn = duckdb.connect(original_db_path)
    new_conn = duckdb.connect(new_db_path)
    conn.execute("SET memory_limit='25GB'")

    schema_query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('main', 'pg_catalog', 'information_schema')"
    # Retrieve list of schemas
    schemas = conn.execute(schema_query).fetchall()

    for schema in schemas:
        schema_name = schema[0]
        # Retrieve list of tables in each schema
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        tables = conn.execute(query).fetchall()

        for table in tables:
            table_name = table[0]
            parquet_file = f"/tmp/{schema_name}_{table_name}.parquet"

            # Export table to Parquet
            export_table_to_parquet(conn, schema_name, table_name, parquet_file)


    for schema in schemas:
        schema_name = schema[0]
        # Retrieve list of tables in each schema
        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        tables = conn.execute(query).fetchall()

        for table in tables:
            table_name = table[0]
            parquet_file = f"/tmp/{schema_name}_{table_name}.parquet"

            # Upload to R2 and get URL
            url = upload_to_r2(parquet_file, bucket_name, prefix)
            print(f"URL: {url}")

            # Create view in new database
            create_view_with_url(new_conn, schema_name, table_name, url)

            # Clean up local file
            os.remove(parquet_file)
    
    conn.close()
    new_conn.close()
    url = upload_to_r2(new_db_path, bucket_name, prefix)
    print(f"URL: {url}")


if __name__ == "__main__":
    main()
