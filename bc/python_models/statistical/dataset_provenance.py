from __future__ import annotations

import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from sqlglot import exp, parse_one


def hash_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_arrow_type(dtype: pa.DataType) -> pa.DataType:
    if pa.types.is_large_string(dtype):
        return pa.string()
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return pa.list_(canonical_arrow_type(dtype.value_type))
    if pa.types.is_fixed_size_list(dtype):
        return pa.list_(canonical_arrow_type(dtype.value_type), dtype.list_size)
    if pa.types.is_struct(dtype):
        return pa.struct(
            [pa.field(field.name, canonical_arrow_type(field.type)) for field in dtype]
        )
    if pa.types.is_map(dtype):
        return pa.map_(
            canonical_arrow_type(dtype.key_type),
            canonical_arrow_type(dtype.item_type),
        )
    return dtype


def canonical_schema(schema: pa.Schema) -> list[tuple[str, str]]:
    return [(field.name, str(canonical_arrow_type(field.type))) for field in schema]


def hash_schema(schema: pa.Schema) -> str:
    fields = canonical_schema(schema)
    payload = json.dumps(fields, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def hash_parquet_schema(path: Path) -> str:
    return hash_schema(pq.read_schema(path))


def relation_hashes(
    con: duckdb.DuckDBPyConnection, *, schema_name: str, relation_name: str
) -> tuple[str, str]:
    row = con.execute(
        """
        SELECT 'view' AS kind, sql
        FROM duckdb_views()
        WHERE database_name = current_database()
          AND schema_name = ?
          AND view_name = ?
        UNION ALL
        SELECT 'table' AS kind, sql
        FROM duckdb_tables()
        WHERE database_name = current_database()
          AND schema_name = ?
          AND table_name = ?
        """,
        [schema_name, relation_name, schema_name, relation_name],
    ).fetchall()
    if len(row) != 1:
        raise ValueError(
            f"expected one catalog relation for {schema_name}.{relation_name}, "
            f"found {len(row)}"
        )
    kind, definition = row[0]
    definition_text = str(definition)
    transformation_payload = json.dumps(
        {
            "kind": str(kind),
            "relation": f"{schema_name}.{relation_name}",
            "definition": definition_text,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    expression = parse_one(definition_text, read="duckdb")
    root = ("", schema_name, relation_name)
    dependencies = sorted(
        {
            (table.catalog, table.db, table.name)
            for table in expression.find_all(exp.Table)
            if (table.catalog, table.db, table.name) != root
            and ("", table.db, table.name) != root
        }
    )
    dependency_payload = json.dumps(
        dependencies,
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return (
        hashlib.sha256(transformation_payload.encode("utf-8")).hexdigest(),
        hashlib.sha256(dependency_payload.encode("utf-8")).hexdigest(),
    )
