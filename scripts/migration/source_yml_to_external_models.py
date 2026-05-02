"""Fold the six dbt-style source.yml files into a single SQLMesh external_models.yaml.

One-shot conversion. Output: bc/external_models.yaml — auto-discovered by SQLMesh
loader (`.venv/.../sqlmesh/core/loader.py:_load_external_models`).

Per-table mapping:
  source.yml entry                      ->  external_models.yaml entry
  -------------------------------------     -------------------------------------
  name (under sources[].name)               name = "<schema>.<table>"
  description                               description
  columns[].name + .data_type               columns: {col: TYPE, ...}
  columns[].description                     column_descriptions: {col: text}
  meta.primary_keys                         grain: [col, ...]
  columns[].tests (not_null / unique)       audits: [...] (rolled up by kind)

After running, `_init_db.py` reads bc/external_models.yaml instead of the
six source.yml files. The six source.yml files can then be deleted.
"""

from __future__ import annotations

import logging
import re
import sys
from pathlib import Path
from typing import Any

import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
STAGING_DIR = REPO_ROOT / "bc" / "models" / "staging"
OUTPUT_PATH = REPO_ROOT / "bc" / "external_models.yaml"
MODELS_DIR = REPO_ROOT / "bc" / "models"

SCHEMAS = ("event", "game", "box_score", "misc", "baseballdatabank", "biodata")

DOC_BLOCK_RE = re.compile(
    r"\{%\s*docs\s+(\w+)\s*%\}(.*?)\{%\s*enddocs\s*%\}", re.DOTALL
)
DOC_REF_RE = re.compile(r"\{\{\s*doc\('([^']+)'\)\s*\}\}")


def _load_doc_dict() -> dict[str, str]:
    """Parse {% docs key %} blocks from bc/models/**/*.md.

    External model column descriptions can't use the `@doc()` macro
    (external_models.yaml is loaded as raw YAML, not parsed as a MODEL
    block, so SQLMesh's macro evaluator never runs). Resolve doc refs
    inline at generation time.
    """
    out: dict[str, str] = {}
    for md_path in sorted(MODELS_DIR.rglob("*.md")):
        for m in DOC_BLOCK_RE.finditer(md_path.read_text()):
            key = m.group(1)
            body = m.group(2)
            cleaned = " ".join(line.strip() for line in body.strip().splitlines())
            text = re.sub(r"\s+", " ", cleaned).strip()
            if key in out and out[key] != text:
                raise RuntimeError(f"doc key {key!r} redefined with different body")
            out[key] = text
    return out


def _resolve_docs(text: str, docs: dict[str, str]) -> str:
    def sub(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in docs:
            raise KeyError(f"unknown doc key: {key}")
        return docs[key]
    return DOC_REF_RE.sub(sub, text)


def _cols_sql(cols: list[str]) -> str:
    """Emit a SQL paren-tuple string suitable for the columns arg of
    SQLMesh's not_null / unique_values audits.

    SQLMesh's audit validator parses arg values via sqlglot; a Python list
    would fail. Single-col gets `(col)`, multi-col `(c1, c2, ...)`.
    """
    return "(" + ", ".join(cols) + ")"


def _build_audits(not_null_cols: list[str], unique_cols: list[str]) -> list[list[Any]]:
    audits: list[list[Any]] = []
    if not_null_cols:
        audits.append(["not_null", {"columns": _cols_sql(sorted(set(not_null_cols)))}])
    for col in sorted(set(unique_cols)):
        # SQLMesh unique_values treats NULL as a duplicate (PARTITION BY col).
        # dbt's `unique` test had the same behavior, but source tests run
        # rarely. Apply IS NOT NULL filter so the audit asserts uniqueness
        # over non-null values only — matching the SQL UNIQUE constraint
        # semantics most users expect.
        audits.append(
            [
                "unique_values",
                {"columns": _cols_sql([col]), "condition": f"{col} IS NOT NULL"},
            ]
        )
    return audits


def _convert_table(
    schema: str, table: dict[str, Any], docs: dict[str, str]
) -> dict[str, Any]:
    name = table["name"]
    fqn = f"{schema}.{name}"

    entry: dict[str, Any] = {"name": fqn}

    if desc := table.get("description"):
        entry["description"] = _resolve_docs(desc, docs)

    columns: dict[str, str] = {}
    column_descriptions: dict[str, str] = {}
    not_null_cols: list[str] = []
    unique_cols: list[str] = []

    for col in table.get("columns", []) or []:
        col_name = col["name"]
        if data_type := col.get("data_type"):
            columns[col_name] = data_type
        if col_desc := col.get("description"):
            column_descriptions[col_name] = _resolve_docs(col_desc, docs)
        for test in col.get("tests", []) or []:
            kind = test if isinstance(test, str) else next(iter(test))
            if kind == "not_null":
                not_null_cols.append(col_name)
            elif kind == "unique":
                unique_cols.append(col_name)
            else:
                log.warning("unknown test on %s.%s: %s", fqn, col_name, kind)

    if columns:
        entry["columns"] = columns
    if column_descriptions:
        entry["column_descriptions"] = column_descriptions

    if pk := (table.get("meta") or {}).get("primary_keys"):
        entry["grain"] = list(pk)

    if audits := _build_audits(not_null_cols, unique_cols):
        entry["audits"] = audits

    return entry


def main() -> int:
    docs = _load_doc_dict()
    log.info("loaded %d doc keys", len(docs))

    entries: list[dict[str, Any]] = []
    for schema in SCHEMAS:
        path = STAGING_DIR / schema / "source.yml"
        if not path.exists():
            log.warning("source.yml missing: %s", path)
            continue
        doc = yaml.safe_load(path.read_text())
        for source in doc.get("sources", []) or []:
            sname = source["name"]
            if sname != schema:
                log.warning("source name %s != schema dir %s in %s", sname, schema, path)
            for table in source.get("tables", []) or []:
                entries.append(_convert_table(sname, table, docs))

    entries.sort(key=lambda e: e["name"])

    with OUTPUT_PATH.open("w") as f:
        yaml.safe_dump(entries, f, sort_keys=False, default_flow_style=False, width=120)

    log.info("wrote %d external models to %s", len(entries), OUTPUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main())
