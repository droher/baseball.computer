"""Phase 1.5 — migrate dbt model YAML metadata into SQLMesh MODEL() blocks.

Walks `bc/models/**/*.yml`, parses each model entry, and enriches the matching
`.sql` file's `MODEL(...)` block with:

- `description` from `description:`
- `column_descriptions (...)` from per-column `description:` (with
  `{{ doc('key') }}` resolved via the 4 docs `.md` files)
- `columns (col TYPE, ...)` from per-column `data_type:` + `contract.enforced`
- `grain` from `constraints: primary_key` or `meta.primary_keys`
- `audits` from per-column `tests:` (not_null, unique, accepted_values,
  dbt_utils.not_null_proportion → forall)
- `physical_properties (download_parquet = '...')` from `meta.download_parquet`

Source-table tests in `bc/models/**/source.yml` are merged into the staging
model that wraps each `source(schema, table)` call, as additional audits on
that staging model. Falls back gracefully if no staging wrapper found (logs
warning).

Idempotent: re-running with the same YAML state regenerates the same MODEL
block.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger("migrate_yaml_to_model_block")

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR = REPO_ROOT / "bc" / "models"

DOC_FILES = [
    MODELS_DIR / "doc_global_cols.md",
    MODELS_DIR / "doc_stat_list.md",
    MODELS_DIR / "doc_metric_calc.md",
    MODELS_DIR / "staging" / "baseballdatabank" / "doc.md",
]

DOC_BLOCK_RE = re.compile(
    r"\{%\s*docs\s+(\S+)\s*%\}(.*?)\{%\s*enddocs\s*%\}", re.DOTALL
)
DOC_REF_RE = re.compile(r"\{\{\s*doc\('([^']+)'\)\s*\}\}")


def _load_doc_dict() -> dict[str, str]:
    """Parse {% docs key %}...{% enddocs %} from the 4 doc files into a dict."""
    docs: dict[str, str] = {}
    for path in DOC_FILES:
        text = path.read_text()
        for m in DOC_BLOCK_RE.finditer(text):
            key, body = m.group(1), m.group(2)
            cleaned = " ".join(line.strip() for line in body.strip().splitlines())
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            docs[key] = cleaned
    return docs


def _resolve_doc_refs(text: str, doc_dict: dict[str, str]) -> str:
    """Resolve `{{ doc('key') }}` refs, leaving non-doc jinja alone."""
    def sub(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in doc_dict:
            raise KeyError(f"unknown doc key: {key}")
        return doc_dict[key]
    return DOC_REF_RE.sub(sub, text)


def _quote_sql_string(s: str) -> str:
    """Single-quote SQL string with embedded quotes escaped, collapsed to one line."""
    s = " ".join(s.split())
    return "'" + s.replace("'", "''") + "'"


_WHOLE_DOC_REF_RE = re.compile(r"^\s*\{\{\s*doc\('([^']+)'\)\s*\}\}\s*$")


def _emit_description(text: str, doc_dict: dict[str, str]) -> str:
    """Render a YAML description as MODEL block SQL.

    Pure `{{ doc('key') }}` refs become `@doc('key')` (re-DRYed via
    bc/macros/_docs.py). Anything else gets `{{ doc() }}` refs inlined
    and the whole thing single-quoted.
    """
    m = _WHOLE_DOC_REF_RE.match(text)
    if m:
        key = m.group(1)
        if key not in doc_dict:
            raise KeyError(f"unknown doc key: {key}")
        return f"@doc('{key}')"
    return _quote_sql_string(_resolve_doc_refs(text, doc_dict))


def _model_block_end(text: str) -> int | None:
    """Return index after the MODEL(...); block."""
    if not text.startswith("MODEL"):
        return None
    open_idx = text.find("(")
    if open_idx == -1:
        return None
    depth = 0
    i = open_idx
    while i < len(text):
        ch = text[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                j = i + 1
                while j < len(text) and text[j] in " \t":
                    j += 1
                if j < len(text) and text[j] == ";":
                    j += 1
                while j < len(text) and text[j] in " \t":
                    j += 1
                if j < len(text) and text[j] == "\n":
                    j += 1
                return j
        i += 1
    return None


def _grain_from_yml(model_yml: dict[str, Any]) -> list[str]:
    meta = model_yml.get("meta") or {}
    if isinstance(meta.get("primary_keys"), list):
        return [str(c) for c in meta["primary_keys"]]
    for c in model_yml.get("constraints") or []:
        if c.get("type") == "primary_key" and c.get("columns"):
            return [str(col) for col in c["columns"]]
    return []


def _quote_audit_arg(v: Any) -> str:
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, (int, float)):
        return str(v)
    return _quote_sql_string(str(v))


def _audits_from_yml(
    model_yml: dict[str, Any],
    yml_path: Path,
    extra_audits: list[str] | None = None,
) -> tuple[list[str], int]:
    """Return (audit_call_strings, skipped_test_count)."""
    audits: list[str] = list(extra_audits or [])
    not_null_cols: list[str] = []
    unique_cols: list[str] = []
    skipped = 0

    for col in model_yml.get("columns") or []:
        cname = col.get("name")
        if not cname:
            continue
        for test in col.get("tests") or []:
            if test == "not_null":
                not_null_cols.append(cname)
            elif test == "unique":
                unique_cols.append(cname)
            elif isinstance(test, dict):
                if "accepted_values" in test:
                    values = test["accepted_values"].get("values") or []
                    quoted = ", ".join(f"'{v}'" for v in values)
                    audits.append(
                        f"accepted_values(column := {cname}, "
                        f"is_in := ({quoted}))"
                    )
                elif "dbt_utils.not_null_proportion" in test:
                    cfg = test["dbt_utils.not_null_proportion"]
                    at_least = cfg.get("at_least", 0.95)
                    audits.append(
                        f"not_null_proportion(column := {cname}, "
                        f"threshold := {at_least})"
                    )
                elif "relationships" in test:
                    rel = test["relationships"]
                    to_field = rel.get("field")
                    to_ref = rel.get("to") or ""
                    m = re.match(r"\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*", to_ref)
                    if m and to_field:
                        audits.append(
                            f"relationships(column := {cname}, "
                            f"to_column := {to_field}, "
                            f"to_model := main_models.{m.group(1)})"
                        )
                    else:
                        skipped += 1
                        logger.debug(
                            "skipping malformed relationships test %s on %s.%s",
                            test, model_yml.get("name"), cname,
                        )
                elif "not_null" in test and isinstance(test["not_null"], dict):
                    cfg = test["not_null"].get("config") or {}
                    where = cfg.get("where")
                    if where:
                        audits.append(
                            f"not_null(columns := ({cname}), "
                            f"condition := ({where}))"
                        )
                    else:
                        not_null_cols.append(cname)
                else:
                    skipped += 1
                    logger.debug(
                        "skipping unknown test %s on %s.%s in %s",
                        test, model_yml.get("name"), cname,
                        yml_path.relative_to(REPO_ROOT),
                    )

    if not_null_cols:
        cols = ", ".join(not_null_cols)
        audits.append(f"not_null(columns := ({cols}))")
    for col in unique_cols:
        audits.append(f"unique_values(columns := ({col}))")

    return audits, skipped


def _enrich_model_block(
    sql_text: str,
    name: str,
    *,
    grain: list[str],
    audits: list[str],
    description: str | None,
    column_descriptions: dict[str, str],
    columns: list[tuple[str, str]],
    physical_properties: dict[str, str],
    enabled: bool,
) -> str | None:
    """Replace the MODEL(...) block with an enriched one."""
    end = _model_block_end(sql_text)
    if end is None:
        return None

    lines = ["MODEL (", f"  name main_models.{name},", "  kind FULL,"]
    if not enabled:
        lines.append("  enabled FALSE,")
    if description:
        lines.append(f"  description {description},")
    if grain:
        lines.append(f"  grain ({', '.join(grain)}),")
    if columns:
        lines.append("  columns (")
        for i, (cname, ctype) in enumerate(columns):
            sep = "," if i < len(columns) - 1 else ""
            lines.append(f"    {cname} {ctype}{sep}")
        lines.append("  ),")
    if column_descriptions:
        lines.append("  column_descriptions (")
        items = list(column_descriptions.items())
        for i, (cname, desc) in enumerate(items):
            sep = "," if i < len(items) - 1 else ""
            lines.append(f"    {cname} = {desc}{sep}")
        lines.append("  ),")
    if audits:
        lines.append("  audits (")
        for i, a in enumerate(audits):
            sep = "," if i < len(audits) - 1 else ""
            lines.append(f"    {a}{sep}")
        lines.append("  ),")
    if physical_properties:
        lines.append("  physical_properties (")
        items = list(physical_properties.items())
        for i, (k, v) in enumerate(items):
            sep = "," if i < len(items) - 1 else ""
            lines.append(f"    {k} = {_quote_sql_string(v)}{sep}")
        lines.append("  ),")
    lines.append(");\n\n")
    new_block = "\n".join(lines)
    return new_block + sql_text[end:]


def _find_sql_for(yml_path: Path, model_name: str) -> Path | None:
    candidate = yml_path.with_name(f"{model_name}.sql")
    if candidate.exists():
        return candidate
    candidate = yml_path.with_suffix(".sql")
    if candidate.exists():
        return candidate
    matches = list(MODELS_DIR.rglob(f"{model_name}.sql"))
    if len(matches) == 1:
        return matches[0]
    return None


def _enabled_from_sql(sql_text: str) -> bool:
    """Detect existing `enabled FALSE` in the current MODEL block to preserve it."""
    end = _model_block_end(sql_text)
    if end is None:
        return True
    block = sql_text[:end]
    return not re.search(r"\benabled\s+FALSE\b", block, re.IGNORECASE)


def _staging_model_for_source(schema: str, table: str) -> str | None:
    """Find the staging model wrapping a given source table.

    Phase 1.5 converted `{{ source('<schema>','<table>') }}` → `<schema>.<table>`
    inline. Match that pattern (case-insensitive, word-bounded).
    """
    pattern = re.compile(
        rf"\bFROM\s+{re.escape(schema)}\.{re.escape(table)}\b", re.IGNORECASE
    )
    matches: list[Path] = []
    for sql_path in (MODELS_DIR / "staging").rglob("*.sql"):
        if pattern.search(sql_path.read_text()):
            matches.append(sql_path)
    if len(matches) == 1:
        return matches[0].stem
    return None


def _staging_output_columns(staging_name: str) -> set[str]:
    """Return the set of output column names declared in the staging model's YAML.

    Source-level tests reference raw source column names; staging models often
    rename. We only port a source-level test if its column name appears in the
    staging model's declared output columns.
    """
    cols: set[str] = set()
    for yml_path in MODELS_DIR.rglob("*.yml"):
        if yml_path.name == "source.yml":
            continue
        doc = yaml.safe_load(yml_path.read_text())
        if not doc:
            continue
        for model_yml in doc.get("models") or []:
            if model_yml.get("name") != staging_name:
                continue
            for c in model_yml.get("columns") or []:
                cname = c.get("name")
                if cname:
                    cols.add(str(cname))
    return cols


def _staging_columns_with_own_tests(staging_name: str) -> set[str]:
    """Return columns for which the staging model's YAML defines its own tests.

    Source-level tests run against raw source tables in dbt; staging models may
    legitimately drop, transform, or condition columns (e.g. NULL-out park_id
    pre-1875). When the staging YAML defines a more specific test on a column,
    treat that as authoritative and skip the broader source-level test.
    """
    cols: set[str] = set()
    for yml_path in MODELS_DIR.rglob("*.yml"):
        if yml_path.name == "source.yml":
            continue
        doc = yaml.safe_load(yml_path.read_text())
        if not doc:
            continue
        for model_yml in doc.get("models") or []:
            if model_yml.get("name") != staging_name:
                continue
            for c in model_yml.get("columns") or []:
                cname = c.get("name")
                if cname and (c.get("tests") or []):
                    cols.add(str(cname))
    return cols


def _build_source_audits() -> dict[str, list[str]]:
    """Return {staging_model_name: [audit_strings, ...]} from all source.yml files."""
    out: dict[str, list[str]] = defaultdict(list)
    for src_yml in MODELS_DIR.rglob("source.yml"):
        doc = yaml.safe_load(src_yml.read_text())
        if not doc:
            continue
        for source in doc.get("sources") or []:
            schema = source.get("name")
            for table in source.get("tables") or []:
                tname = table.get("name")
                if not (schema and tname):
                    continue
                not_null_cols: list[str] = []
                unique_cols: list[str] = []
                for col in table.get("columns") or []:
                    cname = col.get("name")
                    if not cname:
                        continue
                    for test in col.get("tests") or []:
                        if test == "not_null":
                            not_null_cols.append(cname)
                        elif test == "unique":
                            unique_cols.append(cname)
                if not (not_null_cols or unique_cols):
                    continue
                staging = _staging_model_for_source(schema, tname)
                if staging is None:
                    logger.debug(
                        "no unique staging wrapper for source %s.%s — "
                        "skipping %d source-tests",
                        schema, tname,
                        len(not_null_cols) + len(unique_cols),
                    )
                    continue
                staging_cols = _staging_output_columns(staging)
                staging_owned_cols = _staging_columns_with_own_tests(staging)
                # Source-test columns must survive into the staging output
                # under the same name; skip those that don't (renames). Also
                # skip columns where staging YAML defines its own test —
                # the staging test is more specific (e.g. conditional
                # not_null) and the source-level test would falsely override.
                kept_not_null = [
                    c for c in not_null_cols
                    if c in staging_cols and c not in staging_owned_cols
                ]
                kept_unique = [
                    c for c in unique_cols
                    if c in staging_cols and c not in staging_owned_cols
                ]
                dropped = (
                    len(not_null_cols) - len(kept_not_null)
                    + len(unique_cols) - len(kept_unique)
                )
                if dropped:
                    logger.debug(
                        "%s: dropped %d source-test cols not in staging output",
                        staging, dropped,
                    )
                audits = []
                if kept_not_null:
                    audits.append(f"not_null(columns := ({', '.join(kept_not_null)}))")
                for c in kept_unique:
                    audits.append(f"unique_values(columns := ({c}))")
                if audits:
                    out[staging].extend(audits)
    return out


def _columns_from_yml(
    model_yml: dict[str, Any], sql_text: str
) -> list[tuple[str, str]]:
    """Build columns block from per-column data_type, only if contract enforced.

    Skip when the model uses `UNION ALL BY NAME` — sqlglot's column inference
    can't track output schema across UNION-BY-NAME branches with different
    arities, so SQLMesh's downstream schema enforcement projects the wrong
    column set and raises BinderException.
    """
    config = model_yml.get("config") or {}
    contract = (config.get("contract") or {}) if isinstance(config, dict) else {}
    if not contract.get("enforced"):
        return []
    if re.search(r"\bUNION\s+(ALL\s+)?BY\s+NAME\b", sql_text, re.IGNORECASE):
        return []
    out: list[tuple[str, str]] = []
    for col in model_yml.get("columns") or []:
        cname = col.get("name")
        ctype = col.get("data_type")
        if cname and ctype:
            out.append((str(cname), str(ctype).upper()))
    return out


def _column_descriptions_from_yml(
    model_yml: dict[str, Any], doc_dict: dict[str, str]
) -> dict[str, str]:
    """Return column → SQL fragment (already quoted or `@doc('key')`)."""
    out: dict[str, str] = {}
    for col in model_yml.get("columns") or []:
        cname = col.get("name")
        desc = col.get("description")
        if not cname or not desc:
            continue
        out[str(cname)] = _emit_description(str(desc), doc_dict)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(message)s",
    )

    doc_dict = _load_doc_dict()
    logger.info("loaded %d doc keys from %d files", len(doc_dict), len(DOC_FILES))

    source_audits = _build_source_audits()
    logger.info(
        "built %d source-audit groups (%d total source audits)",
        len(source_audits),
        sum(len(v) for v in source_audits.values()),
    )

    enriched = 0
    no_metadata = 0
    no_sql = 0
    total_audits = 0
    total_skipped_tests = 0
    total_columns = 0
    total_col_descriptions = 0
    total_descriptions = 0
    total_phys_props = 0
    used_source_audits: set[str] = set()

    for yml_path in sorted(MODELS_DIR.rglob("*.yml")):
        if yml_path.name == "source.yml":
            continue
        doc = yaml.safe_load(yml_path.read_text())
        if not doc:
            continue
        for model_yml in doc.get("models") or []:
            name = model_yml.get("name")
            if not name:
                continue

            grain = _grain_from_yml(model_yml)
            extra = source_audits.get(name)
            if extra:
                used_source_audits.add(name)
            audits, skipped = _audits_from_yml(model_yml, yml_path, extra_audits=extra)
            total_skipped_tests += skipped
            description_raw = model_yml.get("description")
            description = (
                _emit_description(str(description_raw), doc_dict)
                if description_raw else None
            )
            column_descriptions = _column_descriptions_from_yml(model_yml, doc_dict)

            sql_path_for_inspect = _find_sql_for(yml_path, name)
            sql_text_for_inspect = (
                sql_path_for_inspect.read_text() if sql_path_for_inspect else ""
            )
            columns = _columns_from_yml(model_yml, sql_text_for_inspect)
            meta = model_yml.get("meta") or {}
            physical_properties: dict[str, str] = {}
            if meta.get("download_parquet"):
                physical_properties["download_parquet"] = str(meta["download_parquet"])

            has_metadata = any([
                grain, audits, description, column_descriptions,
                columns, physical_properties,
            ])
            if not has_metadata:
                no_metadata += 1
                continue

            sql_path = _find_sql_for(yml_path, name)
            if sql_path is None:
                logger.warning("no .sql for %s (%s)", name, yml_path)
                no_sql += 1
                continue
            sql_text = sql_path.read_text()
            enabled = _enabled_from_sql(sql_text)
            new_text = _enrich_model_block(
                sql_text,
                name,
                grain=grain,
                audits=audits,
                description=description,
                column_descriptions=column_descriptions,
                columns=columns,
                physical_properties=physical_properties,
                enabled=enabled,
            )
            if new_text is None:
                logger.warning(
                    "no MODEL() block in %s — was it converted in Step 3?",
                    sql_path.relative_to(REPO_ROOT),
                )
                continue

            if args.dry_run:
                logger.info(
                    "[dry-run] %s: desc=%s grain=%d cols=%d coldesc=%d "
                    "audits=%d phys=%d",
                    name, "Y" if description else "N",
                    len(grain), len(columns), len(column_descriptions),
                    len(audits), len(physical_properties),
                )
            else:
                sql_path.write_text(new_text)

            enriched += 1
            total_audits += len(audits)
            total_columns += len(columns)
            total_col_descriptions += len(column_descriptions)
            if description:
                total_descriptions += 1
            total_phys_props += len(physical_properties)

    orphan_source_audits = set(source_audits) - used_source_audits
    orphan_enriched = 0
    for staging_name in sorted(orphan_source_audits):
        sql_matches = list(MODELS_DIR.rglob(f"{staging_name}.sql"))
        if len(sql_matches) != 1:
            logger.warning(
                "orphan source-audit target %s: %d SQL matches", staging_name, len(sql_matches),
            )
            continue
        sql_path = sql_matches[0]
        sql_text = sql_path.read_text()
        enabled = _enabled_from_sql(sql_text)
        new_text = _enrich_model_block(
            sql_text,
            staging_name,
            grain=[],
            audits=source_audits[staging_name],
            description=None,
            column_descriptions={},
            columns=[],
            physical_properties={},
            enabled=enabled,
        )
        if new_text is None:
            logger.warning("orphan %s: no MODEL block", staging_name)
            continue
        if args.dry_run:
            logger.info(
                "[dry-run] orphan %s: audits=%d",
                staging_name, len(source_audits[staging_name]),
            )
        else:
            sql_path.write_text(new_text)
        orphan_enriched += 1
        total_audits += len(source_audits[staging_name])

    logger.info(
        "enriched=%d orphan_source_enriched=%d no_metadata=%d no_sql=%d "
        "descriptions=%d col_descriptions=%d columns=%d "
        "audits=%d phys_props=%d skipped_tests=%d",
        enriched, orphan_enriched, no_metadata, no_sql,
        total_descriptions, total_col_descriptions, total_columns,
        total_audits, total_phys_props, total_skipped_tests,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
