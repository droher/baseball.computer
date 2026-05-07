"""Generate the baseball.computer LSF-1 context packet for LLMs.

Three input streams (priority order): SQLMesh ``Context.models`` (for
all physical and external models), the Pydantic ``Metric`` registry
(for BSL semantic-table measures), and a hand-curated supplement YAML
(for RULES, AMBIGUITIES, VERIFIED_QUERIES, COVERAGE, table synonyms).

Run via ``just gen-llm-context`` or directly with::

    uv run --group build python scripts/generate_llm_context.py \
        --include semantic,main,seeds --out docs/llm/baseball.lsf

LSF-1 spec: ``docs/llm/lsf_1_spec.md``.
"""

from __future__ import annotations

import argparse
import ast
import csv
import inspect
import io
import logging
import re
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BC_DIR = REPO_ROOT / "bc"
SEEDS_DIR = BC_DIR / "seeds"
DOCS_LLM = REPO_ROOT / "docs" / "llm"
DEFAULT_OUT = DOCS_LLM / "baseball.lsf"
DEFAULT_SUPPLEMENT = DOCS_LLM / "supplement.yaml"

ENUM_ROW_LIMIT = 50

if str(BC_DIR) not in sys.path:
    sys.path.insert(0, str(BC_DIR))

logger = logging.getLogger("generate_llm_context")

IncludeToken = Literal["semantic", "main", "seeds", "external"]
ALL_INCLUDE: tuple[IncludeToken, ...] = ("semantic", "main", "seeds", "external")
DEFAULT_INCLUDE: tuple[IncludeToken, ...] = ("semantic", "main", "seeds")

# ----- type mapping ------------------------------------------------------

_INT_DTYPES = {
    "TINYINT", "UTINYINT", "SMALLINT", "USMALLINT", "INT", "UINT",
    "BIGINT", "UBIGINT", "INT128",
}
_FLOAT_DTYPES = {"DOUBLE", "FLOAT", "DECIMAL"}
_STRING_DTYPES = {"TEXT", "VARCHAR", "CHAR", "STRING"}
_TIME_DTYPES = {"TIMESTAMP", "TIMESTAMPTZ", "TIMESTAMPNTZ", "TIMESTAMPLTZ"}
_DATE_DTYPES = {"DATE"}
_BOOL_DTYPES = {"BOOLEAN", "BOOL"}
_JSON_LIKE = {"ARRAY", "LIST", "STRUCT", "MAP", "JSON"}


def _normalize_type(ct: Any) -> str:
    """Map a sqlglot DataType to ``normalized<duckdb>`` per LSF-1 §9.5."""
    name = ct.this.name if hasattr(ct.this, "name") else str(ct.this)
    duckdb_form = ct.sql(dialect="duckdb")
    if name == "USERDEFINED":
        udt = ct.args.get("kind") or duckdb_form
        return f"string<{udt}>"
    if name in _INT_DTYPES:
        return f"int<{name}>"
    if name in _FLOAT_DTYPES:
        return f"float<{duckdb_form}>"
    if name in _STRING_DTYPES:
        return f"string<{name}>"
    if name in _DATE_DTYPES:
        return f"date<{name}>"
    if name in _TIME_DTYPES:
        return f"timestamp<{name}>"
    if name in _BOOL_DTYPES:
        return f"boolean<{name}>"
    if name in _JSON_LIKE:
        return f"json<{duckdb_form}>"
    return f"string<{duckdb_form}>"


def _is_time_type(ct: Any) -> bool:
    name = ct.this.name if hasattr(ct.this, "name") else str(ct.this)
    return name in _TIME_DTYPES or name in _DATE_DTYPES


def _is_numeric_type(ct: Any) -> bool:
    name = ct.this.name if hasattr(ct.this, "name") else str(ct.this)
    return name in _INT_DTYPES or name in _FLOAT_DTYPES


# ----- field encoding ---------------------------------------------------

def _esc(value: str | None) -> str:
    """Escape a single LSF-1 field value (§6.8). ``None``/empty → ``-``."""
    if value is None:
        return "-"
    s = str(value).strip()
    if not s:
        return "-"
    s = s.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ")
    return s


def _list_field(items: Iterable[str]) -> str:
    parts = [s for s in (str(i).strip() for i in items) if s]
    if not parts:
        return "-"
    encoded = [
        p.replace("\\", "\\\\").replace("|", "\\|").replace(";", "\\;")
        for p in parts
    ]
    return ";".join(encoded)


def _row(*fields: Any) -> str:
    return "|".join(_esc(f) if not isinstance(f, list) else _list_field(f) for f in fields)


# ----- supplement loading -----------------------------------------------

@dataclass(frozen=True)
class Ambiguity:
    term: str
    meanings: list[str]
    default: str | None
    clarify_when: str | None


@dataclass(frozen=True)
class Rule:
    severity: str
    scope: str
    text: str


@dataclass(frozen=True)
class VerifiedQuery:
    id: str
    question: str
    notes: str | None
    sql: str


@dataclass(frozen=True)
class Supplement:
    domain_id: str
    domain_description: str
    tz: str
    coverage: str | None
    table_synonyms: dict[str, list[str]]
    ambiguities: list[Ambiguity]
    rules: list[Rule]
    verified_queries: list[VerifiedQuery]


def _load_supplement(path: Path) -> Supplement:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    dom = raw.get("domain") or {}
    return Supplement(
        domain_id=dom.get("id", "domain.baseball"),
        domain_description=dom.get(
            "description",
            "Major-league play-by-play, season, and career analytics.",
        ),
        tz=raw.get("tz", "America/New_York"),
        coverage=(raw.get("coverage") or "").strip() or None,
        table_synonyms={
            str(k): list(v or []) for k, v in (raw.get("table_synonyms") or {}).items()
        },
        ambiguities=[
            Ambiguity(
                term=a["term"],
                meanings=list(a.get("meanings", [])),
                default=a.get("default"),
                clarify_when=a.get("clarify_when"),
            )
            for a in raw.get("ambiguities") or []
        ],
        rules=[
            Rule(
                severity=r["severity"],
                scope=r.get("scope", "global"),
                text=r["text"],
            )
            for r in raw.get("rules") or []
        ],
        verified_queries=[
            VerifiedQuery(
                id=v["id"],
                question=v["question"],
                notes=v.get("notes"),
                sql=v["sql"].rstrip(),
            )
            for v in raw.get("verified_queries") or []
        ],
    )


# ----- table/column data --------------------------------------------------

@dataclass
class Column:
    name: str
    type: str
    role: str
    ref: str | None = None
    desc: str | None = None
    examples: list[str] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)


@dataclass
class Table:
    table_id: str
    physical_name: str
    grain: str
    description: str
    synonyms: list[str]
    columns: list[Column]
    # (object_id_or_table_id, warning_text) tuples emitted alongside METRICS / TABLES
    warns: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class Relationship:
    rel_id: str
    left_col: str
    right_col: str
    cardinality: str
    fanout: str
    join_hint: str
    description: str


@dataclass
class Metric:
    metric_id: str
    name: str
    base_table: str
    expr: str
    agg: str
    time_col: str | None
    default_filter: str | None
    allowed_dims: list[str]
    description: str
    synonyms: list[str]


@dataclass
class EnumValue:
    enum_id: str
    column_ref: str
    value: str
    meaning: str | None
    aliases: list[str]


# ----- table id helpers -------------------------------------------------

BSLKind = Literal["offense", "pitching", "fielding"]
BSLGrain = Literal["season", "event"]

_BSL_GRAINS: tuple[tuple[BSLKind, BSLGrain], ...] = (
    ("offense", "season"),
    ("offense", "event"),
    ("pitching", "season"),
    ("pitching", "event"),
    ("fielding", "season"),
    ("fielding", "event"),
)


def _bsl_table_name(kind: BSLKind, grain: BSLGrain) -> str:
    return f"{kind}_{grain}s"


def _bsl_table_id(kind: BSLKind, grain: BSLGrain) -> str:
    return f"table.{_bsl_table_name(kind, grain)}"


def _physical_table_id(model_name: str) -> str:
    """``main_models.foo`` → ``table.main_models_foo`` (single dot-segment after ``table.``)."""
    short = model_name.replace(".", "_").lower()
    short = re.sub(r"[^a-z0-9_]", "_", short)
    return f"table.{short}"


def _col_ref(table_id: str, col_name: str) -> str:
    return f"{table_id}.{col_name}"


# ----- relationships extraction -----------------------------------------

def _identifier_str(node: Any) -> str | None:
    if node is None:
        return None
    if hasattr(node, "name") and isinstance(node.name, str):
        return node.name
    return str(node)


def _to_model_name(to_model_node: Any) -> str | None:
    """Decode the ``to_model`` sqlglot expression from a relationships() audit."""
    if to_model_node is None:
        return None
    schema = None
    name = None
    if hasattr(to_model_node, "args"):
        a = to_model_node.args
        if "table" in a and a["table"] is not None:
            schema = _identifier_str(a["table"])
        if "this" in a and a["this"] is not None:
            name = _identifier_str(a["this"])
    if name is None:
        return None
    return f"{schema}.{name}" if schema else name


# ----- seed loading -----------------------------------------------------

@dataclass
class Seed:
    name: str
    schema: str
    description: str
    pk_column: str | None
    columns: list[tuple[str, str]]
    column_descriptions: dict[str, str]
    rows: list[dict[str, str]]


def _load_seeds() -> list[Seed]:
    seeds: list[Seed] = []
    for yml_path in sorted(SEEDS_DIR.rglob("*.yml")):
        data = yaml.safe_load(yml_path.read_text()) or {}
        for seed_def in data.get("seeds") or []:
            name = seed_def.get("name")
            if not name:
                continue
            csv_path = yml_path.with_name(f"{name}.csv")
            if not csv_path.exists():
                logger.warning("seed yml %s lists %s with no CSV", yml_path, name)
                continue
            cols_meta = seed_def.get("columns") or []
            columns: list[tuple[str, str]] = []
            pk: str | None = None
            descs: dict[str, str] = {}
            for c in cols_meta:
                cname = c.get("name")
                ctype = c.get("data_type") or "varchar"
                cdesc = (c.get("description") or "").strip()
                if cdesc:
                    descs[cname] = cdesc
                columns.append((cname, ctype.lower()))
                for cons in c.get("constraints") or []:
                    if (cons.get("type") or "").lower() == "primary_key":
                        pk = cname
            with csv_path.open() as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            seeds.append(
                Seed(
                    name=name,
                    schema="main_seeds",
                    description=(seed_def.get("description") or "").strip()
                    or f"Seed table {name}.",
                    pk_column=pk,
                    columns=columns,
                    column_descriptions=descs,
                    rows=rows,
                )
            )
    return seeds


def _table_for_seed(seed: Seed, supplement: Supplement) -> Table:
    physical = f"{seed.schema}.{seed.name}"
    table_id = _physical_table_id(physical)
    grain = (
        f"one row per {seed.pk_column}"
        if seed.pk_column
        else "one row per record"
    )
    cols: list[Column] = []
    for cname, ctype in seed.columns:
        role = "PK" if cname == seed.pk_column else "DIM"
        cols.append(
            Column(
                name=cname,
                type=_seed_type_to_lsf(ctype),
                role=role,
                ref=None,
                desc=seed.column_descriptions.get(cname),
                examples=[],
                tags=[],
            )
        )
    if not cols:
        cols.append(Column(name="value", type="string", role="DIM", desc=None))
    return Table(
        table_id=table_id,
        physical_name=physical,
        grain=grain,
        description=seed.description,
        synonyms=supplement.table_synonyms.get(physical, []),
        columns=cols,
    )


def _seed_type_to_lsf(t: str) -> str:
    t = t.lower()
    if t in {"varchar", "text", "string", "char"}:
        return "string"
    if t in {"int", "integer", "bigint", "smallint", "usmallint", "tinyint"}:
        return f"int<{t.upper()}>"
    if t in {"double", "float", "decimal", "numeric"}:
        return f"float<{t.upper()}>"
    if t in {"boolean", "bool"}:
        return "boolean"
    if t == "date":
        return "date"
    if t.startswith("timestamp"):
        return f"timestamp<{t.upper()}>"
    return f"string<{t.upper()}>"


# ----- model → Table translation ----------------------------------------

def _grain_columns(model: Any) -> list[str]:
    out: list[str] = []
    for g in model.grains or []:
        n = _identifier_str(g) or _identifier_str(getattr(g, "this", None))
        if n:
            out.append(n)
            continue
        # Tuple/Paren: walk children
        for child in getattr(g, "expressions", None) or []:
            n = _identifier_str(child) or _identifier_str(getattr(child, "this", None))
            if n:
                out.append(n)
    return out


def _model_columns(model: Any, ctx: Any) -> dict[str, Any]:
    """``model.columns_to_types`` falls back to engine_adapter.columns() when empty.

    SQLMesh skips column inference on FULL kinds whose query depends on
    macros until plan time, so many published tables expose an empty
    columns dict at Context-load. The live DB has the resolved schema.
    """
    cols = model.columns_to_types or {}
    if cols:
        return dict(cols)
    try:
        live = ctx.engine_adapter.columns(model.name)
    except Exception as exc:
        logger.debug("engine_adapter.columns(%s) failed: %s", model.name, exc)
        return {}
    return dict(live or {})


def _model_table(
    model: Any,
    *,
    fks: dict[str, str],
    supplement: Supplement,
    columns_to_types: dict[str, Any],
    derived_columns: set[str] | None = None,
) -> Table | None:
    cols_to_types = columns_to_types
    if not cols_to_types:
        logger.debug("skipping %s: no columns", model.name)
        return None

    physical = model.name
    table_id = _physical_table_id(physical)
    grain_cols = _grain_columns(model)
    grain_str = (
        f"one row per ({', '.join(grain_cols)})"
        if grain_cols
        else "grain not declared"
    )
    column_descriptions = model.column_descriptions or {}
    derived_columns = derived_columns or set()

    columns: list[Column] = []
    for cname, ctype in cols_to_types.items():
        roles: list[str] = []
        if cname in grain_cols:
            roles.append("PK" if len(grain_cols) == 1 else "DIM")
        if cname in fks:
            roles.append("FK")

        if not roles:
            if _is_time_type(ctype):
                roles.append("TIME")
            elif cname in derived_columns:
                roles.append("DERIVED")
            elif _is_numeric_type(ctype):
                roles.append("MEASURE")
            else:
                roles.append("DIM")

        ref = fks.get(cname)
        desc = column_descriptions.get(cname)
        columns.append(
            Column(
                name=cname,
                type=_normalize_type(ctype),
                role="+".join(dict.fromkeys(roles)),
                ref=ref,
                desc=desc,
                examples=[],
                tags=list(model.tags or []) if False else [],
            )
        )

    description = (model.description or "").strip()
    if not description:
        description = f"{physical} ({model.kind.name.lower()})"

    return Table(
        table_id=table_id,
        physical_name=physical,
        grain=grain_str,
        description=description,
        synonyms=supplement.table_synonyms.get(physical, []),
        columns=columns,
    )


def _extract_relationships(
    models: Mapping[str, Any],
    in_scope_table_ids: set[str],
    in_scope_columns: dict[str, set[str]],
) -> tuple[list[Relationship], dict[str, dict[str, str]]]:
    rels: list[Relationship] = []
    fks_by_table: dict[str, dict[str, str]] = {}
    seen: set[str] = set()
    for m in models.values():
        src_table_id = _physical_table_id(m.name)
        if src_table_id not in in_scope_table_ids:
            continue
        for audit, args in m.audits_with_args or []:
            if audit.name != "relationships":
                continue
            col = args.get("column")
            to_model = args.get("to_model")
            to_col = args.get("to_column")
            src_col = _identifier_str(col)
            dst_model = _to_model_name(to_model)
            dst_col = _identifier_str(to_col)
            if not (src_col and dst_model and dst_col):
                continue
            dst_table_id = _physical_table_id(dst_model)
            if "." not in dst_model:
                logger.warning(
                    "rel from %s.%s -> %s.%s: dst is unqualified; "
                    "the relationships() audit should pass schema-qualified to_model",
                    src_table_id, src_col, dst_model, dst_col,
                )
            if dst_table_id not in in_scope_table_ids:
                logger.debug(
                    "skip rel %s.%s -> %s.%s: dst out of scope",
                    src_table_id, src_col, dst_table_id, dst_col,
                )
                continue
            if src_col not in in_scope_columns.get(src_table_id, set()):
                logger.debug(
                    "skip rel: src col %s.%s not emitted", src_table_id, src_col,
                )
                continue
            if dst_col not in in_scope_columns.get(dst_table_id, set()):
                logger.debug(
                    "skip rel: dst col %s.%s not emitted", dst_table_id, dst_col,
                )
                continue
            rel_id = f"rel.{m.name.replace('.', '_')}_{src_col}__{dst_model.replace('.', '_')}_{dst_col}"
            rel_id = re.sub(r"[^a-z0-9_.]", "_", rel_id.lower())
            if rel_id in seen:
                continue
            seen.add(rel_id)
            join_hint = (
                f"join {dst_model} on {m.name}.{src_col} = {dst_model}.{dst_col}"
            )
            rels.append(
                Relationship(
                    rel_id=rel_id,
                    left_col=_col_ref(src_table_id, src_col),
                    right_col=_col_ref(dst_table_id, dst_col),
                    cardinality="many_to_one",
                    fanout="safe",
                    join_hint=join_hint,
                    description=f"FK from {m.name}.{src_col} to {dst_model}.{dst_col}.",
                )
            )
            fks_by_table.setdefault(src_table_id, {})[src_col] = _col_ref(
                dst_table_id, dst_col
            )
    return rels, fks_by_table


# ----- BSL semantic tables ----------------------------------------------

def _build_bsl_tables(
    metric_registry: Any, layout: Any, supplement: Supplement, constants: Any
) -> tuple[list[Table], list[Metric]]:
    tables: list[Table] = []
    metrics: list[Metric] = []
    metric_kind_grain = metric_registry.metrics_for
    for kind, grain in _BSL_GRAINS:
        from semantic._tables_common import backing_model_name

        physical = backing_model_name(kind, grain)
        table_id = _bsl_table_id(kind, grain)
        dim_names = layout.dim_names(kind, grain)
        ms = metric_kind_grain(kind, grain)
        counting_cols = (
            constants.EVENT_INT_COLS[kind]
            if grain == "event"
            else constants.INT_COLS[kind]
        )

        cols: list[Column] = []
        for d in dim_names:
            role = "TIME" if d == "season" else "DIM"
            cols.append(Column(name=d, type=_dim_type(d), role=role, desc=_dim_desc(d)))

        for cname in counting_cols:
            cols.append(
                Column(
                    name=cname,
                    type="int<INT>",
                    role="MEASURE",
                    desc=f"Counting stat ({cname.replace('_', ' ')}); additive across rows.",
                )
            )

        allowed_dims = [_col_ref(table_id, d) for d in dim_names]
        time_col = _col_ref(table_id, "season") if grain == "season" else None

        for m in ms:
            kind_str = m.classification
            agg = "sum" if kind_str == "sum" else "ratio" if kind_str == "ratio" else "derived"
            role = "MEASURE" if kind_str == "sum" else "DERIVED"
            col_type = "int" if (kind_str == "sum" and "int" in (m.dtype or "").lower()) else "float"
            cols.append(
                Column(
                    name=m.name,
                    type=col_type,
                    role=role,
                    desc=_metric_desc_text(m),
                )
            )

            metric_id = f"metric.{m.name}_{kind}_{grain}"
            expr = _metric_expr_text(m)
            metrics.append(
                Metric(
                    metric_id=metric_id,
                    name=m.name,
                    base_table=table_id,
                    expr=expr,
                    agg=agg,
                    time_col=time_col,
                    default_filter=None,
                    allowed_dims=allowed_dims,
                    description=_metric_desc_text(m),
                    synonyms=[],
                )
            )

        if grain == "season":
            grain_str = "one row per (player_id, team_id, season, game_type)"
            if kind == "fielding":
                grain_str = "one row per (player_id, team_id, season, game_type, fielding_position)"
        else:
            grain_str = "one row per event"
        if grain == "season":
            descr = (
                f"BSL semantic table for {kind} stats at {grain} grain. "
                f"Principal table is {physical}; this view also exposes "
                "league (joined from main_seeds.seed_franchises) and "
                "pre-filters to regular-season game types. "
                f"Counting-stat columns are direct columns on {physical}; "
                "rate columns and DERIVED metrics are computed from those counts. "
                f"Direct SQL against {physical} will not see the league dim."
            )
        else:
            descr = (
                f"BSL semantic table for {kind} stats at {grain} grain. "
                f"Principal table is {physical}; this view also exposes "
                "season, league, park_id, game_type, is_regular_season "
                "(joined from main_models.team_game_start_info on team_id+game_id). "
                f"Counting-stat columns are direct columns on {physical}; "
                "rate columns and DERIVED metrics are computed from those counts. "
                f"Direct SQL against {physical} will not see the joined dims."
            )
        synonyms = supplement.table_synonyms.get(_bsl_table_name(kind, grain), [])
        warns = [
            (f"metric.{m.name}_{kind}_{grain}", _non_additive_warn(m))
            for m in ms
            if m.classification != "sum"
        ]
        tables.append(
            Table(
                table_id=table_id,
                physical_name=physical,
                grain=grain_str,
                description=descr,
                synonyms=synonyms,
                columns=cols,
                warns=warns,
            )
        )
    return tables, metrics


def _non_additive_warn(metric: Any) -> str:
    return (
        f"Non-additive ({metric.classification}): cannot SUM or AVG across rows. "
        "Recompute from underlying counting stats when grouping."
    )


def _metric_desc_text(metric: Any) -> str:
    return f"{metric.classification} metric ({metric.dtype})."


def _metric_expr_text(metric: Any) -> str:
    """Render a Pydantic Metric's lambdas as SQL-shaped text.

    Inspects the lambda source (the registry stores them as ibis
    expressions, so we can't `to_sql` without booting ibis-framework,
    which doesn't ship in the SQLMesh group). For ratios we render
    ``numerator / nullif(denominator,0)``; for derived metrics we render
    the composite expression in terms of sibling measure names.
    """
    try:
        if metric.derived is not None:
            return _render_lambda(metric.derived, scope_var="m")
        if metric.numerator is not None and metric.denominator is not None:
            num = _render_lambda(metric.numerator, scope_var="t")
            den = _render_lambda(metric.denominator, scope_var="t")
            return f"{num} / nullif({den},0)"
        if metric.formula is not None:
            return _render_lambda(metric.formula, scope_var="t")
    except _MetricExprUnrenderable as exc:
        logger.warning("metric %s: cannot render expr (%s)", metric.name, exc)
    if metric.derived is not None and hasattr(metric, "dependencies"):
        try:
            deps = sorted(metric.dependencies())
        except Exception:
            deps = []
        if deps:
            return "f(" + ", ".join(deps) + ") -- see registry"
    return f"-- {metric.classification} metric; expression in python_models.metrics registry"


class _MetricExprUnrenderable(Exception):
    pass


def _fn_body_and_args(fn: Any) -> tuple[ast.AST, ast.arguments]:
    """Return the AST body + arguments for a lambda or single-return function.

    Lambdas: trims surrounding context until the source parses to a Lambda.
    Named functions: parses the source (dedented) and finds the FunctionDef;
    a single ``return X`` body returns ``X`` as the body node.
    """
    try:
        src = inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise _MetricExprUnrenderable(f"getsource failed: {exc}") from exc

    if "lambda" in src:
        idx = src.find("lambda")
        cand = src[idx:].strip()
        while cand:
            try:
                tree = ast.parse(cand, mode="eval")
                if isinstance(tree.body, ast.Lambda):
                    return tree.body.body, tree.body.args
            except SyntaxError:
                pass
            cand = cand[:-1].rstrip()
        raise _MetricExprUnrenderable("could not isolate lambda from source")

    try:
        module = ast.parse(_dedent(src))
    except SyntaxError as exc:
        raise _MetricExprUnrenderable(f"def source did not parse: {exc}") from exc
    for node in ast.walk(module):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if len(body) == 1 and isinstance(body[0], ast.Return) and body[0].value is not None:
                return body[0].value, node.args
            raise _MetricExprUnrenderable(
                f"function {node.name} body is not a single return"
            )
    raise _MetricExprUnrenderable("no lambda or function found")


def _dedent(src: str) -> str:
    lines = src.splitlines()
    if not lines:
        return src
    indents = [len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()]
    pad = min(indents) if indents else 0
    return "\n".join(ln[pad:] if len(ln) >= pad else ln for ln in lines)


def _render_lambda(fn: Any, *, scope_var: str) -> str:
    body, args = _fn_body_and_args(fn)
    arg_names = [a.arg for a in args.args]
    defaults_vals = list(fn.__defaults__ or ())
    name_to_value: dict[str, Any] = {}
    if defaults_vals:
        for n, v in zip(arg_names[-len(defaults_vals):], defaults_vals):
            name_to_value[n] = v
    return _ExprRenderer(scope_var, name_to_value).render(body)


_BIN_OPS: dict[type[ast.operator], str] = {
    ast.Add: "+", ast.Sub: "-", ast.Mult: "*", ast.Div: "/",
    ast.FloorDiv: "/", ast.Mod: "%",
}
_CMP_OPS: dict[type[ast.cmpop], str] = {
    ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">=",
    ast.Eq: "=", ast.NotEq: "<>",
}
_AGG_METHODS = {"sum", "count", "mean", "min", "max", "stddev"}


class _ExprRenderer:
    """AST → SQL-shaped text for the small dialect used by Metric lambdas.

    Handles ``t.col``, ``t.col.sum()``, ``(expr).sum()``, arithmetic,
    ``ibis.coalesce``-style calls, and scope reads on the derived
    measure proxy (``m.measure_name``).
    """

    def __init__(self, scope_var: str, name_values: dict[str, Any] | None = None) -> None:
        self.scope_var = scope_var
        self.name_values = name_values or {}

    def render(self, node: ast.AST) -> str:
        return self._v(node)

    def _v(self, node: ast.AST) -> str:
        method = getattr(self, f"v_{type(node).__name__}", None)
        if method is None:
            raise _MetricExprUnrenderable(
                f"unsupported AST node {type(node).__name__}"
            )
        return method(node)

    def v_Attribute(self, node: ast.Attribute) -> str:
        if isinstance(node.value, ast.Name) and node.value.id == self.scope_var:
            return node.attr
        return f"{self._v(node.value)}.{node.attr}"

    def v_Name(self, node: ast.Name) -> str:
        return node.id

    def v_Call(self, node: ast.Call) -> str:
        args_rendered = [self._v(a) for a in node.args]
        if isinstance(node.func, ast.Attribute):
            method = node.func.attr
            inner = self._v(node.func.value)
            if method in _AGG_METHODS:
                rest = ("," + ",".join(args_rendered)) if args_rendered else ""
                return f"{method}({_strip_outer_parens(inner)}{rest})"
            args = ",".join([inner, *args_rendered])
            return f"{method}({args})"
        return f"{self._v(node.func)}({','.join(args_rendered)})"

    def v_BinOp(self, node: ast.BinOp) -> str:
        op = _BIN_OPS.get(type(node.op))
        if op is None:
            raise _MetricExprUnrenderable(f"binop {type(node.op).__name__}")
        return f"({self._v(node.left)} {op} {self._v(node.right)})"

    def v_UnaryOp(self, node: ast.UnaryOp) -> str:
        if isinstance(node.op, ast.USub):
            return f"(-{self._v(node.operand)})"
        if isinstance(node.op, ast.UAdd):
            return f"(+{self._v(node.operand)})"
        raise _MetricExprUnrenderable(f"unaryop {type(node.op).__name__}")

    def v_Compare(self, node: ast.Compare) -> str:
        parts = [self._v(node.left)]
        for op, cmp in zip(node.ops, node.comparators):
            sym = _CMP_OPS.get(type(op))
            if sym is None:
                raise _MetricExprUnrenderable(f"compare {type(op).__name__}")
            parts.append(sym)
            parts.append(self._v(cmp))
        return " ".join(parts)

    def v_Constant(self, node: ast.Constant) -> str:
        if isinstance(node.value, str):
            return "'" + node.value.replace("'", "''") + "'"
        return repr(node.value)

    def v_IfExp(self, node: ast.IfExp) -> str:
        return (
            f"case when {self._v(node.test)} then {self._v(node.body)} "
            f"else {self._v(node.orelse)} end"
        )

    def v_Subscript(self, node: ast.Subscript) -> str:
        if isinstance(node.value, ast.Name) and node.value.id == self.scope_var:
            col = self._eval_str(node.slice)
            if col is None:
                raise _MetricExprUnrenderable("dynamic subscript not resolvable")
            return col
        raise _MetricExprUnrenderable("subscript outside scope variable")

    def v_JoinedStr(self, node: ast.JoinedStr) -> str:
        s = self._eval_str(node)
        if s is None:
            raise _MetricExprUnrenderable("f-string with unresolvable value")
        return s

    def _eval_str(self, node: ast.AST) -> str | None:
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.JoinedStr):
            parts: list[str] = []
            for v in node.values:
                if isinstance(v, ast.Constant) and isinstance(v.value, str):
                    parts.append(v.value)
                elif isinstance(v, ast.FormattedValue):
                    sub = self._eval_str(v.value)
                    if sub is None:
                        return None
                    parts.append(sub)
                else:
                    return None
            return "".join(parts)
        if isinstance(node, ast.Name):
            if node.id in self.name_values:
                return str(self.name_values[node.id])
            return None
        return None


def _strip_outer_parens(s: str) -> str:
    if s.startswith("(") and s.endswith(")"):
        depth = 0
        for i, ch in enumerate(s):
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0 and i < len(s) - 1:
                    return s
        return s[1:-1]
    return s


_DIM_TYPES = {
    "player_id": "string<PLAYER_ID>",
    "team_id": "string<TEAM_ID>",
    "game_id": "string<GAME_ID>",
    "park_id": "string<PARK_ID>",
    "season": "int<USMALLINT>",
    "league": "string",
    "game_type": "string",
    "is_regular_season": "boolean",
    "fielding_position": "string",
}

_DIM_DESCS = {
    "player_id": "Retrosheet player id.",
    "team_id": "Retrosheet team id (current era).",
    "game_id": "Retrosheet game id.",
    "park_id": "Retrosheet park id.",
    "season": "Season year.",
    "league": "League id (AL/NL/...).",
    "game_type": "Game-type code; season tables filter to regular season by default.",
    "is_regular_season": "True for regular-season games.",
    "fielding_position": "Defensive position code (P, C, 1B, ...).",
}


def _dim_type(dim: str) -> str:
    if dim not in _DIM_TYPES:
        raise KeyError(
            f"BSL dim {dim!r} has no entry in _DIM_TYPES; add one in "
            "scripts/generate_llm_context.py when adding to bc/semantic/_layout.py"
        )
    return _DIM_TYPES[dim]


def _dim_desc(dim: str) -> str:
    if dim not in _DIM_DESCS:
        raise KeyError(
            f"BSL dim {dim!r} has no entry in _DIM_DESCS; add one in "
            "scripts/generate_llm_context.py when adding to bc/semantic/_layout.py"
        )
    return _DIM_DESCS[dim]


# ----- enums from seeds -------------------------------------------------

def _build_enums(seeds: list[Seed], in_scope: set[str]) -> list[EnumValue]:
    """ENUM rows for small seed tables. Seeds always emit a TABLE separately."""
    enums: list[EnumValue] = []
    for seed in seeds:
        physical = f"{seed.schema}.{seed.name}"
        table_id = _physical_table_id(physical)
        if not seed.pk_column or len(seed.rows) > ENUM_ROW_LIMIT:
            continue
        if table_id not in in_scope:
            continue
        col_ref = _col_ref(table_id, seed.pk_column)
        enum_id = f"enum.{seed.name.replace('seed_', '')}"
        for row in seed.rows:
            value = row.get(seed.pk_column)
            if value is None:
                continue
            meaning_parts = [f"{k}={v}" for k, v in row.items() if k != seed.pk_column and v]
            enums.append(
                EnumValue(
                    enum_id=enum_id,
                    column_ref=col_ref,
                    value=value,
                    meaning=", ".join(meaning_parts) if meaning_parts else None,
                    aliases=[],
                )
            )
    return enums


# ----- emission ----------------------------------------------------------

def _emit_table(buf: io.StringIO, table: Table) -> None:
    buf.write(_row(
        "TABLE",
        table.table_id,
        table.physical_name,
        table.grain,
        table.description,
        table.synonyms,
    ) + "\n")
    buf.write(f"COLS|{table.table_id}|name|type|role|ref|desc|examples|tags\n")
    for col in table.columns:
        buf.write(_row(
            "COL",
            table.table_id,
            col.name,
            col.type,
            col.role,
            col.ref,
            col.desc,
            col.examples,
            col.tags,
        ) + "\n")
    for target_id, text in table.warns:
        buf.write(_row("WARN", target_id, text) + "\n")
    buf.write("\n")


def _emit_relationship(buf: io.StringIO, rel: Relationship) -> None:
    buf.write(_row(
        "REL",
        rel.rel_id,
        rel.left_col,
        rel.right_col,
        rel.cardinality,
        rel.fanout,
        rel.join_hint,
        rel.description,
    ) + "\n")


def _emit_metric(buf: io.StringIO, m: Metric) -> None:
    buf.write(_row(
        "METRIC",
        m.metric_id,
        m.name,
        m.base_table,
        m.expr,
        m.agg,
        m.time_col,
        m.default_filter,
        m.allowed_dims,
        m.description,
        m.synonyms,
    ) + "\n")


def _emit_enum(buf: io.StringIO, e: EnumValue) -> None:
    buf.write(_row(
        "ENUM",
        e.enum_id,
        e.column_ref,
        e.value,
        e.meaning,
        e.aliases,
    ) + "\n")


def _emit_packet(
    *,
    supplement: Supplement,
    tables: list[Table],
    relationships: list[Relationship],
    metrics: list[Metric],
    enums: list[EnumValue],
    dialect: str,
) -> str:
    buf = io.StringIO()
    buf.write(f'<DB_CONTEXT version="LSF-1" dialect="{dialect}">\n')
    buf.write(_row("DOMAIN", supplement.domain_id, supplement.domain_description) + "\n")
    buf.write(_row("TZ", supplement.tz) + "\n")
    if supplement.coverage:
        buf.write(_row("COVERAGE", supplement.coverage) + "\n")
    buf.write("\n")

    buf.write("TABLES\n")
    for t in tables:
        _emit_table(buf, t)

    buf.write("RELATIONSHIPS\n")
    for r in relationships:
        _emit_relationship(buf, r)
    buf.write("\n")

    if metrics:
        buf.write("METRICS\n")
        for m in metrics:
            _emit_metric(buf, m)
        buf.write("\n")

    if enums:
        buf.write("ENUMS\n")
        for e in enums:
            _emit_enum(buf, e)
        buf.write("\n")

    if supplement.ambiguities:
        buf.write("AMBIGUITIES\n")
        for a in supplement.ambiguities:
            buf.write(_row(
                "AMBIG",
                a.term,
                a.meanings,
                a.default,
                a.clarify_when,
            ) + "\n")
        buf.write("\n")

    if supplement.rules:
        buf.write("RULES\n")
        for r in supplement.rules:
            buf.write(_row("RULE", r.severity, r.scope, r.text) + "\n")
        buf.write("\n")

    if supplement.verified_queries:
        buf.write("VERIFIED_QUERIES\n")
        for vq in supplement.verified_queries:
            buf.write(_row("VQ", vq.id, vq.question, vq.notes) + "\n")
            buf.write(f"SQL|{vq.id}<<\n")
            buf.write(vq.sql.rstrip() + "\n")
            buf.write(">>SQL\n")
        buf.write("\n")

    buf.write("</DB_CONTEXT>\n")
    return buf.getvalue()


# ----- validation --------------------------------------------------------

@dataclass
class ValidationReport:
    violations: list[str]
    counts: dict[str, int]


_FIELD_LEN = {
    "TABLE": 6,
    "COL": 9,
    "REL": 8,
    "METRIC": 11,
    "ENUM": 6,
    "AMBIG": 5,
    "RULE": 4,
    "VQ": 4,
    "WARN": 3,
    "DOMAIN": 3,
    "TZ": 2,
    "COVERAGE": 2,
    "CURRENCY": 2,
}


def _split_record(line: str) -> list[str]:
    """Split on unescaped ``|``. Per spec §6.8, escapes are interpreted *after*
    record splitting — so this stage only consumes ``\\\\`` and ``\\|``; ``\\;``
    and other sequences pass through verbatim for the list-decoding step.
    """
    out: list[str] = []
    cur: list[str] = []
    i = 0
    while i < len(line):
        ch = line[i]
        if ch == "\\" and i + 1 < len(line):
            nxt = line[i + 1]
            if nxt in ("\\", "|"):
                cur.append(nxt)
                i += 2
                continue
            cur.append(ch)
            i += 1
            continue
        if ch == "|":
            out.append("".join(cur))
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    out.append("".join(cur))
    return out


def _list_items(field_value: str) -> list[str]:
    """Split a list field on unescaped ``;`` and unescape ``\\;``."""
    if field_value == "-" or not field_value:
        return []
    out: list[str] = []
    cur: list[str] = []
    i = 0
    while i < len(field_value):
        ch = field_value[i]
        if ch == "\\" and i + 1 < len(field_value) and field_value[i + 1] == ";":
            cur.append(";")
            i += 2
            continue
        if ch == ";":
            out.append("".join(cur))
            cur = []
            i += 1
            continue
        cur.append(ch)
        i += 1
    out.append("".join(cur))
    return out


def validate(packet: str) -> ValidationReport:
    lines = packet.splitlines()
    violations: list[str] = []
    counts: dict[str, int] = {
        "tables": 0, "cols": 0, "rels": 0, "metrics": 0, "enums": 0,
        "rules": 0, "ambigs": 0, "verified_queries": 0, "warns": 0,
    }

    open_tags = sum(1 for ln in lines if ln.startswith("<DB_CONTEXT "))
    close_tags = sum(1 for ln in lines if ln.startswith("</DB_CONTEXT>"))
    if open_tags != 1:
        violations.append(f"v1: expected 1 opening DB_CONTEXT, found {open_tags}")
    if close_tags != 1:
        violations.append(f"v2: expected 1 closing DB_CONTEXT, found {close_tags}")
    if open_tags >= 1:
        first = next((ln for ln in lines if ln.startswith("<DB_CONTEXT ")), "")
        if 'version="LSF-1"' not in first:
            violations.append("v3: wrapper missing version=\"LSF-1\"")
        m = re.search(r'dialect="([^"]+)"', first)
        if not m or not m.group(1):
            violations.append("v4: wrapper missing dialect=...")

    # Walk lines once, skipping content inside SQL literal blocks. Counters
    # and reference checks must not see lines inside `<<` ... `>>SQL`.
    in_sql_block = False
    sql_id_open: str | None = None
    record_lines: list[tuple[str, str]] = []  # (record_kind, line)
    sql_blocks: list[str] = []  # IDs of SQL blocks observed
    for raw in lines:
        if in_sql_block:
            if raw.strip() == ">>SQL":
                in_sql_block = False
                sql_id_open = None
            continue
        if raw.startswith("SQL|") and "<<" in raw:
            sql_id_open = raw[len("SQL|"): raw.index("<<")]
            sql_blocks.append(sql_id_open)
            in_sql_block = True
            continue
        if raw.strip() == ">>SQL":
            violations.append("v19: stray >>SQL marker")
            continue
        kind = raw.split("|", 1)[0] if "|" in raw else raw.strip()
        record_lines.append((kind, raw))
    if in_sql_block:
        violations.append(f"v19: SQL block {sql_id_open} not terminated")

    domain_count = sum(1 for k, _ in record_lines if k == "DOMAIN")
    if domain_count != 1:
        violations.append(f"v5: expected 1 DOMAIN, found {domain_count}")

    coverage_count = sum(1 for k, _ in record_lines if k == "COVERAGE")
    if coverage_count > 1:
        violations.append(f"v8: expected at most 1 COVERAGE, found {coverage_count}")

    has_tables = any(k == "TABLES" for k, _ in record_lines)
    has_rels = any(k == "RELATIONSHIPS" for k, _ in record_lines)
    if not has_tables:
        violations.append("v6: missing TABLES section")
    if not has_rels:
        violations.append("v7: missing RELATIONSHIPS section")

    table_ids: set[str] = set()
    column_refs: set[str] = set()  # "{table_id}.{col}"
    tables_with_cols: set[str] = set()
    tables_pending_cols_header: set[str] = set()
    fk_cols: list[tuple[str, str, str]] = []  # (table_id, col, ref)

    for kind, raw in record_lines:
        if kind == "TABLE":
            f = _split_record(raw)
            if len(f) != _FIELD_LEN["TABLE"]:
                violations.append(f"v9: TABLE wrong field count: {raw[:80]}")
                continue
            counts["tables"] += 1
            table_ids.add(f[1])
            tables_pending_cols_header.add(f[1])
        elif kind == "COLS":
            f = _split_record(raw)
            if len(f) < 9 or f[2:9] != ["name", "type", "role", "ref", "desc", "examples", "tags"]:
                violations.append(f"v12: COLS header wrong order: {raw[:80]}")
                continue
            tables_pending_cols_header.discard(f[1])
        elif kind == "COL":
            f = _split_record(raw)
            if len(f) != _FIELD_LEN["COL"]:
                violations.append(f"v9: COL wrong field count: {raw[:80]}")
                continue
            counts["cols"] += 1
            t_id, name, _ttype, role, ref = f[1], f[2], f[3], f[4], f[5]
            if t_id not in table_ids:
                violations.append(f"v10: COL {name} references unknown table {t_id}")
                continue
            if t_id in tables_pending_cols_header:
                violations.append(f"v12: missing COLS header before COL for {t_id}")
                tables_pending_cols_header.discard(t_id)
            tables_with_cols.add(t_id)
            column_refs.add(f"{t_id}.{name}")
            if "FK" in role.split("+"):
                fk_cols.append((t_id, name, ref))

    for t_id in table_ids:
        if t_id not in tables_with_cols:
            violations.append(f"v11: table {t_id} has no columns")

    for t_id, name, ref in fk_cols:
        if ref == "-":
            continue
        if ref not in column_refs:
            violations.append(f"v13: FK {t_id}.{name} ref {ref!r} not defined")

    for kind, raw in record_lines:
        if kind != "REL":
            continue
        f = _split_record(raw)
        if len(f) != _FIELD_LEN["REL"]:
            violations.append(f"v9: REL wrong field count: {raw[:80]}")
            continue
        counts["rels"] += 1
        if f[2] not in column_refs:
            violations.append(f"v14: REL {f[1]} left_col {f[2]} not defined")
        if f[3] not in column_refs:
            violations.append(f"v14: REL {f[1]} right_col {f[3]} not defined")

    metric_ids: set[str] = set()
    for kind, raw in record_lines:
        if kind != "METRIC":
            continue
        f = _split_record(raw)
        if len(f) != _FIELD_LEN["METRIC"]:
            violations.append(f"v9: METRIC wrong field count: {raw[:80]}")
            continue
        counts["metrics"] += 1
        metric_ids.add(f[1])
        if f[3] not in table_ids:
            violations.append(f"v15: METRIC {f[1]} base_table {f[3]} not defined")
        for dim in _list_items(f[8]):
            if dim and dim not in column_refs:
                violations.append(f"v16: METRIC {f[1]} allowed_dim {dim} not defined")

    for kind, raw in record_lines:
        if kind != "ENUM":
            continue
        f = _split_record(raw)
        if len(f) != _FIELD_LEN["ENUM"]:
            violations.append(f"v9: ENUM wrong field count: {raw[:80]}")
            continue
        counts["enums"] += 1
        if f[2] not in column_refs:
            violations.append(f"v17: ENUM {f[1]} column_ref {f[2]} not defined")

    object_ids = table_ids | column_refs | metric_ids
    vq_ids: set[str] = set()
    for kind, raw in record_lines:
        if kind == "WARN":
            f = _split_record(raw)
            if len(f) != _FIELD_LEN["WARN"]:
                violations.append(f"v9: WARN wrong field count: {raw[:80]}")
                continue
            counts["warns"] += 1
            target = f[1]
            if "." in target and target not in object_ids:
                violations.append(f"v18: WARN target {target} not defined")
        elif kind == "AMBIG":
            counts["ambigs"] += 1
        elif kind == "RULE":
            counts["rules"] += 1
        elif kind == "VQ":
            counts["verified_queries"] += 1
            f = _split_record(raw)
            if len(f) >= 2:
                vq_ids.add(f[1])

    for sql_id in sql_blocks:
        if sql_id not in vq_ids:
            violations.append(f"v20: SQL block {sql_id} has no matching VQ")

    return ValidationReport(violations=violations, counts=counts)


# ----- model selection --------------------------------------------------

def _select_models(
    ctx_models: Mapping[str, Any],
    *,
    include: tuple[IncludeToken, ...],
    explicit_tables: set[str] | None,
) -> list[Any]:
    out: list[Any] = []
    want_main = "main" in include
    want_external = "external" in include
    for m in ctx_models.values():
        kind = m.kind.name
        is_main = m.name.startswith("main_models.") and kind != "EXTERNAL"
        is_external = kind == "EXTERNAL"
        if explicit_tables is not None:
            if m.name in explicit_tables:
                out.append(m)
            continue
        if want_main and is_main:
            out.append(m)
        elif want_external and is_external:
            out.append(m)
    return out


# ----- main -------------------------------------------------------------

def _parse_include(value: str) -> tuple[IncludeToken, ...]:
    raw = [p.strip().lower() for p in value.split(",") if p.strip()]
    out: list[IncludeToken] = []
    for tok in raw:
        if tok == "all":
            out = list(ALL_INCLUDE)
            break
        if tok in ALL_INCLUDE:
            out.append(tok)  # type: ignore[arg-type]
        else:
            raise SystemExit(f"unknown --include token {tok!r}; valid: {ALL_INCLUDE} or 'all'")
    return tuple(dict.fromkeys(out))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--include",
        default=",".join(DEFAULT_INCLUDE),
        help="Comma list of {semantic,main,seeds,external} or 'all'.",
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--supplement", type=Path, default=DEFAULT_SUPPLEMENT)
    parser.add_argument(
        "--tables",
        default="",
        help="Comma list of fully-qualified model names (overrides --include for physical tables).",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Run §16 structural validation post-emit; non-zero exit on violations.",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(levelname)s %(name)s: %(message)s",
    )
    include = _parse_include(args.include)
    explicit_tables = (
        {s.strip() for s in args.tables.split(",") if s.strip()}
        if args.tables
        else None
    )

    supplement = _load_supplement(args.supplement)
    logger.info(
        "loaded supplement: %d rules, %d ambiguities, %d verified queries",
        len(supplement.rules), len(supplement.ambiguities), len(supplement.verified_queries),
    )

    from sqlmesh import Context  # type: ignore[import-not-found]

    ctx = Context(paths=[str(BC_DIR)])
    selected_models = _select_models(
        ctx.models, include=include, explicit_tables=explicit_tables
    )
    logger.info(
        "selected %d models from SQLMesh ctx (%d total)", len(selected_models), len(ctx.models)
    )

    seeds = _load_seeds() if "seeds" in include else []
    logger.info("loaded %d seeds", len(seeds))

    bsl_tables: list[Table] = []
    bsl_metrics: list[Metric] = []
    if "semantic" in include:
        from python_models.metrics import _constants as metric_constants  # type: ignore[import-not-found]
        from python_models.metrics import _metric_registrations  # noqa: F401
        from python_models.metrics import registry as metric_registry  # type: ignore[import-not-found]
        from semantic import _layout as bsl_layout  # type: ignore[import-not-found]

        bsl_tables, bsl_metrics = _build_bsl_tables(
            metric_registry, bsl_layout, supplement, metric_constants
        )

    resolved_cols: dict[str, dict[str, Any]] = {}
    keep_models: list[Any] = []
    pre_filter_count = len(selected_models)
    for m in selected_models:
        cols = _model_columns(m, ctx)
        if not cols:
            logger.warning("skipping %s: no columns at Context load and engine has none", m.name)
            continue
        resolved_cols[_physical_table_id(m.name)] = cols
        keep_models.append(m)
    selected_models = keep_models
    logger.info(
        "resolved columns for %d models (%d in scope skipped for empty schema)",
        len(keep_models), pre_filter_count - len(keep_models),
    )

    in_scope_columns: dict[str, set[str]] = {
        tid: set(cols.keys()) for tid, cols in resolved_cols.items()
    }
    for t in bsl_tables:
        in_scope_columns[t.table_id] = {c.name for c in t.columns}

    seed_tables = [_table_for_seed(s, supplement) for s in seeds]
    for st in seed_tables:
        in_scope_columns[st.table_id] = {c.name for c in st.columns}

    selected_table_ids = set(in_scope_columns.keys())

    relationships, fks_by_table = _extract_relationships(
        ctx.models, selected_table_ids, in_scope_columns
    )
    logger.info("extracted %d relationships", len(relationships))

    physical_tables: list[Table] = []
    for m in selected_models:
        tid = _physical_table_id(m.name)
        t = _model_table(
            m,
            fks=fks_by_table.get(tid, {}),
            supplement=supplement,
            columns_to_types=resolved_cols[tid],
        )
        if t is not None:
            physical_tables.append(t)

    enums = _build_enums(seeds, selected_table_ids)

    all_tables = bsl_tables + physical_tables + seed_tables

    packet = _emit_packet(
        supplement=supplement,
        tables=all_tables,
        relationships=relationships,
        metrics=bsl_metrics,
        enums=enums,
        dialect="duckdb",
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(packet)
    logger.info("wrote %s (%d bytes)", args.out, len(packet))

    if args.validate:
        report = validate(packet)
        for v in report.violations:
            logger.error("VIOLATION: %s", v)
        logger.info(
            "counts: tables=%d cols=%d rels=%d metrics=%d enums=%d rules=%d ambigs=%d vqs=%d warns=%d",
            report.counts["tables"],
            report.counts["cols"],
            report.counts["rels"],
            report.counts["metrics"],
            report.counts["enums"],
            report.counts["rules"],
            report.counts["ambigs"],
            report.counts["verified_queries"],
            report.counts["warns"],
        )
        if report.violations:
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
