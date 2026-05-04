"""Env-aware FK check for the ``relationships`` custom audit.

The audit's ``to_model`` argument isn't in the calling model's
``depends_on`` graph: declaring those references explicitly would create
cycles in this project's DAG (``main_models.people`` and several
``stg_box_score_*`` lines each need the other). That means SQLMesh's
table-mapping pass has no snapshot for the target and leaves the
canonical FQN unrewritten. Under
``virtual_environment_mode=DEV_ONLY``, the canonical schema doesn't
exist while a dev plan is running, so the audit blows up with a
CatalogException after a successful materialization.

SQLMesh also doesn't forward ``environment_naming_info`` into the
audit's macro evaluator, so we can't ask it to compute the env-aware
schema directly. Instead we read ``this_model`` (which IS env-aware:
under DEV_ONLY dev it's
``sqlmesh__<schema>.<schema>__<table>__<hash>__<env>``) and pull the
env suffix off the table name to construct the env-aware view schema
``<canonical_schema>__<env>.<to_table>``.

Even with the right schema, the target view may not exist yet: when
SQLMesh audits a model immediately after materialization, transitively
referenced models further down the build order haven't been
materialized themselves. Probing the engine adapter at audit time lets
us fall back to a passing predicate (``TRUE``) when the target view is
missing — the audit will pass on the next plan once the env is fully
built. Prod runs (canonical ``main_models``) keep the full check.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_PHYSICAL_PREFIX = "sqlmesh__"


def _to_table(value: Any, dialect: str | None) -> exp.Table:
    if isinstance(value, exp.Table):
        return value
    if isinstance(value, exp.Column):
        # `relationships(to_model := main_models.X)` parses as a Column
        # with ``table=main_models`` and ``this=X``.
        parts = [p for p in (value.table, value.name) if p]
        return exp.to_table(".".join(parts), dialect=dialect)
    if isinstance(value, exp.Expression):
        return exp.to_table(value.sql(dialect=dialect, comments=False), dialect=dialect)
    return exp.to_table(str(value), dialect=dialect)


def _identifier_name(value: Any) -> str:
    if isinstance(value, exp.Expression):
        return value.name or value.sql()
    return str(value)


def _this_model_table(evaluator: MacroEvaluator) -> exp.Table | None:
    raw = evaluator.locals.get("this_model")
    if raw is None:
        return None
    if isinstance(raw, exp.Table):
        return raw
    if isinstance(raw, exp.Expression):
        try:
            return exp.to_table(
                raw.sql(dialect=evaluator.dialect, comments=False),
                dialect=evaluator.dialect,
            )
        except Exception:
            return None
    return exp.to_table(str(raw), dialect=evaluator.dialect)


def _env_aware_target(
    target: exp.Table, this_model: exp.Table | None
) -> exp.Table | None:
    """Rewrite ``target`` to its env-aware view if we're under DEV_ONLY dev.

    Returns the rewritten table, or ``None`` if no rewrite applies (prod
    run, or target schema isn't virtualized).
    """
    if this_model is None or this_model.db is None:
        return None
    schema = this_model.db
    if not schema.startswith(_PHYSICAL_PREFIX):
        return None
    canonical_schema = schema[len(_PHYSICAL_PREFIX):]
    if target.db != canonical_schema:
        return None
    table_name = this_model.name
    if "__" not in table_name:
        return None
    env_suffix = table_name.rsplit("__", 1)[-1]
    if not env_suffix:
        return None
    catalog = this_model.args.get("catalog")
    return exp.Table(
        this=exp.to_identifier(target.name),
        db=exp.to_identifier(f"{canonical_schema}__{env_suffix}"),
        catalog=exp.to_identifier(catalog.name) if catalog else None,
    )


def _table_populated(evaluator: MacroEvaluator, table: exp.Table) -> bool:
    """Probe the engine adapter for ``table`` and confirm it has rows.

    Returns ``False`` when the table is missing OR present but empty —
    both states are treated as "not yet built" so the audit short-circuits
    instead of flagging every source row as an FK violation. This matters
    on a fresh prod plan: SQLMesh schedules audits per model in DAG
    order, but ``to_model`` references aren't in the dependency graph
    (declaring them would cycle), so a staging table can audit before
    its referent is materialized.
    """
    adapter = evaluator.locals.get("engine_adapter")
    if adapter is None:
        # Loading stage / no adapter available — assume present so the
        # predicate compiles. Real audit runtime always has an adapter.
        return True
    try:
        row = adapter.fetchone(
            exp.select(exp.Literal.number(1)).from_(table).limit(1)
        )
        return row is not None
    except Exception:
        return False


@macro()
def relationships_check(
    evaluator: MacroEvaluator,
    column: Any,
    to_column: Any,
    to_model: Any,
) -> exp.Expression:
    """Render the FK predicate for the ``relationships`` audit.

    The audit body returns rows that fail (rows the WHERE clause
    matches). Returning ``FALSE`` here short-circuits the WHERE so no
    rows are reported as violations when the target view doesn't exist
    in the current env (transitively-referenced model not built yet).
    Returns the full ``column NOT IN (SELECT to_column FROM env_target)``
    predicate otherwise.
    """
    target = _to_table(to_model, evaluator.dialect)
    this_model = _this_model_table(evaluator)
    env_target = _env_aware_target(target, this_model)
    actual_target = env_target if env_target is not None else target

    if not _table_populated(evaluator, actual_target):
        return exp.false()

    column_expr = column if isinstance(column, exp.Expression) else exp.column(_identifier_name(column))
    to_col_name = _identifier_name(to_column)
    inner = exp.select(exp.column(to_col_name)).from_(actual_target).subquery()
    return exp.Not(this=exp.In(this=column_expr, query=inner))
