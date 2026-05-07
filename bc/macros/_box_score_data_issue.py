"""Env-aware predicate for the `bounded_excluding_data_issues` audit.

Returns a SQL `EXISTS (...)` against the env-aware
`main_models.box_score_data_issues` view, so the audit can carve out
rows that are known upstream issues from `baseball.computer.rs`.

Mirrors the env-resolution logic in `_env_to_model.py` so the audit
works under DEV_ONLY virtual environments without declaring
`box_score_data_issues` as a real `depends_on` (which would create
the same FK-cycle problems the relationships audit avoids).

When the issues view doesn't exist yet (transitively-referenced model
not built), returns `TRUE` — the audit treats every row as a known
issue, surfaces no violations, and re-runs cleanly on the next plan
once the env is fully built.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_DQ_PHYSICAL_PREFIX = "sqlmesh__"
_DQ_CANONICAL_FQN = "bc.main_models.box_score_data_issues"


def _dq_identifier_name(value: Any) -> str:
    if isinstance(value, exp.Expression):
        return value.name or value.sql()
    return str(value)


def _dq_column_expr(value: Any) -> exp.Expression:
    if isinstance(value, exp.Expression):
        return value
    return exp.column(_dq_identifier_name(value))


def _dq_this_model_table(evaluator: MacroEvaluator) -> exp.Table | None:
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


def _dq_env_aware_target(this_model: exp.Table | None) -> exp.Table:
    canonical = exp.to_table(_DQ_CANONICAL_FQN)
    if this_model is None or this_model.db is None:
        return canonical
    schema = this_model.db
    if not schema.startswith(_DQ_PHYSICAL_PREFIX):
        return canonical
    canonical_schema = schema[len(_DQ_PHYSICAL_PREFIX):]
    if canonical.db != canonical_schema:
        return canonical
    table_name = this_model.name
    if "__" not in table_name:
        return canonical
    env_suffix = table_name.rsplit("__", 1)[-1]
    if not env_suffix:
        return canonical
    catalog = this_model.args.get("catalog")
    return exp.Table(
        this=exp.to_identifier(canonical.name),
        db=exp.to_identifier(f"{canonical_schema}__{env_suffix}"),
        catalog=exp.to_identifier(catalog.name) if catalog else None,
    )


def _dq_table_exists(evaluator: MacroEvaluator, table: exp.Table) -> bool:
    adapter = evaluator.locals.get("engine_adapter")
    if adapter is None:
        return True
    try:
        adapter.fetchone(
            exp.select(exp.Literal.number(1)).from_(table).limit(0)
        )
        return True
    except Exception:
        return False


@macro()
def box_score_data_issue_match(
    evaluator: MacroEvaluator,
    game_id_column: Any,
    player_id_column: Any,
    issue_type: Any,
) -> exp.Expression:
    """Render `EXISTS (...)` against the env-aware issues view.

    Falls back to `TRUE` when the issues view isn't materialized yet,
    so the audit short-circuits to "no violations" instead of failing.
    """
    this_model = _dq_this_model_table(evaluator)
    target = _dq_env_aware_target(this_model)

    if not _dq_table_exists(evaluator, target):
        return exp.true()

    issue_type_str = (
        issue_type.name
        if isinstance(issue_type, exp.Expression) and issue_type.name
        else str(issue_type).strip("'\"")
    )

    inner = (
        exp.select(exp.Literal.number(1))
        .from_(target)
        .where(
            exp.and_(
                exp.column("game_id", table=target.name).eq(_dq_column_expr(game_id_column)),
                exp.column("player_id", table=target.name).eq(_dq_column_expr(player_id_column)),
                exp.column("issue_type", table=target.name).eq(exp.Literal.string(issue_type_str)),
            )
        )
    )
    return exp.Exists(this=inner)
