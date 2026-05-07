"""Env-aware predicate for the `team_game_has_one_starter` audit's carve-out.

Returns a SQL `EXISTS (...)` against the env-aware
`main_models.team_game_data_issues` view so the audit can let through
team-games where standard invariants don't apply because of source-level
data artifacts (e.g. a scratched starting pitcher who never appeared in
events).

Same env-resolution shape as `_box_score_data_issue.py` and
`_env_to_model.py`; declared as a separate macro so the FK-cycle
exclusions stay symmetric across the three.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_TG_PHYSICAL_PREFIX = "sqlmesh__"
_TG_CANONICAL_FQN = "bc.main_models.team_game_data_issues"


def _tg_identifier_name(value: Any) -> str:
    if isinstance(value, exp.Expression):
        return value.name or value.sql()
    return str(value)


def _tg_column_expr(value: Any) -> exp.Expression:
    if isinstance(value, exp.Expression):
        return value
    return exp.column(_tg_identifier_name(value))


def _tg_this_model_table(evaluator: MacroEvaluator) -> exp.Table | None:
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


def _tg_env_aware_target(this_model: exp.Table | None) -> exp.Table:
    canonical = exp.to_table(_TG_CANONICAL_FQN)
    if this_model is None or this_model.db is None:
        return canonical
    schema = this_model.db
    if not schema.startswith(_TG_PHYSICAL_PREFIX):
        return canonical
    canonical_schema = schema[len(_TG_PHYSICAL_PREFIX):]
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


def _tg_table_exists(evaluator: MacroEvaluator, table: exp.Table) -> bool:
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
def team_game_data_issue_match(
    evaluator: MacroEvaluator,
    game_id_column: Any,
    team_id_column: Any,
    issue_type: Any,
) -> exp.Expression:
    """Render `EXISTS (...)` against the env-aware team-game issues view.

    Falls back to `TRUE` when the issues view isn't materialized yet so
    the carve-out short-circuits to "every row excluded" until the env
    is fully built.
    """
    this_model = _tg_this_model_table(evaluator)
    target = _tg_env_aware_target(this_model)

    if not _tg_table_exists(evaluator, target):
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
                exp.column("game_id", table=target.name).eq(_tg_column_expr(game_id_column)),
                exp.column("team_id", table=target.name).eq(_tg_column_expr(team_id_column)),
                exp.column("issue_type", table=target.name).eq(exp.Literal.string(issue_type_str)),
            )
        )
    )
    return exp.Exists(this=inner)
