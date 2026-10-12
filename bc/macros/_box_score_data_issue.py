from __future__ import annotations

from typing import Any

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_DQ_CANONICAL_FQN = "bc.main_models.box_score_data_issues"


def _dq_identifier_name(value: Any) -> str:
    if isinstance(value, exp.Expression):
        return value.name or value.sql()
    return str(value)


def _dq_column_expr(value: Any) -> exp.Expression:
    if isinstance(value, exp.Expression):
        return value
    return exp.column(_dq_identifier_name(value))


def _dq_resolve_target(evaluator: MacroEvaluator) -> exp.Table:
    return exp.to_table(
        evaluator.resolve_table(_DQ_CANONICAL_FQN), dialect=evaluator.dialect
    )


def _dq_table_exists(evaluator: MacroEvaluator, table: exp.Table) -> bool:
    adapter = evaluator.locals.get("engine_adapter")
    if adapter is None:
        return True
    try:
        adapter.fetchone(exp.select(exp.Literal.number(1)).from_(table).limit(0))
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
    target = _dq_resolve_target(evaluator)

    if not _dq_table_exists(evaluator, target):
        return exp.false()

    issue_type_str = (
        issue_type.name
        if isinstance(issue_type, exp.Expression) and issue_type.name
        else str(issue_type).strip("'\"")
    )

    return exp.Exists(
        this=(
            exp.select(exp.Literal.number(1))
            .from_(target)
            .where(
                exp.and_(
                    exp.column("game_id", table=target.name).eq(
                        _dq_column_expr(game_id_column)
                    ),
                    exp.column("player_id", table=target.name).eq(
                        _dq_column_expr(player_id_column)
                    ),
                    exp.column("issue_type", table=target.name).eq(
                        exp.Literal.string(issue_type_str)
                    ),
                )
            )
        )
    )
