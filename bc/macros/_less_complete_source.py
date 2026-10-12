from __future__ import annotations

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator

_FIRST_COMPLETE_BOX_SCORE_SEASON = 1875


@macro()
def is_less_complete_source(
    evaluator: MacroEvaluator, game_table: exp.Expression
) -> exp.Expression:
    del evaluator
    alias = game_table.name
    return exp.Paren(
        this=exp.or_(
            exp.column("is_negro_leagues", table=alias).is_(exp.true()),
            exp.and_(
                exp.column("source_type", table=alias).eq(
                    exp.Literal.string("BoxScore")
                ),
                exp.column("season", table=alias)
                < exp.Literal.number(_FIRST_COMPLETE_BOX_SCORE_SEASON),
            ),
        )
    )
