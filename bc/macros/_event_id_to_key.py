"""Pack/unpack a (game_event_key, event_id) pair into a UINTEGER event_key."""

from __future__ import annotations

from sqlglot.expressions.core import Expression
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator


@macro()
def event_id_to_key(
    _evaluator: MacroEvaluator,
    event_id: Expression,
    game_event_key: Expression,
) -> str:
    return f"({game_event_key} // 255 * 255 + {event_id})::UINTEGER"


@macro()
def event_key_to_id(_evaluator: MacroEvaluator, event_key: Expression) -> str:
    return f"({event_key} - ({event_key} // 255))::UTINYINT"
