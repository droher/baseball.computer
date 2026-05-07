"""BSL ``SemanticTable`` factories for offense/pitching/fielding × season/event.

Each factory wires the same Pydantic ``Metric`` objects used by the
build-time ``metrics_*`` tables into BSL measures. ``Metric.derived``
lambdas hand directly to ``with_measures``: BSL's ``_classify_measure``
introspects each lambda and tags it ``[calc]`` if it references other
registered measures, otherwise ``[base]``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boring_semantic_layer as bsl
import ibis

from python_models.metrics import _metric_registrations  # noqa: F401  (registry side-effect)
from python_models.metrics.registry import metrics_for

from ._layout import (
    EVENT_DIM_NAMES,
    FIELDING_SEASON_DIM_NAMES,
    OFFENSE_PITCHING_SEASON_DIM_NAMES,
)
from ._tables_common import (
    Env,
    MetricKind,
    event_with_game_info,
    season_with_league,
)

DEFAULT_DB_PATH = Path(__file__).resolve().parents[2] / "bc.db"


def connect(env: Env = "dev", db_path: Path | str | None = None) -> Any:
    """Open ``bc.db`` read-only for BSL queries.

    The returned connection carries no env state; pass the same ``env``
    string into the table factories so they pick the right schema
    (``main_models__dev`` vs ``main_models``).
    """
    path = Path(db_path) if db_path is not None else DEFAULT_DB_PATH
    return ibis.duckdb.connect(str(path), read_only=True)


def _build_table(
    con: Any,
    kind: MetricKind,
    env: Env,
    grain: str,
    name: str,
    dimensions: dict[str, Any],
) -> Any:
    if grain == "season":
        backing = season_with_league(con, kind, env, regular_season_only=True)
    else:
        backing = event_with_game_info(con, kind, env)

    metrics = metrics_for(kind, grain)  # type: ignore[arg-type]
    # BSL classifies measures by introspecting the lambda's body. Bound
    # methods like ``m.evaluate`` confuse the classifier (it can't see
    # past ``self``), so wrap each base metric in a fresh ``lambda t``.
    # Derived lambdas already match BSL's MeasureScope shape.
    base = {
        m.name: (lambda t, _m=m: _m.evaluate(t))
        for m in metrics if m.derived is None
    }
    calc = {m.name: m.derived for m in metrics if m.derived is not None}

    # Two with_measures calls so BSL classifies derived lambdas after
    # base measures already exist in scope. Within each call BSL
    # introspects against the running scope, so a derived metric whose
    # deps are themselves derived still resolves (graph traversal).
    table = (
        bsl.to_semantic_table(backing, name=name)
        .with_dimensions(**dimensions)
        .with_measures(**base)
        .with_measures(**calc)
    )
    return table


def _attr_dims(names: list[str]) -> dict[str, Any]:
    return {n: (lambda t, _n=n: getattr(t, _n)) for n in names}


_OFFENSE_PITCHING_SEASON_DIMS: dict[str, Any] = _attr_dims(
    OFFENSE_PITCHING_SEASON_DIM_NAMES
)
_FIELDING_SEASON_DIMS: dict[str, Any] = _attr_dims(FIELDING_SEASON_DIM_NAMES)
_EVENT_DIMS: dict[str, Any] = _attr_dims(EVENT_DIM_NAMES)


def offense_seasons(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "offense", env, "season", "offense_seasons",
        _OFFENSE_PITCHING_SEASON_DIMS,
    )


def offense_events(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "offense", env, "event", "offense_events", _EVENT_DIMS,
    )


def pitching_seasons(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "pitching", env, "season", "pitching_seasons",
        _OFFENSE_PITCHING_SEASON_DIMS,
    )


def pitching_events(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "pitching", env, "event", "pitching_events", _EVENT_DIMS,
    )


def fielding_seasons(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "fielding", env, "season", "fielding_seasons",
        _FIELDING_SEASON_DIMS,
    )


def fielding_events(con: Any, env: Env = "dev") -> Any:
    return _build_table(
        con, "fielding", env, "event", "fielding_events", _EVENT_DIMS,
    )
