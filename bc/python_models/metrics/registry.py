"""Pydantic Metric class + METRICS registry for the 9 metrics_* tables.

Three forms of expression:

- ``formula(t)`` — single lambda over an Ibis table.
- ``numerator(t) / denominator(t)`` — semantic-friendly ratio.
- ``derived(m)`` — composition over already-evaluated measures, where
  ``m`` is a measure-scope: anything that responds to attribute access by
  name. Same shape as BSL's ``MeasureScope`` so the same lambda works in
  both ``build_metric_sql`` and ``bsl.SemanticTable.with_measures``.

``evaluate_all(t, metrics)`` runs a topological pass: non-derived metrics
first, then derived metrics resolve against the accumulated dict.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, model_validator

# Ibis expressions are `ir.Expr` at runtime. We type as ``Any`` here
# because (a) the public API surface is small enough that runtime checks
# do the work, and (b) ibis's type stubs are partial — pyright already
# yields reportUnknownMemberType across the codebase.
IbisExpr = Any
TableExpr = Any

MetricKind = Literal["offense", "pitching", "fielding"]
MetricSource = Literal["season", "event"]
MetricClassification = Literal["sum", "ratio", "derived"]


class _MeasureProxy:
    """Attribute-access view over a ``dict[str, IbisExpr]``.

    Used by ``evaluate_all`` to feed already-computed measures into a
    derived lambda. Mirrors BSL's MeasureScope shape so the same
    ``derived=lambda m: m.obp + m.slg`` works in both worlds.
    """

    __slots__ = ("_d",)

    def __init__(self, d: dict[str, IbisExpr]) -> None:
        self._d = d

    def __getattr__(self, name: str) -> IbisExpr:
        try:
            return self._d[name]
        except KeyError as e:
            raise AttributeError(
                f"derived metric references unknown measure {name!r}; "
                f"available: {sorted(self._d)}"
            ) from e


class _DepCaptureProxy:
    """Records every attribute access; arithmetic returns self.

    Probe object used to introspect a derived lambda's dependencies
    without evaluating real Ibis expressions. Any ``m.x`` access records
    ``x``; arithmetic / comparison / call all return ``self`` so chained
    expressions (``m.a + m.b * m.c``) keep flowing.
    """

    def __init__(self, sink: set[str]) -> None:
        object.__setattr__(self, "_sink", sink)

    def __getattr__(self, name: str) -> _DepCaptureProxy:
        # Dunders never name a sibling measure. Raising AttributeError
        # also keeps Python machinery (copy, deepcopy, repr) from
        # polluting the dep set.
        if name.startswith("__") and name.endswith("__"):
            raise AttributeError(name)
        self._sink.add(name)  # type: ignore[attr-defined]
        return self

    def _self(self, *_a: Any, **_k: Any) -> _DepCaptureProxy:
        return self

    def __bool__(self) -> bool:
        # Branching (``m.a if m.cond else m.b``) silently drops one
        # branch's deps because Python only evaluates the chosen side.
        # Force authors toward ``ibis.cases`` / ``ibis.coalesce``.
        raise TypeError(
            "derived metric lambdas must not branch on measure values "
            "(use ibis.cases / ibis.coalesce for conditionals)"
        )

    __add__ = __radd__ = _self
    __sub__ = __rsub__ = _self
    __mul__ = __rmul__ = _self
    __truediv__ = __rtruediv__ = _self
    __floordiv__ = __rfloordiv__ = _self
    __mod__ = __rmod__ = _self
    __pow__ = __rpow__ = _self
    __matmul__ = __rmatmul__ = _self
    __lshift__ = __rshift__ = _self
    __and__ = __or__ = __xor__ = _self
    __rand__ = __ror__ = __rxor__ = _self
    __invert__ = _self
    __neg__ = _self
    __pos__ = _self
    __abs__ = _self
    __lt__ = __le__ = __gt__ = __ge__ = _self
    __call__ = _self


class Metric(BaseModel):
    """One named ratio/formula computed over the basic_stats / event_agg CTEs.

    Exactly one of {formula, numerator+denominator, derived} must be set.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    name: str
    kind: MetricKind
    source: MetricSource
    dtype: str = "DOUBLE"
    formula: Callable[[TableExpr], IbisExpr] | None = None
    numerator: Callable[[TableExpr], IbisExpr] | None = None
    denominator: Callable[[TableExpr], IbisExpr] | None = None
    derived: Callable[[Any], IbisExpr] | None = None

    @property
    def classification(self) -> MetricClassification:
        """Coarse shape of this metric's expression.

        ``"sum"``     — single ``formula`` lambda; pure aggregation or
                        arithmetic over columns. Stays ``[base]`` in BSL.
        ``"ratio"``   — ``numerator / denominator`` form; one SUM/SUM
                        ratio. BSL flags as ``[calc]`` because it parses
                        as a BinOp.
        ``"derived"`` — ``derived`` lambda over the measure scope; a true
                        composite of other registered measures
                        (e.g. ``OPS = OBP + SLG``). Also ``[calc]`` in
                        BSL today.

        Used by ``classifications_for`` and the BSL semantic-layer
        consumers to recover the lost ratio-vs-composite distinction
        that BSL collapses into a single ``[calc]`` bucket.
        """
        if self.derived is not None:
            return "derived"
        if self.numerator is not None:
            return "ratio"
        return "sum"

    @model_validator(mode="after")
    def _exactly_one_form(self) -> Metric:
        forms = [
            self.formula is not None,
            self.numerator is not None and self.denominator is not None,
            self.derived is not None,
        ]
        if sum(forms) != 1:
            raise ValueError(
                f"Metric {self.name!r}: exactly one of "
                "(formula | numerator+denominator | derived) must be set"
            )
        if (self.numerator is None) != (self.denominator is None):
            raise ValueError(
                f"Metric {self.name!r}: numerator and denominator must come together"
            )
        return self

    def evaluate(self, t: TableExpr) -> IbisExpr:
        """Apply a non-derived metric to an Ibis table.

        Derived metrics need other measures in scope — use ``evaluate_all``.
        """
        if self.formula is not None:
            return self.formula(t)
        if self.numerator is not None and self.denominator is not None:
            return self.numerator(t) / self.denominator(t)
        raise ValueError(
            f"Metric {self.name!r}: derived form requires evaluate_all() "
            "(needs sibling measures in scope)"
        )

    def dependencies(self) -> set[str]:
        """Names this derived metric reads off the measure scope.

        Returns empty set for non-derived metrics.
        """
        if self.derived is None:
            return set()
        sink: set[str] = set()
        try:
            self.derived(_DepCaptureProxy(sink))
        except Exception as e:
            raise ValueError(
                f"Metric {self.name!r}: derived lambda raised during dependency "
                f"capture; the lambda must use only attribute access on the "
                f"scope (got {type(e).__name__}: {e})"
            ) from e
        return sink


def evaluate_all(
    t: TableExpr, metrics: list[Metric]
) -> dict[str, IbisExpr]:
    """Two-pass evaluator: non-derived first, then derived in topo order.

    Returns ``{name: ibis_expr}`` in registration order so downstream
    column emission stays stable. Raises on cycles or missing deps.
    """
    out: dict[str, IbisExpr] = {}

    base: list[Metric] = []
    derived: list[Metric] = []
    for m in metrics:
        (derived if m.derived is not None else base).append(m)

    for m in base:
        out[m.name] = m.evaluate(t)

    deps = {m.name: m.dependencies() for m in derived}
    by_name = {m.name: m for m in derived}

    visiting: set[str] = set()
    resolved: set[str] = set()
    order: list[str] = []

    def visit(name: str, stack: list[str]) -> None:
        if name in resolved or name not in by_name:
            return
        if name in visiting:
            cycle = " -> ".join([*stack, name])
            raise ValueError(f"derived metric cycle detected: {cycle}")
        visiting.add(name)
        for dep in deps[name]:
            if dep in by_name:
                visit(dep, [*stack, name])
            elif dep not in out:
                raise ValueError(
                    f"Metric {name!r}: derived references unknown measure "
                    f"{dep!r} (not in base or derived set)"
                )
        visiting.remove(name)
        resolved.add(name)
        order.append(name)

    for m in derived:
        visit(m.name, [])

    for name in order:
        m = by_name[name]
        assert m.derived is not None
        out[name] = m.derived(_MeasureProxy(out))

    return {m.name: out[m.name] for m in metrics}


MetricKey = tuple[str, MetricKind]
METRICS: dict[MetricKey, Metric] = {}


def register(metric: Metric) -> Metric:
    """Add a Metric to the global registry, keyed by (name, kind).

    Same metric name can appear under multiple kinds (e.g. ``walk_rate``
    is registered separately for offense and pitching). Re-registering
    the *same* (name, kind) is always an error — even if the formula
    callable matches — because that's almost certainly a copy-paste bug.
    """
    key = (metric.name, metric.kind)
    if key in METRICS:
        existing = METRICS[key]
        raise ValueError(
            f"Metric {metric.name!r} ({metric.kind}) already registered "
            f"(existing source={existing.source!r}, new source={metric.source!r}). "
            f"If both registrations are intentional, give them distinct names."
        )
    METRICS[key] = metric
    return metric


def metrics_for(kind: MetricKind, source: MetricSource) -> list[Metric]:
    """All metrics matching (kind, source), in registration order."""
    return [m for m in METRICS.values() if m.kind == kind and m.source == source]


def classifications_for(
    kind: MetricKind, source: MetricSource
) -> dict[str, MetricClassification]:
    """Classification per metric for a (kind, source) slice.

    Recovers the ratio-vs-derived distinction BSL collapses into a
    single ``[calc]`` bucket. ``"sum"`` matches BSL ``[base]``; ``"ratio"``
    and ``"derived"`` both fall under BSL ``[calc]``.
    """
    return {m.name: m.classification for m in metrics_for(kind, source)}
