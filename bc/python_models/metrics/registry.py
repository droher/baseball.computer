"""Pydantic Metric class + METRICS registry for the 9 metrics_* tables.

Phase 2 only uses ``formula`` (single lambda for one-shot expressions)
or ``numerator``/``denominator`` (semantic-layer-friendly ratio) so the
generated SQL matches the Phase 1.6 macro literally. Phase 3 will turn
on ``derived`` for true composition (e.g. OPS = OBP + SLG).
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


class Metric(BaseModel):
    """One named ratio/formula computed over the basic_stats / event_agg CTEs.

    Exactly one of {formula, numerator+denominator, derived} must be set.
    Phase 2 uses ``formula`` and ``numerator/denominator``; ``derived``
    is reserved for Phase 3 composition.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    name: str
    kind: MetricKind
    source: MetricSource
    dtype: str = "DOUBLE"
    formula: Callable[[TableExpr], IbisExpr] | None = None
    numerator: Callable[[TableExpr], IbisExpr] | None = None
    denominator: Callable[[TableExpr], IbisExpr] | None = None
    derived: Callable[[TableExpr, dict[str, IbisExpr]], IbisExpr] | None = None

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
        """Apply the metric to an Ibis table expression."""
        if self.formula is not None:
            return self.formula(t)
        if self.numerator is not None and self.denominator is not None:
            return self.numerator(t) / self.denominator(t)
        raise NotImplementedError(
            f"Metric {self.name!r}: 'derived' form is Phase 3 — not used yet"
        )


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
