"""Render registry Metric lambdas as DuckDB SQL text.

The registry stores each metric as ibis lambdas. Evaluating those lambdas
against proxy objects that build SQL strings instead of ibis expressions
yields the text the LSF-1 packet publishes as a metric's ``expr`` and the
bodies of the ``metrics.<name>(...)`` macros in the DuckLake catalog. No
ibis or DuckDB import is needed, so the SQLMesh build group, the BSL
group, and the publish scripts can all use it.
"""

# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import final, override

from .registry import Metric, MetricKind, MetricSource

MetricSlice = tuple[MetricKind, MetricSource, list[Metric]]

_PREC_ATOM = 0
_PREC_MUL = 1
_PREC_ADD = 2
_PREC_CMP = 3

_OP_PREC: dict[str, int] = {
    "*": _PREC_MUL,
    "/": _PREC_MUL,
    "+": _PREC_ADD,
    "-": _PREC_ADD,
    ">": _PREC_CMP,
    ">=": _PREC_CMP,
    "<": _PREC_CMP,
    "<=": _PREC_CMP,
    "=": _PREC_CMP,
    "<>": _PREC_CMP,
}
_NON_ASSOCIATIVE = {"-", "/"}


def _merge(left: tuple[str, ...], right: tuple[str, ...]) -> tuple[str, ...]:
    out = list(left)
    for c in right:
        if c not in out:
            out.append(c)
    return tuple(out)


def _literal(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return repr(value)


@final
class SqlText:
    """A SQL fragment with enough precedence information to parenthesize.

    Division renders as ``left / nullif(right, 0)`` whenever the divisor
    reads a column, so every published expression returns NULL rather
    than inf or NaN on an empty denominator. The ibis lambdas themselves
    divide plainly, which is why the stored rate columns on the
    ``metrics_*`` tables can hold inf or NaN where the macros hold NULL.
    """

    __slots__: tuple[str, ...] = ("text", "prec", "is_bool", "columns")

    def __init__(
        self,
        text: str,
        prec: int = _PREC_ATOM,
        *,
        is_bool: bool = False,
        columns: tuple[str, ...] = (),
    ) -> None:
        self.text: str = text
        self.prec: int = prec
        self.is_bool: bool = is_bool
        self.columns: tuple[str, ...] = columns

    @override
    def __str__(self) -> str:
        return self.text

    @override
    def __repr__(self) -> str:
        return f"SqlText({self.text!r})"

    def wrapped(self, parent_prec: int, *, right_of: str | None = None) -> str:
        needs = self.prec > parent_prec or (
            right_of in _NON_ASSOCIATIVE and self.prec == parent_prec
        )
        return f"({self.text})" if needs else self.text

    def _binary(self, other: object, op: str, *, reverse: bool = False) -> SqlText:
        rhs = other if isinstance(other, SqlText) else SqlText(_literal(other))
        left, right = (rhs, self) if reverse else (self, rhs)
        prec = _OP_PREC[op]
        if op == "/" and right.columns:
            right = SqlText(f"nullif({right.text}, 0)", columns=right.columns)
        text = f"{left.wrapped(prec)} {op} {right.wrapped(prec, right_of=op)}"
        return SqlText(
            text,
            prec,
            is_bool=prec == _PREC_CMP,
            columns=_merge(left.columns, right.columns),
        )

    def __add__(self, other: object) -> SqlText:
        return self._binary(other, "+")

    def __radd__(self, other: object) -> SqlText:
        return self._binary(other, "+", reverse=True)

    def __sub__(self, other: object) -> SqlText:
        return self._binary(other, "-")

    def __rsub__(self, other: object) -> SqlText:
        return self._binary(other, "-", reverse=True)

    def __mul__(self, other: object) -> SqlText:
        return self._binary(other, "*")

    def __rmul__(self, other: object) -> SqlText:
        return self._binary(other, "*", reverse=True)

    def __truediv__(self, other: object) -> SqlText:
        return self._binary(other, "/")

    def __rtruediv__(self, other: object) -> SqlText:
        return self._binary(other, "/", reverse=True)

    def __gt__(self, other: object) -> SqlText:
        return self._binary(other, ">")

    def __ge__(self, other: object) -> SqlText:
        return self._binary(other, ">=")

    def __lt__(self, other: object) -> SqlText:
        return self._binary(other, "<")

    def __le__(self, other: object) -> SqlText:
        return self._binary(other, "<=")

    @override
    def __eq__(self, other: object) -> SqlText:
        return self._binary(other, "=")

    @override
    def __ne__(self, other: object) -> SqlText:
        return self._binary(other, "<>")

    @override
    def __hash__(self) -> int:
        return hash(self.text)

    def __bool__(self) -> bool:
        raise TypeError(
            f"SqlText {self.text!r} has no truth value; metric lambdas must stay "
            "expression-only so they render to SQL"
        )

    def sum(self) -> SqlText:
        inner = (
            f"case when {self.text} then 1 else 0 end" if self.is_bool else self.text
        )
        return SqlText(f"sum({inner})", columns=self.columns)

    def fill_null(self, value: object) -> SqlText:
        return SqlText(
            f"coalesce({self.text}, {_literal(value)})", columns=self.columns
        )

    def round(self, digits: int) -> SqlText:
        return SqlText(f"round({self.text}, {digits})", columns=self.columns)


@final
class _TableProxy:
    """Stands in for the ibis table: attribute or item access names a column."""

    __slots__: tuple[str, ...] = ("_rename",)

    def __init__(self, rename: Callable[[str], str] | None = None) -> None:
        self._rename: Callable[[str], str] | None = rename

    def _column(self, name: str) -> SqlText:
        text = self._rename(name) if self._rename else name
        return SqlText(text, columns=(name,))

    def __getattr__(self, name: str) -> SqlText:
        if name.startswith("__"):
            raise AttributeError(name)
        return self._column(name)

    def __getitem__(self, name: str) -> SqlText:
        return self._column(name)


@final
class _MeasureProxy:
    """Stands in for the derived-metric scope: attribute access names a sibling."""

    __slots__: tuple[str, ...] = ("_resolve",)

    def __init__(self, resolve: Callable[[str], SqlText]) -> None:
        self._resolve: Callable[[str], SqlText] = resolve

    def __getattr__(self, name: str) -> SqlText:
        if name.startswith("__"):
            raise AttributeError(name)
        return self._resolve(name)


@dataclass(frozen=True)
class RenderedMetric:
    metric: Metric
    expr: str
    expanded: str
    columns: tuple[str, ...]

    @property
    def name(self) -> str:
        return self.metric.name


def _render_base(metric: Metric, table: _TableProxy) -> SqlText:
    if metric.formula is not None:
        return metric.formula(table)
    assert metric.numerator is not None and metric.denominator is not None
    return metric.numerator(table) / metric.denominator(table)


def _render_expanded(
    metrics: dict[str, Metric],
    name: str,
    rename: Callable[[str], str] | None,
    memo: dict[str, SqlText],
    stack: tuple[str, ...] = (),
) -> SqlText:
    if name in memo:
        return memo[name]
    if name in stack:
        raise ValueError(f"derived metric cycle: {' -> '.join([*stack, name])}")
    metric = metrics.get(name)
    if metric is None:
        raise ValueError(
            f"metric {stack[-1] if stack else '?'!r} references unknown measure {name!r}"
        )
    if metric.derived is None:
        out = _render_base(metric, _TableProxy(rename))
    else:
        scope = _MeasureProxy(
            lambda dep: _render_expanded(metrics, dep, rename, memo, (*stack, name))
        )
        out = metric.derived(scope)
    memo[name] = out
    return out


def render_metrics(metrics: Iterable[Metric]) -> list[RenderedMetric]:
    """Render every metric of one (kind, source) slice.

    ``expr`` names sibling measures for derived metrics, matching how the
    registry composes them; ``expanded`` substitutes those siblings down
    to base-table columns so the text stands alone inside a macro.
    """
    ordered = list(metrics)
    by_name = {m.name: m for m in ordered}
    memo: dict[str, SqlText] = {}
    out: list[RenderedMetric] = []
    for m in ordered:
        expanded = _render_expanded(by_name, m.name, None, memo)
        if m.derived is None:
            expr = expanded
        else:
            expr = m.derived(_MeasureProxy(lambda dep: SqlText(dep)))
        out.append(
            RenderedMetric(
                metric=m,
                expr=expr.text,
                expanded=expanded.text,
                columns=expanded.columns,
            )
        )
    return out


@dataclass(frozen=True)
class MacroSpec:
    name: str
    params: tuple[str, ...]
    body: str
    members: tuple[tuple[MetricKind, MetricSource], ...]

    @property
    def signature(self) -> str:
        return f"{self.name}({', '.join(self.params)})"


def _positional_body(
    metrics: Iterable[Metric], name: str, columns: tuple[str, ...]
) -> str:
    by_name = {m.name: m for m in metrics}
    index = {c: i for i, c in enumerate(columns)}
    rendered = _render_expanded(by_name, name, lambda c: f"__arg{index[c]}", {})
    return rendered.text


def macro_specs(slices: Iterable[MetricSlice]) -> list[MacroSpec]:
    """One macro per distinct metric name across every (kind, source) slice.

    Two slices may register the same name (``walk_rate`` for offense over
    plate appearances and for pitching over batters faced). Those share a
    macro when their bodies agree position-for-position; a name whose
    bodies disagree raises so the registry is fixed before anything is
    published under a misleading name.
    """
    specs: dict[str, MacroSpec] = {}
    shapes: dict[str, str] = {}
    for kind, source, metrics in slices:
        for r in render_metrics(metrics):
            shape = _positional_body(metrics, r.name, r.columns)
            existing = specs.get(r.name)
            if existing is None:
                specs[r.name] = MacroSpec(
                    name=r.name,
                    params=r.columns,
                    body=r.expanded,
                    members=((kind, source),),
                )
                shapes[r.name] = shape
                continue
            if shapes[r.name] != shape:
                raise ValueError(
                    f"metric {r.name!r} has different definitions across slices "
                    f"({existing.members[0]} vs {(kind, source)}); give them distinct names"
                )
            specs[r.name] = MacroSpec(
                name=existing.name,
                params=existing.params,
                body=existing.body,
                members=(*existing.members, (kind, source)),
            )
    return list(specs.values())
