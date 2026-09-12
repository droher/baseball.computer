"""The SQL rendered from registry metrics must agree with the ibis lambdas.

The renderer is the single source for the packet's METRIC expressions and
the published ``metrics`` macros, so it is checked against ibis itself on
random data rather than against hand-typed strings.
"""

from __future__ import annotations

import math
import random

import ibis
import pandas as pd
import pytest

from python_models.metrics.registry import (
    Metric,
    MetricKind,
    MetricSource,
    evaluate_all,
    metrics_for,
)
from python_models.metrics.sql_render import (
    MetricSlice,
    macro_specs,
    render_metrics,
)

KINDS: tuple[MetricKind, ...] = ("offense", "pitching", "fielding")
SOURCES: tuple[MetricSource, ...] = ("season", "event")


def _slices() -> list[MetricSlice]:
    return [(k, s, metrics_for(k, s)) for k in KINDS for s in SOURCES]


def _random_frame(columns: list[str], rows: int = 240, seed: int = 11) -> pd.DataFrame:
    rng = random.Random(seed)
    data = {"grp": [i % 6 for i in range(rows)]}
    for c in columns:
        data[c] = [rng.randint(1, 40) for _ in range(rows)]
    return pd.DataFrame(data)


@pytest.mark.parametrize("kind,source", [(k, s) for k in KINDS for s in SOURCES])
def test_rendered_sql_matches_ibis_evaluation(
    kind: MetricKind, source: MetricSource
) -> None:
    metrics = metrics_for(kind, source)
    rendered = render_metrics(metrics)
    columns = sorted({c for r in rendered for c in r.columns})
    frame = _random_frame(columns)

    con = ibis.duckdb.connect()
    t = con.create_table("t", frame)
    via_ibis = (
        t.group_by("grp")
        .aggregate(**evaluate_all(t, metrics))
        .to_pandas()
        .sort_values("grp")
        .reset_index(drop=True)
    )
    select = ", ".join(f"{r.expanded} AS {r.name}" for r in rendered)
    via_sql = (
        con.con.execute(f"SELECT grp, {select} FROM t GROUP BY grp ORDER BY grp")
        .df()
        .reset_index(drop=True)
    )
    assert list(via_sql.columns) == list(via_ibis.columns)
    for r in rendered:
        diff = (
            (via_sql[r.name].astype(float) - via_ibis[r.name].astype(float)).abs().max()
        )
        assert diff < 1e-9, f"{kind}/{source} {r.name}: max abs diff {diff}"


def test_packet_expr_names_siblings_for_derived_metrics() -> None:
    by_name = {r.name: r for r in render_metrics(metrics_for("offense", "season"))}
    ops = by_name["on_base_plus_slugging"]
    assert ops.expr == "on_base_percentage + slugging_percentage"
    assert ops.expanded == (
        f"{by_name['on_base_percentage'].expanded} + {by_name['slugging_percentage'].expanded}"
    )
    assert ops.columns == (
        "on_base_successes",
        "on_base_opportunities",
        "total_bases",
        "at_bats",
    )


def test_renderer_parenthesizes_by_precedence_and_sums_booleans() -> None:
    ratio_a = Metric(
        name="a",
        kind="offense",
        source="event",
        numerator=lambda t: (t.x - t.y).sum(),
        denominator=lambda t: t.z.sum(),
    )
    ratio_b = Metric(
        name="b",
        kind="offense",
        source="event",
        numerator=lambda t: t.x.sum() * 9,
        denominator=lambda t: (t.z / 3).sum(),
    )
    quotient = Metric(
        name="q",
        kind="offense",
        source="event",
        derived=lambda m: m.a / m.b,
    )
    flagged = Metric(
        name="f",
        kind="offense",
        source="event",
        numerator=lambda t: (t.x > 0).sum(),
        denominator=lambda t: t.y.fill_null(0).sum(),
    )
    out = {r.name: r for r in render_metrics([ratio_a, ratio_b, quotient, flagged])}
    assert out["a"].expanded == "sum(x - y) / nullif(sum(z), 0)"
    assert out["b"].expanded == "sum(x) * 9 / nullif(sum(z / 3), 0)"
    assert out["q"].expanded == (
        "sum(x - y) / nullif(sum(z), 0) / nullif(sum(x) * 9 / nullif(sum(z / 3), 0), 0)"
    )
    assert out["f"].expanded == (
        "sum(case when x > 0 then 1 else 0 end) / nullif(sum(coalesce(y, 0)), 0)"
    )
    assert out["q"].columns == ("x", "y", "z")


def test_macro_specs_cover_every_metric_name_once() -> None:
    specs = macro_specs(_slices())
    names = [s.name for s in specs]
    assert len(names) == len(set(names))
    registered = {m.name for _, _, ms in _slices() for m in ms}
    assert set(names) == registered
    by_name = {s.name: s for s in specs}
    for kind, source, ms in _slices():
        for r in render_metrics(ms):
            spec = by_name[r.name]
            assert len(spec.params) == len(r.columns), r.name
            assert (kind, source) in spec.members
    assert len(by_name["walk_rate"].members) == 2


def test_macro_specs_reject_conflicting_bodies_under_one_name() -> None:
    offense = Metric(
        name="rate",
        kind="offense",
        source="season",
        numerator=lambda t: t.a.sum(),
        denominator=lambda t: t.b.sum(),
    )
    pitching = Metric(
        name="rate",
        kind="pitching",
        source="season",
        numerator=lambda t: t.a.sum() * 9,
        denominator=lambda t: t.b.sum(),
    )
    with pytest.raises(ValueError, match="different definitions"):
        macro_specs(
            [("offense", "season", [offense]), ("pitching", "season", [pitching])]
        )


@pytest.mark.parametrize("kind,source", [(k, s) for k in KINDS for s in SOURCES])
def test_zero_denominators_render_null_never_inf_or_nan(
    kind: MetricKind, source: MetricSource
) -> None:
    metrics = metrics_for(kind, source)
    rendered = render_metrics(metrics)
    columns = sorted({c for r in rendered for c in r.columns})
    frame = _random_frame(columns)
    frame.loc[frame["grp"] == 0, columns] = 0
    con = ibis.duckdb.connect()
    _ = con.create_table("t", frame)
    select = ", ".join(f"{r.expanded} AS {r.name}" for r in rendered)
    rows = con.con.execute(
        f"SELECT grp, {select} FROM t GROUP BY grp ORDER BY grp"
    ).fetchall()
    for row in rows:
        for r, value in zip(rendered, row[1:], strict=True):
            if value is not None:
                assert math.isfinite(float(value)), (
                    f"{kind}/{source} {r.name} grp={row[0]}: {value}"
                )
    zero_row = rows[0]
    for r, value in zip(rendered, zero_row[1:], strict=True):
        if "nullif(" in r.expanded:
            assert value is None, f"{kind}/{source} {r.name} on all-zero group: {value}"


def test_comparisons_render_and_truthiness_is_rejected() -> None:
    flagged = Metric(
        name="f",
        kind="offense",
        source="event",
        formula=lambda t: (t.x == 1).sum() + (t.y != 0).sum(),
    )
    (out,) = render_metrics([flagged])
    assert out.expanded == (
        "sum(case when x = 1 then 1 else 0 end) + sum(case when y <> 0 then 1 else 0 end)"
    )
    branching = Metric(
        name="b",
        kind="offense",
        source="event",
        formula=lambda t: t.x.sum() if t.x == 1 else t.y.sum(),
    )
    with pytest.raises(TypeError, match="no truth value"):
        _ = render_metrics([branching])
