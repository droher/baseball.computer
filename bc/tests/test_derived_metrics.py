"""Tests for ``Metric.derived`` composition and ``evaluate_all``.

These run against ad-hoc ``Metric`` instances passed straight to
``evaluate_all`` — no global registry mutation, so the tests don't
collide with the production METRICS dict.
"""

from __future__ import annotations

import ibis
import pytest

from python_models.metrics import _metric_registrations  # noqa: F401
from python_models.metrics.registry import (
    METRICS,
    Metric,
    evaluate_all,
    metrics_for,
)


def _memtable():
    """10-row synthetic batting line for algebraic checks."""
    rows = [
        # (at_bats, hits, total_bases, on_base_successes, on_base_opportunities,
        #  trajectory_known, balls_batted, trajectory_broad_known,
        #  trajectory_broad_air_ball, trajectory_broad_ground_ball)
        (4, 2, 5, 3, 5, 1, 4, 1, 1, 3),
        (3, 1, 1, 2, 4, 1, 2, 1, 2, 0),
        (5, 3, 7, 3, 5, 1, 5, 1, 3, 2),
        (4, 0, 0, 1, 4, 0, 3, 1, 0, 3),
        (4, 1, 2, 1, 4, 1, 4, 1, 2, 2),
        (3, 2, 4, 2, 3, 1, 3, 1, 1, 2),
        (5, 1, 4, 2, 5, 1, 4, 0, 0, 0),
        (4, 2, 3, 2, 4, 1, 4, 1, 1, 3),
        (3, 0, 0, 0, 3, 1, 2, 1, 0, 2),
        (4, 1, 1, 1, 4, 1, 3, 1, 1, 2),
    ]
    cols = [
        "at_bats", "hits", "total_bases", "on_base_successes",
        "on_base_opportunities", "trajectory_known", "balls_batted",
        "trajectory_broad_known", "trajectory_broad_air_ball",
        "trajectory_broad_ground_ball",
    ]
    return ibis.memtable(
        {c: [r[i] for r in rows] for i, c in enumerate(cols)},
    )


# -----------------------------------------------------------------------------
# Cycle / missing-dep diagnostics


def test_evaluate_all_detects_cycle():
    a = Metric(
        name="a", kind="offense", source="season",
        derived=lambda m: m.b + 1,
    )
    b = Metric(
        name="b", kind="offense", source="season",
        derived=lambda m: m.a + 1,
    )
    t = ibis.memtable({"x": [1]})
    with pytest.raises(ValueError, match="cycle"):
        evaluate_all(t, [a, b])


def test_evaluate_all_self_cycle_caught():
    a = Metric(
        name="a", kind="offense", source="season",
        derived=lambda m: m.a + 1,
    )
    t = ibis.memtable({"x": [1]})
    with pytest.raises(ValueError, match="cycle"):
        evaluate_all(t, [a])


def test_evaluate_all_missing_dep_raises():
    bogus = Metric(
        name="bogus", kind="offense", source="season",
        derived=lambda m: m.does_not_exist + 1,
    )
    t = ibis.memtable({"x": [1]})
    with pytest.raises(ValueError, match="unknown measure"):
        evaluate_all(t, [bogus])


def test_evaluate_all_preserves_registration_order():
    """Returned dict preserves the input list order, even when topo
    order would differ."""
    a = Metric(
        name="a", kind="offense", source="season",
        derived=lambda m: m.c,
    )
    b = Metric(
        name="b", kind="offense", source="season",
        formula=lambda t: t.x.sum(),
    )
    c = Metric(
        name="c", kind="offense", source="season",
        derived=lambda m: m.b + 1,
    )
    t = ibis.memtable({"x": [1, 2, 3]})
    out = evaluate_all(t, [a, b, c])
    assert list(out) == ["a", "b", "c"]


# -----------------------------------------------------------------------------
# Algebraic equivalence on a synthetic memtable


def _scalar(expr) -> float:
    """Evaluate a scalar Ibis expression to a Python float."""
    return float(expr.execute())


def test_ops_equals_obp_plus_slg():
    t = _memtable()
    # Pluck the registered offense metrics, eval against the memtable,
    # then verify ops == obp + slg arithmetically.
    metrics = {m.name: m for m in metrics_for("offense", "season")}
    out = evaluate_all(
        t, [metrics["on_base_percentage"], metrics["slugging_percentage"],
            metrics["on_base_plus_slugging"]],
    )
    obp = _scalar(out["on_base_percentage"])
    slg = _scalar(out["slugging_percentage"])
    ops = _scalar(out["on_base_plus_slugging"])
    assert ops == pytest.approx(obp + slg, abs=1e-12)


def test_isolated_power_equals_slg_minus_ba():
    t = _memtable()
    metrics = {m.name: m for m in metrics_for("offense", "season")}
    out = evaluate_all(
        t, [metrics["batting_average"], metrics["slugging_percentage"],
            metrics["isolated_power"]],
    )
    ba = _scalar(out["batting_average"])
    slg = _scalar(out["slugging_percentage"])
    iso = _scalar(out["isolated_power"])
    assert iso == pytest.approx(slg - ba, abs=1e-12)


def test_known_trajectory_out_hit_ratio_composes():
    t = _memtable()
    metrics = {m.name: m for m in metrics_for("offense", "event")}
    out = evaluate_all(
        t, [metrics["known_trajectory_rate_outs"],
            metrics["known_trajectory_rate_hits"],
            metrics["known_trajectory_out_hit_ratio"]],
    )
    outs = _scalar(out["known_trajectory_rate_outs"])
    hits = _scalar(out["known_trajectory_rate_hits"])
    ratio = _scalar(out["known_trajectory_out_hit_ratio"])
    assert ratio == pytest.approx(outs / hits, abs=1e-12)


def test_ground_air_out_ratio_composes():
    t = _memtable()
    metrics = {m.name: m for m in metrics_for("offense", "event")}
    out = evaluate_all(
        t, [metrics["air_ball_rate_outs"], metrics["ground_ball_rate_outs"],
            metrics["ground_air_out_ratio"]],
    )
    air = _scalar(out["air_ball_rate_outs"])
    ground = _scalar(out["ground_ball_rate_outs"])
    ratio = _scalar(out["ground_air_out_ratio"])
    assert ratio == pytest.approx(ground / air, abs=1e-12)


def test_coverage_weighted_air_ball_ba_matches_canonical_formula():
    """Derived rewrite must reproduce the original
    coverage_weighted_ba(t, x_col, ratio_fn) shape:
        SUM(x * hits) * R / (SUM(x * hits) * R + SUM(x * (at_bats - hits)))
    where R = known_trajectory_broad_out_hit_ratio.
    """
    t = _memtable()
    metrics = {m.name: m for m in metrics_for("offense", "event")}
    needed = [
        metrics["known_trajectory_broad_rate_outs"],
        metrics["known_trajectory_broad_rate_hits"],
        metrics["known_trajectory_broad_out_hit_ratio"],
        metrics["sum_trajectory_broad_air_ball_hits"],
        metrics["sum_trajectory_broad_air_ball_outs"],
        metrics["coverage_weighted_air_ball_batting_average"],
    ]
    out = evaluate_all(t, needed)
    sum_hits = _scalar(out["sum_trajectory_broad_air_ball_hits"])
    sum_outs = _scalar(out["sum_trajectory_broad_air_ball_outs"])
    r = _scalar(out["known_trajectory_broad_out_hit_ratio"])
    cw = _scalar(out["coverage_weighted_air_ball_batting_average"])
    expected = (sum_hits * r) / (sum_hits * r + sum_outs)
    assert cw == pytest.approx(expected, abs=1e-12)


# -----------------------------------------------------------------------------
# Production registry sanity


@pytest.mark.parametrize(
    "kind,source,name",
    [
        ("offense", "season", "on_base_plus_slugging"),
        ("offense", "season", "isolated_power"),
        ("pitching", "season", "on_base_plus_slugging_against"),
        ("offense", "event", "known_trajectory_out_hit_ratio"),
        ("offense", "event", "ground_air_out_ratio"),
        ("offense", "event", "ground_air_hit_ratio"),
        ("offense", "event", "known_angle_out_hit_ratio"),
        ("offense", "event", "coverage_weighted_air_ball_batting_average"),
        ("pitching", "event", "coverage_weighted_pulled_batting_average"),
    ],
)
def test_registered_as_derived(kind, source, name):
    m = METRICS[(name, kind)]
    assert m.source == source
    assert m.derived is not None, (
        f"{name} ({kind}) should be a derived metric after Stage B"
    )
    # Stage B converts ratio composites away from numerator/denominator;
    # the old shape should not linger or evaluate_all would route them
    # through the wrong branch.
    assert m.numerator is None and m.denominator is None
    assert m.formula is None


def test_dependencies_empty_for_non_derived():
    """Non-derived metrics must report zero deps so evaluate_all skips
    the topological pass for them."""
    for m in METRICS.values():
        if m.derived is None:
            assert m.dependencies() == set(), (
                f"{m.name} ({m.kind}): non-derived metric should have empty deps"
            )


def test_dep_capture_proxy_rejects_branching():
    """A derived lambda that branches on a measure value silently drops
    one branch's deps. The proxy must raise rather than return a bool."""
    bogus = Metric(
        name="branchy", kind="offense", source="season",
        derived=lambda m: m.a if m.cond else m.b,
    )
    with pytest.raises(ValueError, match="must not branch"):
        bogus.dependencies()


def test_dep_capture_proxy_ignores_dunders():
    """Dunder lookups (copy / deepcopy / repr) on the proxy must not
    pollute the dep set."""
    import copy as _copy

    sentinel = Metric(
        name="just_a", kind="offense", source="season",
        derived=lambda m: m.a + m.b,
    )
    deps = sentinel.dependencies()
    assert deps == {"a", "b"}, f"expected exact deps, got {deps}"
    # Probe stays sane under a stdlib introspector.
    from python_models.metrics.registry import _DepCaptureProxy
    sink: set[str] = set()
    p = _DepCaptureProxy(sink)
    _ = _copy.copy(p)  # would explode if __copy__ poisoned the sink
    assert "__copy__" not in sink and "__deepcopy__" not in sink
