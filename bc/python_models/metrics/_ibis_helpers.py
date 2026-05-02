"""Small Ibis helpers for metric lambdas.

These keep the registration site readable when an inlined expression
gets long (e.g., the coverage-weighted batting averages).
"""

from __future__ import annotations

from typing import Any

TableExpr = Any
IbisExpr = Any


def known_trajectory_out_hit_ratio(t: TableExpr) -> IbisExpr:
    """SUM(trajectory_known * balls_batted * (at_bats - hits)) / SUM(balls_batted * (at_bats - hits))
    divided by
    SUM(trajectory_known * balls_batted * hits) / SUM(balls_batted * hits)
    """
    return (
        (t.trajectory_known * t.balls_batted * (t.at_bats - t.hits)).sum()
        / (t.balls_batted * (t.at_bats - t.hits)).sum()
    ) / (
        (t.trajectory_known * t.balls_batted * t.hits).sum()
        / (t.balls_batted * t.hits).sum()
    )


def known_trajectory_broad_out_hit_ratio(t: TableExpr) -> IbisExpr:
    return (
        (t.trajectory_broad_known * t.balls_batted * (t.at_bats - t.hits)).sum()
        / (t.balls_batted * (t.at_bats - t.hits)).sum()
    ) / (
        (t.trajectory_broad_known * t.balls_batted * t.hits).sum()
        / (t.balls_batted * t.hits).sum()
    )


def known_angle_out_hit_ratio(t: TableExpr) -> IbisExpr:
    return (
        (t.batted_angle_known * (t.at_bats - t.hits)).sum()
        / (t.balls_batted * (t.at_bats - t.hits)).sum()
    ) / ((t.batted_angle_known * t.hits).sum() / t.hits.sum())


def coverage_weighted_ba(t: TableExpr, x_col: str, ratio_fn) -> IbisExpr:
    """coverage_weighted_<x>_batting_average pattern:

        SUM(x * hits) * R / (SUM(x * hits) * R + SUM(x * (at_bats - hits)))

    where R is the relevant out_hit_ratio (trajectory / broad / angle).
    """
    x = t[x_col]
    r = ratio_fn(t)
    hits_term = (x * t.hits).sum() * r
    outs_term = (x * (t.at_bats - t.hits)).sum()
    return hits_term / (hits_term + outs_term)
