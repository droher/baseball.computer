"""Smoke + row-equivalence tests for the BSL semantic tables.

Runs only under the ``spikes-bsl`` uv group (the migration env can't
install boring-semantic-layer because xorq pins an incompatible
sqlglot). pytest collects-and-skips cleanly under the migration env via
``importorskip``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

bsl = pytest.importorskip("boring_semantic_layer")
import ibis  # noqa: E402

from semantic import (  # noqa: E402
    connect,
    offense_events,
    offense_seasons,
    pitching_events,
)

DB_PATH = Path(__file__).resolve().parents[2] / "bc.db"


@pytest.fixture(scope="module")
def con():
    if not DB_PATH.exists():
        pytest.skip(f"bc.db not built: {DB_PATH}")
    return connect("dev", DB_PATH)


@pytest.fixture(scope="module")
def offense(con):
    return offense_seasons(con, env="dev")


@pytest.fixture(scope="module")
def offense_event_table(con):
    return offense_events(con, env="dev")


@pytest.fixture(scope="module")
def pitching_event_table(con):
    return pitching_events(con, env="dev")


def test_dimensions_and_calc_classification(offense):
    """BSL must classify ratio + composite metrics as [calc] and pure
    aggregations as [base]. This is the introspection-graph invariant
    the LLM/MCP tooling relies on.

    BSL flags any BinOp it sees during introspection as calc — that
    includes our ``numerator/denominator`` Metric form (e.g. OBP, SLG,
    BA), since those resolve to a division. Only pure aggregations
    (e.g. ``SUM(col)`` formulas) and the secondary_average-style
    aggregate-of-arithmetic ratios stay base.
    """
    base = set(offense.get_measures())
    calc = set(offense.get_calculated_measures())

    # Sanity overlap check: no measure can be both.
    assert not (base & calc), f"measures both base+calc: {base & calc}"

    # Derived (Metric.derived) and ratio-shaped metrics show up as calc.
    for name in (
        "on_base_plus_slugging",
        "isolated_power",
        "on_base_percentage",
        "slugging_percentage",
        "batting_average",
    ):
        assert name in calc, f"{name!r} should be a calc measure"

    # Sanity: the union covers every metric we registered for this
    # (kind, source). Anything missing means BSL silently dropped a
    # measure, which would break LLM/MCP introspection downstream.
    from python_models.metrics.registry import metrics_for
    expected = {m.name for m in metrics_for("offense", "season")}
    actual = base | calc
    missing = expected - actual
    assert not missing, f"BSL dropped offense/season measures: {missing}"


@pytest.mark.parametrize(
    "fixture,name",
    [
        ("offense_event_table", "ground_air_out_ratio"),
        ("offense_event_table", "known_trajectory_out_hit_ratio"),
        ("offense_event_table", "coverage_weighted_air_ball_batting_average"),
        ("pitching_event_table", "ground_air_out_ratio"),
        ("pitching_event_table", "known_trajectory_out_hit_ratio"),
    ],
)
def test_event_derived_classified_as_calc(request, fixture, name):
    table = request.getfixturevalue(fixture)
    calc = set(table.get_calculated_measures())
    assert name in calc, f"{name!r} should be a calc measure on {fixture}"




def test_row_equivalence_top_50_2024_batters(con, offense):
    """OPS / OBP / SLG via BSL match the materialized
    ``metrics_player_season_league_offense`` row-for-row to 1e-9.
    """
    pa_threshold_table = con.table(
        "metrics_player_season_league_offense", database="main_models__dev"
    )
    top_50 = (
        pa_threshold_table.filter(pa_threshold_table.season == 2024)
        .order_by(ibis.desc("plate_appearances"))
        .limit(50)
        .select("player_id")
        .execute()
    )
    if top_50.empty:
        pytest.skip("no 2024 batters in dev — bc.db not backfilled?")

    player_ids = top_50["player_id"].tolist()

    bsl_df = (
        offense.query(
            dimensions=["player_id", "season", "league"],
            measures=["on_base_percentage", "slugging_percentage",
                      "on_base_plus_slugging"],
            filters=[ibis._.season == 2024,
                     ibis._.player_id.isin(player_ids)],
        )
        .execute()
        .sort_values(["player_id", "league"])
        .reset_index(drop=True)
    )

    ref = (
        pa_threshold_table.filter(
            (pa_threshold_table.season == 2024)
            & pa_threshold_table.player_id.isin(player_ids)
        )
        .select(
            "player_id", "season", "league",
            "on_base_percentage", "slugging_percentage",
            "on_base_plus_slugging",
        )
        .execute()
        .sort_values(["player_id", "league"])
        .reset_index(drop=True)
    )

    assert len(bsl_df) == len(ref), (
        f"row count mismatch: BSL={len(bsl_df)} vs ref={len(ref)}"
    )
    for col in ("on_base_percentage", "slugging_percentage",
                "on_base_plus_slugging"):
        diffs = (bsl_df[col] - ref[col]).abs()
        max_diff = float(diffs.max())
        assert max_diff < 1e-9, (
            f"{col}: max abs diff {max_diff} > 1e-9 across {len(bsl_df)} rows"
        )
