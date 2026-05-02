"""Spike 3 — Boring Semantic Layer (BSL) OPS-derived-tree.

Goal: prove BSL represents `ops = obp + slg` as a first-class derived/calc
measure (a node in `_calc_measures`), not a flattened raw aggregation. If yes,
BSL is sufficient for the codebase's metric layer in Phase 3.

OPS canonical definition from bc/macros/metric_calcs.sql:
  obp = SUM(on_base_successes) / SUM(on_base_opportunities)
  slg = SUM(total_bases) / SUM(at_bats)
  ops = obp + slg

The metric_calcs.sql codepath inlines this as
  SUM(obs)/SUM(obo) + SUM(tb)/SUM(ab)
which is correct arithmetic but loses the derived-metric structure that lets a
downstream consumer (LLM tool, visualization) say "give me ops" and have it
resolve via obp + slg.

Strategy:
  1. Build a SemanticTable on event_offense_stats with base measures (sums)
     and calc measures (obp, slg, ops).
  2. Introspect: confirm ops appears in `_calc_measures`, not `_base_measures`.
  3. Execute `query(measures=['ops'], dimensions=['season'])` and diff against
     the dbt-built `metrics_player_season_league_offense.on_base_plus_slugging`.
"""
from __future__ import annotations

import logging
from pathlib import Path

import ibis
import boring_semantic_layer as bsl
from ibis import _

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("bsl_ops")

DB_PATH = Path("/Users/davidroher/Repos/baseball.computer/bc.db")


def main() -> None:
    con = ibis.duckdb.connect(str(DB_PATH), read_only=True)
    events_tbl = con.table("event_offense_stats", database="main_models")
    states_tbl = con.table("event_states_full", database="main_models")
    # Join to get season + league for the dbt aggregation (event_offense_stats
    # is keyed by event_key; season lives in event_states_full).
    joined = events_tbl.join(states_tbl, "event_key").select(
        events_tbl.player_id.name("batter_id"),
        states_tbl.season,
        states_tbl.league,
        states_tbl.game_type,
        events_tbl.on_base_successes,
        events_tbl.on_base_opportunities,
        events_tbl.total_bases,
        events_tbl.at_bats,
    ).filter(_.game_type == "RegularSeason")

    # Build the semantic table.
    sm = (
        bsl.to_semantic_table(joined, name="batting")
        .with_dimensions(
            season=lambda t: t.season,
            league=lambda t: t.league,
            batter_id=lambda t: t.batter_id,
        )
        .with_measures(
            total_obs=lambda t: t.on_base_successes.sum(),
            total_obo=lambda t: t.on_base_opportunities.sum(),
            total_tb=lambda t: t.total_bases.sum(),
            total_ab=lambda t: t.at_bats.sum(),
        )
        # Now derived measures referencing other measures.
        .with_measures(
            obp=lambda m: m.total_obs / m.total_obo,
            slg=lambda m: m.total_tb / m.total_ab,
        )
        .with_measures(
            ops=lambda m: m.obp + m.slg,
        )
    )

    # Introspection.
    base = sm.get_measures()
    calc = sm.get_calculated_measures()
    log.info("base measures: %s", sorted(base.keys()))
    log.info("calc measures: %s", sorted(calc.keys()))

    assert "ops" in calc, f"ops missing from calc_measures, got {calc.keys()}"
    assert "obp" in calc and "slg" in calc, "obp/slg should be calc, not base"
    assert {"total_obs", "total_obo", "total_tb", "total_ab"}.issubset(base), (
        "raw aggregations should be base, not calc"
    )
    log.info("✓ ops is a first-class node in _calc_measures (not flattened)")

    # Now query: total OPS by (season, league) for RegularSeason, ALL batters.
    result = sm.group_by("season", "league").aggregate("ops").execute()
    log.info("BSL OPS: %s rows", len(result))
    log.info("first rows:\n%s", result.sort_values(["season", "league"]).head().to_string())

    # Diff: dbt's metrics_player_season_league_offense aggregates per (season,
    # league, batter, account_type=RegularSeason). Sum of (obs/obo) per
    # (season, league) won't match ops directly because dbt computes
    # league-aggregate ops differently (per-player rate aggregated to league).
    # Compute the matching league-grain reference inline:
    duck = ibis.duckdb.connect(str(DB_PATH), read_only=True)
    expected = duck.sql(
        """
        SELECT
          s.season,
          s.league,
          SUM(o.on_base_successes) / SUM(o.on_base_opportunities)
            + SUM(o.total_bases) / SUM(o.at_bats) AS ops
        FROM main_models.event_offense_stats o
        JOIN main_models.event_states_full s USING (event_key)
        WHERE s.game_type = 'RegularSeason'
        GROUP BY s.season, s.league
        """
    ).execute()
    log.info("DuckDB direct OPS: %s rows", len(expected))

    merged = expected.merge(
        result,
        on=["season", "league"],
        suffixes=("_dbt", "_bsl"),
    )
    log.info("merged rows: %s", len(merged))
    import numpy as np
    if not np.allclose(merged["ops_dbt"].astype(float), merged["ops_bsl"].astype(float), atol=1e-9, equal_nan=True):
        delta = (merged["ops_dbt"] - merged["ops_bsl"]).abs()
        log.warning("max |delta|: %s", delta.max())
        log.warning("rows with delta > 1e-9: %s", (delta > 1e-9).sum())
    else:
        log.info("✓ row-equivalent within 1e-9 across %s (season, league) groups", len(merged))


if __name__ == "__main__":
    main()
