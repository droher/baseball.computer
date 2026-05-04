"""Spike 2 — Port `calc_park_factors_advanced` to Ibis.

The dbt model (bc/models/intermediate/park_factors/calc_park_factors_advanced.sql)
is the codebase's hardest expression: a 7-CTE pipeline with a 2-yr rolling
RANGE window, a synthetic-prior union-all (Bayesian shrinkage), a self-join
across parks, and an odds-based weighted average.

If Ibis can express all of it, Ibis is sufficient for everything in Phase 2.

Strategy:
  1. Re-implement the dbt SQL in Ibis windowed expressions.
  2. Compile to DuckDB SQL via `.compile()` — verbosity comparison.
  3. Execute against bc.db, restrict to one season slice, diff vs the dbt-built
     `bc.main_models.calc_park_factors_advanced` table. Tolerance 1e-9.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

import ibis
import ibis.expr.types as ir
from ibis import _

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("park_factors_ibis")

DB_PATH = Path("/Users/davidroher/Repos/baseball.computer/bc.db")
SCHEMA = "main_models"

STATS = [
    "plate_appearances", "singles", "doubles", "triples",
    "home_runs", "strikeouts", "walks", "batting_outs", "runs", "balls_in_play",
    "trajectory_fly_ball", "trajectory_ground_ball", "trajectory_line_drive",
    "trajectory_pop_up", "trajectory_unknown",
    "batted_distance_infield", "batted_distance_outfield", "batted_distance_unknown",
    "batted_angle_left", "batted_angle_right", "batted_angle_middle",
]
RATE_STATS = STATS[1:]
PRIOR_SAMPLE_SIZE = 1000
PREV_YEARS = 2


def park_factors_ibis(con: ibis.BaseBackend) -> ir.Table:
    states = con.table("event_states_full", database=SCHEMA)
    offense = con.table("event_offense_stats", database=SCHEMA)
    game_start = con.table("game_start_info", database=SCHEMA)

    # 1. unique_park_seasons (parks with >25 RegularSeason home games)
    unique_park_seasons = (
        game_start.filter(_.game_type == "RegularSeason")
        .group_by(["park_id", "season", _.home_league.name("league")])
        .aggregate(games=_.count())
        .filter(_.games > 25)
        .select("park_id", "season", "league")
    )

    # 2. batting_agg — sum stats by (park, season, league, batter, pitcher)
    joined = (
        states.filter(
            (_.game_type == "RegularSeason") & (~_.is_interleague)
        )
        .join(offense, "event_key")
        .join(unique_park_seasons, ["park_id", "season", "league"])
    )
    sum_aggs = {s: getattr(joined, s).sum().cast("int32").name(s) for s in STATS}
    batting_agg = joined.group_by(
        ["park_id", "season", "league", "batter_id", "pitcher_id"]
    ).aggregate(**sum_aggs)

    # 3. multi_year_range — 2-yr trailing window per (park, batter, pitcher, league)
    w = ibis.window(
        group_by=["park_id", "batter_id", "pitcher_id", "league"],
        order_by="season",
        preceding=PREV_YEARS,
        following=0,
    )
    rolled_cols = {
        s: getattr(batting_agg, s).sum().over(w).cast("int32").name(s)
        for s in STATS
    }
    multi_year_range = batting_agg.select(
        "park_id", "season", "league", "batter_id", "pitcher_id", **rolled_cols
    )

    # 4. averages — league-wide rate per stat per (season, league)
    avg_aggs = {
        f"avg_{s}_per_pa": (
            getattr(multi_year_range, s).sum() / multi_year_range.plate_appearances.sum()
        ).name(f"avg_{s}_per_pa")
        for s in RATE_STATS
    }
    averages = multi_year_range.group_by(["season", "league"]).aggregate(**avg_aggs)

    # 5. with_priors — UNION ALL multi_year_range with synthetic priors
    # Cast multi_year_range stats to float64 to preserve dbt's UNION ALL semantics
    # (dbt's INT batting_agg + DOUBLE prior stats coerces to DOUBLE; mirror that).
    myr_float = multi_year_range.select(
        "park_id", "season", "league", "batter_id", "pitcher_id",
        *[getattr(multi_year_range, s).cast("float64").name(s) for s in STATS],
    )
    priors = averages.join(unique_park_seasons, ["season", "league"]).select(
        unique_park_seasons.park_id,
        averages.season,
        averages.league,
        ibis.literal("MARK").name("batter_id"),
        ibis.literal("PRIOR").name("pitcher_id"),
        ibis.literal(PRIOR_SAMPLE_SIZE).cast("float64").name("plate_appearances"),
        *[
            (getattr(averages, f"avg_{s}_per_pa") * PRIOR_SAMPLE_SIZE)
            .cast("float64")
            .name(s)
            for s in RATE_STATS
        ],
    )
    with_priors = myr_float.union(priors)

    # 6. self_joined — every (this_park, other_park) pair within (season, batter, pitcher)
    this = with_priors.view()
    other = with_priors.view()
    paired = this.join(
        other,
        (this.park_id != other.park_id)
        & (this.season == other.season)
        & (this.batter_id == other.batter_id)
        & (this.pitcher_id == other.pitcher_id),
    )
    select_cols = {
        "this_park_id": this.park_id,
        "other_park_id": other.park_id,
        "season": this.season,
        "league": this.league,
        "batter_id": this.batter_id,
        "pitcher_id": this.pitcher_id,
    }
    for s in STATS:
        select_cols[f"this_{s}"] = getattr(this, s)
        select_cols[f"other_{s}"] = getattr(other, s)
    self_joined = paired.select(**select_cols)

    # sample_size = sqrt(min(this_pa, other_pa))
    self_joined = self_joined.mutate(
        sample_size=ibis.least(
            self_joined.this_plate_appearances, self_joined.other_plate_appearances
        ).sqrt()
    )
    pair_window = ibis.window(
        group_by=["this_park_id", "other_park_id", "season", "league"]
    )
    self_joined = self_joined.mutate(
        sum_sample_size=self_joined.sample_size.sum().over(pair_window)
    )

    # 7. rate_calculation — per-stat per-pa rates + sample weight
    rate_cols = {}
    for s in RATE_STATS:
        rate_cols[f"this_{s}_per_pa"] = (
            getattr(self_joined, f"this_{s}") / self_joined.this_plate_appearances
        )
        rate_cols[f"other_{s}_per_pa"] = (
            getattr(self_joined, f"other_{s}") / self_joined.other_plate_appearances
        )
    rates = self_joined.mutate(**rate_cols)
    park_window = ibis.window(group_by=["this_park_id", "season", "league"])
    rates = rates.mutate(scaling_factor=rates.sum_sample_size.max().over(park_window))
    rates = rates.mutate(
        sample_weight=rates.sample_size * (rates.scaling_factor / rates.sum_sample_size)
    )

    # 8. weighted_average — odds-based per-park park factor
    sw = rates.sample_weight
    agg_cols = {"sqrt_sample_size": rates.sample_size.sum()}
    for s in RATE_STATS:
        this_per_pa = getattr(rates, f"this_{s}_per_pa")
        other_per_pa = getattr(rates, f"other_{s}_per_pa")
        agg_cols[f"avg_this_{s}_per_pa"] = (this_per_pa * sw).sum() / sw.sum()
        agg_cols[f"avg_other_{s}_per_pa"] = (other_per_pa * sw).sum() / sw.sum()
    weighted = rates.group_by(
        [rates.this_park_id.name("park_id"), "season", "league"]
    ).aggregate(**agg_cols)

    # 9. final — round + odds ratio
    final_cols = {
        "park_id": weighted.park_id,
        "season": weighted.season,
        "league": weighted.league,
        "sqrt_sample_size": weighted.sqrt_sample_size.round(0),
    }
    for s in RATE_STATS:
        avg_this = getattr(weighted, f"avg_this_{s}_per_pa")
        avg_other = getattr(weighted, f"avg_other_{s}_per_pa")
        this_odds = avg_this / (1 - avg_this)
        other_odds = avg_other / (1 - avg_other)
        final_cols[f"{s}_park_factor"] = (this_odds / other_odds).round(2)
    return weighted.select(**final_cols)


def diff(con: ibis.BaseBackend, season: int) -> None:
    expected = (
        con.table("calc_park_factors_advanced", database=SCHEMA)
        .filter(_.season == season)
        .order_by(["park_id", "league"])
        .execute()
    )
    log.info("dbt expected rows: %s", len(expected))

    ibis_tbl = park_factors_ibis(con)
    actual = (
        ibis_tbl.filter(_.season == season)
        .order_by(["park_id", "league"])
        .execute()
    )
    log.info("ibis actual rows: %s", len(actual))

    # Align column order; expected may have ROUND-2 floats, so allclose at 1e-2.
    common = [c for c in expected.columns if c in actual.columns]
    log.info("common columns: %s", len(common))
    assert set(expected.columns) == set(actual.columns), (
        "column mismatch", set(expected.columns) ^ set(actual.columns),
    )
    e = expected[common].reset_index(drop=True)
    a = actual[common].reset_index(drop=True)
    assert (e["park_id"] == a["park_id"]).all(), "key alignment"
    assert (e["league"] == a["league"]).all(), "key alignment"

    import numpy as np

    for c in common:
        if e[c].dtype == object:
            mismatch = (e[c] != a[c]).sum()
        else:
            mismatch = (~np.isclose(e[c].astype(float), a[c].astype(float), atol=1e-2, equal_nan=True)).sum()
        if mismatch:
            log.warning("col %s: %s mismatched", c, mismatch)
        else:
            log.info("col %s: ok", c)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, default=1986)
    p.add_argument("--print-sql", action="store_true")
    args = p.parse_args()

    con = ibis.duckdb.connect(str(DB_PATH), read_only=True)

    if args.print_sql:
        sql = ibis.to_sql(park_factors_ibis(con), dialect="duckdb")
        log.info("compiled SQL:\n%s", sql)
        with open(Path(__file__).parent / "compiled.sql", "w") as f:
            f.write(str(sql))

    diff(con, args.season)


if __name__ == "__main__":
    main()
