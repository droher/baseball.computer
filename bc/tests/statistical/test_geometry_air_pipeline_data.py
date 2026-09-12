from __future__ import annotations

import polars as pl

from python_models.statistical.backtests.geometry_air_pipeline_data import (
    CLUE_LEVELS,
    PIPELINES,
    clue_label,
    pipeline_of,
    with_clues,
)


def test_with_clues_matches_the_scalar_definition_on_every_combination() -> None:
    results = ["hit", "out_in_play", "sacrifice", "fielders_choice", None]
    fielders = [None, 0, 1, 3, 6, 7, 9, 10]
    depths = [None, "Shallow", "Default", "Deep", "ExtraDeep", "Unknown"]
    rows = [
        (result, fielder, depth)
        for result in results
        for fielder in fielders
        for depth in depths
    ]
    frame = pl.DataFrame(
        {
            "result_family": [row[0] for row in rows],
            "batted_to_fielder": pl.Series([row[1] for row in rows], dtype=pl.Int32),
            "depth": [row[2] for row in rows],
        }
    )
    clues = with_clues(frame)["clue"].to_list()
    assert clues == [clue_label(*row) for row in rows]
    assert set(clues) <= set(CLUE_LEVELS)


def test_pipeline_of_covers_every_declared_season_once() -> None:
    for name, (first, last) in PIPELINES.items():
        assert pipeline_of(first) == name and pipeline_of(min(last, 2100)) == name
    assert pipeline_of(2008) == "A" and pipeline_of(2009) == "B"
    assert pipeline_of(2019) == "B" and pipeline_of(2020) == "C"
