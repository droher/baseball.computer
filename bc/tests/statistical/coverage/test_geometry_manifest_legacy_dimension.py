from __future__ import annotations

import polars as pl

from python_models.statistical.bayes.manifest_ingest import (
    geometry_dimension_for_export,
)


def _export_frame(labels: list[str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "event_key": [1] * len(labels),
            "class_index": list(range(len(labels))),
            "class_label": labels,
            "expected_share": [1.0 / len(labels)] * len(labels),
        }
    )


def test_legacy_location_side_angle_vocabulary_is_exposed_as_angle() -> None:
    legacy = _export_frame(["Default", "Foul", "FoulLine", "Left", "Middle", "Right"])
    assert geometry_dimension_for_export(legacy, "location_side") == "location_angle"


def test_current_global_location_side_vocabulary_remains_side() -> None:
    current = _export_frame(["All", "Left", "Center", "Right"])
    assert geometry_dimension_for_export(current, "location_side") == "location_side"


def test_angle_vocabulary_does_not_rename_another_dimension() -> None:
    labels = _export_frame(["Default", "Foul", "FoulLine", "Left", "Middle", "Right"])
    assert geometry_dimension_for_export(labels, "location_angle") == "location_angle"
