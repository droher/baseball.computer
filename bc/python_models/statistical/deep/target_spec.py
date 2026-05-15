"""Per-deep-target metadata: what to fit, on which dataset, how to calibrate."""

from __future__ import annotations

from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

DeepTargetKind = Literal["multiclass", "binary"]
ClassUniverseSource = Literal["train_distinct", "configured"]
CalibrationMethod = Literal["temperature", "isotonic"]


class DeepTargetSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    dataset_name: str
    target_column: str
    weight_column: str
    kind: DeepTargetKind
    class_universe_source: ClassUniverseSource = "train_distinct"
    configured_class_labels: tuple[str, ...] = ()
    calibration_method: CalibrationMethod = "temperature"
    fold_count: int = 5
    slice_columns: tuple[str, ...] = ()
    game_id_column: str = "game_id"
    split_column: str = "split_partition"
    train_label: str = "TRAIN"
    validate_label: str = "VALIDATE"
    test_label: str = "TEST"
    filter_predicate: str | None = Field(
        default=None,
        description=(
            "Optional Polars-expression-compatible SQL predicate applied "
            "to the dataset frame before fitting. Used by per-dimension "
            "Geometry specs to filter geometry_dimension == 'trajectory' etc."
        ),
    )

    def published_manifest_name(self) -> str:
        return f"dl_proposal_{self.name}"
