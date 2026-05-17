"""Pydantic specs for shared entity-embedding pretraining."""

from __future__ import annotations

from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

PretrainHeadKind = Literal["multiclass", "binary"]
HeadClassUniverseSource = Literal["train_distinct", "configured"]


class HeadSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    target_column: str
    kind: PretrainHeadKind
    configured_class_labels: tuple[str, ...] = ()
    class_universe_source: HeadClassUniverseSource = "train_distinct"
    target_remap: tuple[tuple[str, str], ...] = Field(
        default=(),
        description=(
            "Pre-fit mapping applied to this head's target column before "
            "class-index encoding. Same semantics as DeepTargetSpec.target_remap."
        ),
    )
    loss_weight: float = 1.0


class PretrainSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    dataset_name: str
    head_specs: tuple[HeadSpec, ...]
    weight_column: str = "training_weight"
    split_column: str = "primary_fold"
    train_label: str = "TRAIN"
    validate_label: str = "VALIDATE"
    test_label: str = "TEST"
    game_id_column: str = "game_id"
    row_filter_predicate: str | None = Field(
        default=None,
        description=(
            "Optional polars-expression predicate (eval'd via pl.SQLContext) "
            "applied to the dataset frame before splitting. Use to restrict "
            "rows to those relevant to the spec's heads (e.g. batted-ball-only)."
        ),
    )

    def head_by_name(self, name: str) -> HeadSpec:
        for head in self.head_specs:
            if head.name == name:
                return head
        raise KeyError(f"unknown head {name!r}; have {[h.name for h in self.head_specs]}")
