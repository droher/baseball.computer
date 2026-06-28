"""Per-deep-target metadata: what to fit and on which dataset."""

from __future__ import annotations

from typing import ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

DeepTargetKind = Literal["multiclass", "binary"]
ClassUniverseSource = Literal["train_distinct", "configured"]
LossType = Literal["cross_entropy", "focal"]


class DeepTargetSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    dataset_name: str
    target_column: str
    weight_column: str
    kind: DeepTargetKind
    proposal_dimension: str = Field(
        description=(
            "Row-level value stamped into the dl_*_proposal_manifest "
            "'dimension' column. Matches the JOIN predicate in the "
            "corresponding model_input_* view (e.g. 'trajectory', "
            "'pitch_summary', 'park_factors'). Also used to derive "
            "published_manifest_name()."
        ),
    )
    class_universe_source: ClassUniverseSource = "train_distinct"
    configured_class_labels: tuple[str, ...] = ()
    target_remap: tuple[tuple[str, str], ...] = Field(
        default=(),
        description=(
            "Pre-fit mapping applied to the target column: each (src, dst) "
            "pair replaces rows where target == src with dst. Use to collapse "
            "low-signal classes into a parent class (e.g. all bunt variants "
            "to 'Bunt'). Applied before class-universe encoding, so dst "
            "values must appear in configured_class_labels (when configured)."
        ),
    )
    loss_type: LossType = "cross_entropy"
    focal_gamma: float = Field(
        default=2.0,
        description="Focusing parameter for focal loss; ignored when loss_type='cross_entropy'.",
    )
    fold_count: int = Field(default=5, ge=2)
    slice_columns: tuple[str, ...] = ()
    game_id_column: str = "game_id"
    split_column: str = "split_partition"
    train_label: str = "TRAIN"
    validate_label: str = "VALIDATE"
    test_label: str = "TEST"
    filter_predicate: str | None = Field(
        default=None,
        description=(
            "Optional SQL predicate (DuckDB-compatible) applied to the "
            "dataset frame before fitting. Used by per-dimension Geometry "
            "specs to filter geometry_dimension == 'trajectory' etc."
        ),
    )
    loss_mask_predicate: str | None = Field(
        default=None,
        description=(
            "Optional SQL predicate (DuckDB-compatible) evaluated per row "
            "against the post-filter dataset frame. When the predicate is "
            "false, the row's sample_weight is zeroed (held out of loss) but "
            "predictions are still emitted. Use to exclude rows whose target "
            "value is deterministically derivable from other dataset columns "
            "(e.g. observed_status = 'derived')."
        ),
    )
    pretrained_embeddings_artifact_id: str | None = Field(
        default=None,
        description=(
            "Optional pretrain artifact id OR pretrain name. When set, the "
            "fit_keras loop loads matching high-card Embedding rows from "
            "the pretrain artifact's embeddings.parquet / vocab.json "
            "before training. Resolution order: published pointer under "
            "BC_STATS_PUBLISHED_ROOT/pretrain/<name>.json (via "
            "find_published_pretrain), then fallback to rglob under "
            "DEEP_ROOT for <artifact_id>/manifest.json."
        ),
    )
    freeze_pretrained_embeddings: bool = Field(
        default=False,
        description=(
            "When True (and pretrained_embeddings_artifact_id is set), each "
            "embed_<col> layer is marked trainable=False post-load and the "
            "model is re-compiled so the freeze takes effect."
        ),
    )

    def published_manifest_name(self) -> str:
        return f"dl_proposal_{self.proposal_dimension}"

    def remap_dict(self) -> dict[str, str]:
        return dict(self.target_remap)
