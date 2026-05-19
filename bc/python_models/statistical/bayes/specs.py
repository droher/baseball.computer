"""Per-Bayes-target metadata: dataset filter, DL covariate sourcing, default flavors."""

from __future__ import annotations

from typing import Callable, ClassVar

from pydantic import BaseModel, ConfigDict, Field

from python_models.statistical.schemas import GammaDlFlavor


class BayesTargetSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    name: str = Field(description="Bayes model name (e.g. 'trajectory_observedness').")
    dimension: str = Field(
        description=(
            "Conceptual dimension stamped into BayesArtifactExtras.dimension. "
            "Usually matches the dataset-row dimension; for broad_contact it "
            "diverges from dataset_dimension_filter."
        )
    )
    dataset_name: str = Field(
        description="Modeling-dataset view name. All Model A targets share model_input_observation_batted_ball."
    )
    dataset_dimension_filter: str = Field(
        description=(
            "Value of the dataset's `dimension` column used to filter training rows. "
            "broad_contact reuses 'trajectory' rows because the broad class is "
            "derived from trajectory upstream."
        )
    )
    builder: Callable[..., object] = Field(
        description=(
            "PyMC model factory. Called as builder(inputs, priors=, "
            "gamma_dl_flavor=, dimension=). Returns pm.Model."
        )
    )
    dl_proposal_dimension: str | None = Field(
        default=None,
        description=(
            "Published DL manifest name (e.g. 'trajectory') whose probabilities.parquet "
            "supplies the gamma_dl_shrunk covariate. None forces gamma_dl_zero-only."
        ),
    )
    dl_class_collapse_positive: tuple[str, ...] = Field(
        default=(),
        description=(
            "When non-empty, sum the DL probabilities of these class labels "
            "(from the source class_labels.json) and take logit(p_sum) as the "
            "covariate. Used by broad_contact to collapse trajectory classes "
            "into AirBall vs GroundBall. Empty means use logit(max(dl_p_class))."
        ),
    )
    default_flavors: tuple[GammaDlFlavor, ...] = Field(
        default=("gamma_dl_zero", "gamma_dl_shrunk"),
        description=(
            "Flavors expected to be fit for this target. A target with "
            "dl_proposal_dimension=None must restrict default_flavors to ('gamma_dl_zero',)."
        ),
    )
