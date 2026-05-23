"""Per-Bayes-target metadata: dataset filter, prep entrypoint."""

from __future__ import annotations

from typing import Callable, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

OutcomeKind = Literal["bernoulli", "multinomial"]


class BayesTargetSpec(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        frozen=True, arbitrary_types_allowed=True
    )

    name: str = Field(description="Bayes model name (e.g. 'trajectory_observedness').")
    dimension: str = Field(
        description=(
            "Conceptual dimension stamped into BayesArtifactExtras.dimension. "
            "v1 always matches dataset_dimension_filter."
        )
    )
    dataset_name: str = Field(
        description="Modeling-dataset view name (e.g. model_input_observation_batted_ball)."
    )
    dataset_dimension_filter: str = Field(
        description="Value of the dataset's `dimension` column used to filter training rows."
    )
    prep_fn: Callable[..., object] = Field(
        description=(
            "Event-grain prep entrypoint. Called as prep_fn(parquet_path, "
            "dimension=spec.dataset_dimension_filter, smoke_limit=, seed=)."
        )
    )
    builder: Callable[..., object] = Field(
        description=("PyMC model factory. Called as builder(inputs, priors=...).")
    )
    sample_size: int | None = Field(
        default=None,
        description=(
            "Production row budget for this target's prep step. None means "
            "use the full filtered dataset. Explicit smoke_limit / env "
            "override / --smoke still take precedence in run_bayes_model."
        ),
    )
    outcome_kind: OutcomeKind = Field(
        default="bernoulli",
        description=(
            "Selects the posterior export and likelihood branch in "
            "run_bayes_model. 'bernoulli' writes event_propensity.parquet "
            "with p_observed_mean; 'multinomial' writes "
            "event_credit.parquet with per-position expected_share."
        ),
    )

    def published_manifest_name(self) -> str:
        return self.name
