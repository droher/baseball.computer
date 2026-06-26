"""Per-Bayes-target metadata: dataset filter, prep entrypoint."""

from __future__ import annotations

from typing import Callable, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

OutcomeKind = Literal["bernoulli", "multinomial", "count"]
GammaDlFlavor = Literal["gamma_dl_zero", "gamma_dl_shrunk"]
GammaPropensityFlavor = Literal[
    "gamma_propensity_zero", "gamma_propensity_class", "gamma_propensity_offset"
]


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
            "event_credit.parquet with per-position expected_share; "
            "'count' fits a NegativeBinomial and writes park_factor_posterior "
            "and park_factor_summary at parameter grain."
        ),
    )
    multinomial_export: (
        Literal[
            "credit",
            "ball_handler",
            "geometry",
            "pitch_summary",
            "advancement",
        ]
        | None
    ) = Field(
        default=None,
        description=(
            "Routes the multinomial export branch in run_bayes_model. "
            "'credit' writes event_credit.parquet (with putout "
            "marginalization for the assist target); 'ball_handler' writes "
            "ball_handler_probabilities.parquet with a plain per-event "
            "softmax; 'geometry' writes geometry_probabilities.parquet with a "
            "per-event softmax and the flavor-gated DL covariate; "
            "'pitch_summary' fits a cell-grain count-vector Multinomial and "
            "writes pitch_summary_posterior.parquet and "
            "pitch_summary_summary.parquet at (result_family, season, league, "
            "final-count class) parameter grain; 'advancement' writes "
            "advancement_probabilities.parquet with a per-(event, baserunner) "
            "softmax over the 7 advancement classes. None for bernoulli targets."
        ),
    )
    count_export: Literal["park_factor", "run_expectancy"] | None = Field(
        default=None,
        description=(
            "Routes the count export branch in run_bayes_model. "
            "'park_factor' writes park_factor_posterior.parquet and "
            "park_factor_summary.parquet at (park_id, season, league, "
            "outcome) parameter grain; 'run_expectancy' writes "
            "run_expectancy_posterior.parquet and run_expectancy_summary.parquet "
            "at (state, season, league) parameter grain. None for non-count "
            "targets."
        ),
    )
    dl_proposal_dimension: str | None = Field(
        default=None,
        description=(
            "Published DL manifest dimension whose per-event probabilities "
            "feed the gamma_dl_shrunk covariate. None means gamma_dl_zero-only."
        ),
    )
    dl_class_collapse_positive: tuple[str, ...] = Field(
        default=(),
        description=(
            "When non-empty, sum these class probabilities and take the logit "
            "of the sum. Empty means use logit(max p) per row."
        ),
    )
    default_flavors: tuple[GammaDlFlavor, ...] = Field(
        default=("gamma_dl_zero",),
        description=(
            "Flavors fit for this target. A target with dl_proposal_dimension=None "
            "must restrict default_flavors to ('gamma_dl_zero',)."
        ),
    )
    propensity_dimension: str | None = Field(
        default=None,
        description=(
            "Observation-propensity dimension whose standardized logit of "
            "propensity_p_observed feeds the gamma_propensity_class MNAR "
            "covariate. None means gamma_propensity_zero-only."
        ),
    )
    default_propensity_flavors: tuple[GammaPropensityFlavor, ...] = Field(
        default=("gamma_propensity_zero",),
        description=(
            "Propensity flavors fit for this target. The first entry is the "
            "default when run_bayes_model receives no explicit flavor. A "
            "target with propensity_dimension=None must restrict "
            "default_propensity_flavors to ('gamma_propensity_zero',)."
        ),
    )

    @model_validator(mode="after")
    def _shrunk_flavor_requires_dl_source(self) -> "BayesTargetSpec":
        if self.dl_proposal_dimension is None and self.default_flavors != (
            "gamma_dl_zero",
        ):
            raise ValueError(
                "default_flavors must be ('gamma_dl_zero',) when dl_proposal_dimension is None"
            )
        return self

    @model_validator(mode="after")
    def _class_flavor_requires_propensity_dimension(self) -> "BayesTargetSpec":
        if (
            self.propensity_dimension is None
            and "gamma_propensity_class" in self.default_propensity_flavors
        ):
            raise ValueError(
                "default_propensity_flavors must exclude 'gamma_propensity_class' "
                "when propensity_dimension is None"
            )
        return self

    @model_validator(mode="after")
    def _count_export_requires_count_kind(self) -> "BayesTargetSpec":
        if (self.count_export is not None) != (self.outcome_kind == "count"):
            raise ValueError("count_export must be set iff outcome_kind is 'count'")
        return self

    def published_manifest_name(self) -> str:
        return self.name
