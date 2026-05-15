"""Per-model fitting configuration consumed by deep and Bayesian fits.

``ModelConfig`` is the contract between an EDA report (which surfaces
weak-identification flags + blocking findings) and a published model
artifact. Each model declares how it handles each weak-identification
flag the EDA may raise; the publication gate (see
``publication.evaluate_publication_gate``) compares the EDA report
against the config and refuses to publish when an unaddressed flag is
present.

Treatments correspond to doc-02 §"Blocking Findings" and doc-03
§"Hierarchical priors". The semantics:

- ``partial_pool`` — the effect enters the hierarchy with a shared prior
  pulling estimates toward the population mean.
- ``fixed_prior`` — the effect is constrained by an informative prior
  rather than inferred from the data.
- ``drop`` — the effect is removed from the model formula.
- ``merge_levels`` — categorical levels are merged into a coarser
  grouping (e.g. era buckets) before fitting.
- ``mark_weakly_identified`` — the effect is fitted but rows or columns
  inheriting it must carry a ``weak_identification`` flag in the
  published outputs (doc-05 §"Tier policies").
- ``accept_unidentified`` — explicit opt-in: the user has decided this
  flag does not block publication. Use sparingly; documented in
  ``rationale``.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Literal

from pydantic import BaseModel, Field, field_validator

WeakIdentificationTreatment = Literal[
    "partial_pool",
    "fixed_prior",
    "drop",
    "merge_levels",
    "mark_weakly_identified",
    "accept_unidentified",
]


_TREATMENT_VALUES: frozenset[str] = frozenset(
    {
        "partial_pool",
        "fixed_prior",
        "drop",
        "merge_levels",
        "mark_weakly_identified",
        "accept_unidentified",
    }
)


class WeakIdentificationPolicy(BaseModel):
    """How a model handles a single weak-identification flag.

    A flag matches by ``effect`` (e.g. ``"scorer_park"``,
    ``"is_heldout_alignment_regime"``) and optional ``slice`` pattern.
    A ``slice`` of ``"*"`` matches every slice; otherwise the strings
    must match exactly. Multiple policies may cover the same effect
    with different slices.
    """

    effect: str
    slice: str = "*"
    treatment: WeakIdentificationTreatment
    rationale: str = Field(
        default="",
        description=(
            "Free-text justification. Required for ``accept_unidentified`` "
            "treatments so that audits can review why a flag was bypassed."
        ),
    )

    @field_validator("treatment")
    @classmethod
    def _validate_treatment(cls, value: str) -> str:
        if value not in _TREATMENT_VALUES:
            raise ValueError(f"treatment {value!r} not in {sorted(_TREATMENT_VALUES)}")
        return value


class ModelConfig(BaseModel):
    """Frozen per-fit contract for a Bayesian/deep model.

    The exporter serializes one of these next to every published
    artifact so SQLMesh ingestion and audits can read it without
    re-running the EDA pipeline.
    """

    model_name: str
    model_version: str
    dataset_name: str
    dataset_version: str
    addressed_weak_identifications: tuple[WeakIdentificationPolicy, ...] = ()
    expected_blocking_findings: tuple[str, ...] = Field(
        default=(),
        description=(
            "EDA ``BlockingFinding`` codes that the model owner has "
            "explicitly acknowledged. Findings outside this set fail the "
            "publication gate."
        ),
    )
    notes: str = ""

    @field_validator("addressed_weak_identifications")
    @classmethod
    def _validate_policies(
        cls, value: tuple[WeakIdentificationPolicy, ...]
    ) -> tuple[WeakIdentificationPolicy, ...]:
        seen: set[tuple[str, str]] = set()
        for policy in value:
            key = (policy.effect, policy.slice)
            if key in seen:
                raise ValueError(
                    "duplicate weak-identification policy for "
                    f"effect={policy.effect!r} slice={policy.slice!r}"
                )
            seen.add(key)
            if policy.treatment == "accept_unidentified" and not policy.rationale:
                raise ValueError(
                    f"accept_unidentified treatment for effect={policy.effect!r} "
                    f"slice={policy.slice!r} requires a non-empty rationale."
                )
        return value

    def lookup_policy(
        self, *, effect: str, slice_value: str
    ) -> WeakIdentificationPolicy | None:
        """Return the most-specific policy matching ``(effect, slice_value)``.

        Exact slice matches win over the ``"*"`` wildcard. Returns
        ``None`` when neither an exact nor a wildcard policy is found.
        """
        wildcard: WeakIdentificationPolicy | None = None
        for policy in self.addressed_weak_identifications:
            if policy.effect != effect:
                continue
            if policy.slice == slice_value:
                return policy
            if policy.slice == "*":
                wildcard = policy
        return wildcard


def build_policies(
    items: Iterable[tuple[str, str, WeakIdentificationTreatment]],
    *,
    rationale: str = "",
) -> tuple[WeakIdentificationPolicy, ...]:
    """Build a tuple of policies from compact ``(effect, slice, treatment)`` triples.

    Useful when constructing model configs in tests or notebooks.
    """
    return tuple(
        WeakIdentificationPolicy(
            effect=effect,
            slice=slice_value,
            treatment=treatment,
            rationale=rationale,
        )
        for effect, slice_value, treatment in items
    )
