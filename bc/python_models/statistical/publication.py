"""Publication gate comparing EDA findings to a model's ModelConfig.

The gate runs before the ``publish-manifest`` CLI writes a pointer to a
fitted artifact. It enforces three rules per doc-02 §"Acceptance
Criteria" and doc-05 §"Tier policies":

1. Every blocking EDA finding must appear in
   ``ModelConfig.expected_blocking_findings`` — unexpected findings
   block the publication.
2. Every EDA ``WeakIdentificationFlag`` must match a
   ``WeakIdentificationPolicy`` on the model. Untreated flags block.
3. ``accept_unidentified`` policies are allowed but logged as warnings
   so audits can review them.

The gate returns a structured :class:`PublicationGateResult`. Callers
(CLI, validate command, programmatic publishers) decide whether to
treat warnings as failures or merely surface them.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

from pydantic import BaseModel

from python_models.statistical.model_config import ModelConfig, WeakIdentificationPolicy
from python_models.statistical.schemas import (
    BlockingFinding,
    EdaReport,
    WeakIdentificationFlag,
)

_log = logging.getLogger(__name__)


class PublicationViolation(BaseModel):
    code: str
    message: str
    severity: str
    effect: str | None = None
    slice: str | None = None


class PublicationGateResult(BaseModel):
    model_name: str
    dataset_artifact_id: str | None = None
    blocking_violations: tuple[PublicationViolation, ...] = ()
    warnings: tuple[PublicationViolation, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blocking_violations


def evaluate_publication_gate(
    *,
    config: ModelConfig,
    eda_report: EdaReport,
    treat_accept_unidentified_as_warning: bool = True,
) -> PublicationGateResult:
    """Run the publication gate against an EDA report.

    The check has two halves:

    1. ``ModelConfig.expected_blocking_findings`` ⊇ EDA blocking codes.
    2. Every EDA ``WeakIdentificationFlag`` has a matching policy.

    ``ModelConfig.dataset_name`` and ``ModelConfig.dataset_version`` are
    cross-checked against the EDA report so a config can't be paired
    with a snapshot it wasn't authored against.
    """
    blocking: list[PublicationViolation] = []
    warnings: list[PublicationViolation] = []

    if config.dataset_name != eda_report.dataset_name:
        blocking.append(
            PublicationViolation(
                code="dataset_name_mismatch",
                message=(
                    f"ModelConfig.dataset_name={config.dataset_name!r} "
                    f"but EDA report dataset_name={eda_report.dataset_name!r}."
                ),
                severity="block",
            )
        )
    if config.dataset_version != eda_report.dataset_version:
        blocking.append(
            PublicationViolation(
                code="dataset_version_mismatch",
                message=(
                    f"ModelConfig.dataset_version={config.dataset_version!r} "
                    f"but EDA report dataset_version={eda_report.dataset_version!r}."
                ),
                severity="block",
            )
        )

    blocking.extend(_check_blocking_findings(config, eda_report.blocking_findings))
    weak_violations, weak_warnings = _check_weak_identification(
        config,
        eda_report.weak_identification_flags,
        treat_accept_unidentified_as_warning,
    )
    blocking.extend(weak_violations)
    warnings.extend(weak_warnings)

    result = PublicationGateResult(
        model_name=config.model_name,
        dataset_artifact_id=eda_report.dataset_artifact_id,
        blocking_violations=tuple(blocking),
        warnings=tuple(warnings),
    )
    _log.info(
        "publication_gate model=%s passed=%s blocking=%d warnings=%d",
        config.model_name,
        result.passed,
        len(result.blocking_violations),
        len(result.warnings),
    )
    return result


def _check_blocking_findings(
    config: ModelConfig,
    findings: Sequence[BlockingFinding],
) -> list[PublicationViolation]:
    expected = frozenset(config.expected_blocking_findings)
    out: list[PublicationViolation] = []
    for finding in findings:
        if finding.code in expected:
            continue
        out.append(
            PublicationViolation(
                code="unexpected_blocking_finding",
                message=(
                    f"EDA emitted blocking code {finding.code!r} "
                    f"({finding.message}) but model config does not list it in "
                    "expected_blocking_findings."
                ),
                severity="block",
            )
        )
    return out


def _check_weak_identification(
    config: ModelConfig,
    flags: Sequence[WeakIdentificationFlag],
    treat_accept_as_warning: bool,
) -> tuple[list[PublicationViolation], list[PublicationViolation]]:
    blocking: list[PublicationViolation] = []
    warnings: list[PublicationViolation] = []
    for flag in flags:
        policy = config.lookup_policy(effect=flag.effect, slice_value=flag.slice)
        if policy is None:
            blocking.append(
                PublicationViolation(
                    code="weak_identification_unaddressed",
                    message=(
                        f"EDA flagged effect={flag.effect!r} "
                        f"slice={flag.slice!r} ({flag.reason}, share="
                        f"{flag.share:.3f}); ModelConfig has no matching "
                        "addressed_weak_identifications policy."
                    ),
                    severity="block",
                    effect=flag.effect,
                    slice=flag.slice,
                )
            )
            continue
        warnings.append(_policy_warning(flag, policy, treat_accept_as_warning))
    return blocking, warnings


def _policy_warning(
    flag: WeakIdentificationFlag,
    policy: WeakIdentificationPolicy,
    treat_accept_as_warning: bool,
) -> PublicationViolation:
    if policy.treatment == "accept_unidentified" and treat_accept_as_warning:
        return PublicationViolation(
            code="weak_identification_accepted",
            message=(
                f"effect={flag.effect!r} slice={flag.slice!r} accepted via "
                f"accept_unidentified policy. Rationale: {policy.rationale}"
            ),
            severity="warn",
            effect=flag.effect,
            slice=flag.slice,
        )
    return PublicationViolation(
        code="weak_identification_addressed",
        message=(
            f"effect={flag.effect!r} slice={flag.slice!r} addressed by "
            f"treatment={policy.treatment!r}."
        ),
        severity="info",
        effect=flag.effect,
        slice=flag.slice,
    )
