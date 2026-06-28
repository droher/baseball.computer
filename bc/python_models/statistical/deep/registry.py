"""Target-name → DeepTargetSpec + published-manifest table mapping.

Populated by each ``deep/targets/<family>.py`` module as it ships. PR1
leaves the registry empty; PR3 (Geometry) is the first to register.
"""

from __future__ import annotations

from typing import Literal

from python_models.statistical.deep.target_spec import DeepTargetSpec

SiblingManifestName = Literal[
    "dl_proposal_manifest",
    "dl_credit_proposal_manifest",
    "dl_advancement_proposal_manifest",
    "dl_embedding_artifact",
]


_TARGETS: dict[str, DeepTargetSpec] = {}
_MANIFEST_SIBLINGS: dict[str, SiblingManifestName] = {}


def register_target(
    spec: DeepTargetSpec,
    *,
    sibling_manifest: SiblingManifestName,
) -> None:
    if spec.name in _TARGETS:
        existing = _TARGETS[spec.name]
        if existing != spec:
            raise ValueError(
                f"target {spec.name!r} already registered with different spec"
            )
        return
    _TARGETS[spec.name] = spec
    _MANIFEST_SIBLINGS[spec.name] = sibling_manifest


def get_target(name: str) -> DeepTargetSpec:
    try:
        return _TARGETS[name]
    except KeyError as exc:
        known = ", ".join(sorted(_TARGETS)) or "<empty>"
        raise KeyError(f"unknown deep target {name!r}. registered: {known}") from exc


def sibling_manifest_for(name: str) -> SiblingManifestName:
    return _MANIFEST_SIBLINGS[name]


def all_target_names() -> tuple[str, ...]:
    return tuple(sorted(_TARGETS))
