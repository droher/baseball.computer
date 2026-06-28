"""Bayes target registry. Populated by ``bayes/targets/*`` modules on import."""

from __future__ import annotations

from python_models.statistical.bayes.specs import BayesTargetSpec

_TARGETS: dict[str, BayesTargetSpec] = {}


def register_target(spec: BayesTargetSpec) -> None:
    existing = _TARGETS.get(spec.name)
    if existing is not None:
        if existing != spec:
            raise ValueError(
                f"bayes target {spec.name!r} already registered with different spec"
            )
        return
    _TARGETS[spec.name] = spec


def get_target(name: str) -> BayesTargetSpec:
    try:
        return _TARGETS[name]
    except KeyError as exc:
        known = ", ".join(sorted(_TARGETS)) or "<empty>"
        raise KeyError(f"unknown bayes target {name!r}. registered: {known}") from exc


def all_target_names() -> tuple[str, ...]:
    return tuple(sorted(_TARGETS))


def all_targets() -> tuple[BayesTargetSpec, ...]:
    return tuple(_TARGETS[name] for name in sorted(_TARGETS))
