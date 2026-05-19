"""Registry sanity: every target either supports shrunk via DL proposal or is zero-only."""

from __future__ import annotations

import pytest

from python_models.statistical.bayes import targets as _targets  # noqa: F401
from python_models.statistical.bayes.registry import all_targets


def test_every_bayes_target_declares_dl_proposal_dimension_or_uses_zero_only() -> None:
    """A target listing ``gamma_dl_shrunk`` must set ``dl_proposal_dimension``."""
    targets = all_targets()
    assert targets, "no bayes targets registered"
    for spec in targets:
        if "gamma_dl_shrunk" in spec.default_flavors:
            assert spec.dl_proposal_dimension is not None, (
                f"target {spec.name!r} lists gamma_dl_shrunk but has dl_proposal_dimension=None"
            )


def test_observation_targets_share_dataset() -> None:
    targets = all_targets()
    datasets = {spec.dataset_name for spec in targets}
    assert datasets == {"model_input_observation_batted_ball"}


def test_broad_contact_uses_trajectory_collapse() -> None:
    from python_models.statistical.bayes.registry import get_target

    spec = get_target("broad_contact_observedness")
    assert spec.dataset_dimension_filter == "trajectory"
    assert spec.dl_proposal_dimension == "trajectory"
    assert set(spec.dl_class_collapse_positive) == {"Fly", "LineDrive", "PopUp"}


def test_unknown_target_raises() -> None:
    from python_models.statistical.bayes.registry import get_target

    with pytest.raises(KeyError):
        _ = get_target("does_not_exist")
