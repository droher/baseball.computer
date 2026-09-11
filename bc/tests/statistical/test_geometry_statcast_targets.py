from __future__ import annotations

import pytest
from hypothesis import given, strategies as st

from python_models.statistical.backtests.geometry_statcast_targets import (
    standardize_trajectory,
)


@given(
    st.one_of(
        st.none(),
        st.floats(min_value=-90, max_value=90, allow_nan=False, allow_infinity=False),
    )
)
def test_ground_and_bunt_classification_does_not_depend_on_angle(
    angle: float | None,
) -> None:
    for recorded in ("GroundBall", "GroundBallBunt"):
        result = standardize_trajectory(
            recorded, "GroundBall", angle, pairing_complete=True
        )
        assert result.trajectory == "GroundBall"
        assert result.status == "ground_preserved"
        assert result.recorded_bunt == (recorded == "GroundBallBunt")


@given(
    st.one_of(
        st.none(),
        st.floats(min_value=-90, max_value=90, allow_nan=False, allow_infinity=False),
    )
)
def test_airborne_observations_never_become_ground(angle: float | None) -> None:
    for recorded in ("LineDrive", "Fly", "PopUp", "PopUpBunt", "LineDriveBunt"):
        result = standardize_trajectory(recorded, "Fly", angle, pairing_complete=True)
        assert result.broad_type == "Air"
        assert result.trajectory != "GroundBall"
        if angle is not None and angle < 10:
            assert result.status == "air_angle_conflict"
            assert result.trajectory is None


@pytest.mark.parametrize(
    ("angle", "expected"),
    [
        (10, "LineDrive"),
        (25, "LineDrive"),
        (25.01, "Fly"),
        (50, "Fly"),
        (50.01, "PopUp"),
    ],
)
def test_airborne_boundaries_follow_the_declared_convention(
    angle: float, expected: str
) -> None:
    result = standardize_trajectory("Fly", "LineDrive", angle, pairing_complete=True)
    assert result.trajectory == expected
    assert result.angle_boundary == (angle in {10, 25, 50})


def test_broad_source_conflicts_preserve_sources_without_a_calibration_target() -> None:
    for recorded, reference in [("GroundBall", "Fly"), ("PopUp", "GroundBall")]:
        result = standardize_trajectory(recorded, reference, 30, pairing_complete=True)
        assert result.trajectory is None
        assert result.status == "broad_source_conflict"
        assert result.recorded_broad_type != result.statcast_broad_type


def test_missing_broad_type_and_unverified_pairing_cannot_create_a_target() -> None:
    unresolved = standardize_trajectory("Unknown", None, 35, pairing_complete=True)
    assert unresolved.trajectory is None and unresolved.status == "broad_unresolved"
    unmatched = standardize_trajectory(
        "GroundBall", "GroundBall", 0, pairing_complete=False
    )
    assert unmatched.trajectory is None and unmatched.status == "pairing_unresolved"
    reference = standardize_trajectory(
        "Unknown", "GroundBall", None, pairing_complete=True
    )
    assert (
        reference.trajectory == "GroundBall"
        and reference.broad_origin == "statcast_only"
    )
    missing_angle = standardize_trajectory("Fly", None, None, pairing_complete=True)
    assert (
        missing_angle.trajectory is None and missing_angle.status == "air_angle_missing"
    )


@pytest.mark.parametrize("angle", [float("nan"), float("inf"), -91, 91])
def test_invalid_angles_remain_errors_even_for_ground_balls(angle: float) -> None:
    with pytest.raises(ValueError, match="invalid launch angle"):
        standardize_trajectory("GroundBall", "GroundBall", angle, pairing_complete=True)
