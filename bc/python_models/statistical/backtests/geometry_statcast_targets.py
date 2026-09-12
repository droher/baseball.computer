from __future__ import annotations

import math
from typing import Literal

from pydantic import BaseModel

BroadType = Literal["Ground", "Air"]
Trajectory = Literal["GroundBall", "LineDrive", "Fly", "PopUp"]
CONTRACT = "trajectory-air-standard-v1"


class StandardizedTrajectory(BaseModel):
    contract: Literal["trajectory-air-standard-v1"] = "trajectory-air-standard-v1"
    trajectory: Trajectory | None
    broad_type: BroadType | None
    recorded_broad_type: BroadType | None
    statcast_broad_type: BroadType | None
    recorded_bunt: bool | None
    status: Literal[
        "ground_preserved",
        "air_angle_standardized",
        "air_angle_missing",
        "air_angle_conflict",
        "broad_source_conflict",
        "broad_unresolved",
        "pairing_unresolved",
    ]
    broad_origin: Literal[
        "agreement", "recorded_only", "statcast_only", "conflict", "unknown"
    ]
    angle_boundary: bool


RECORDED_AIR_SUBTYPES: tuple[str, ...] = ("Fly", "LineDrive", "PopUp")


def recorded_air_subtype(value: str | None) -> str | None:
    return value if value in RECORDED_AIR_SUBTYPES else None


def broad_type(value: str | None) -> BroadType | None:
    if value in {"GroundBall", "GroundBallBunt"}:
        return "Ground"
    if value in {"LineDrive", "Fly", "PopUp", "LineDriveBunt", "PopUpBunt"}:
        return "Air"
    if value in {None, "Unknown", "Bunt", "UnspecifiedBunt", "FoulBunt"}:
        return None
    raise ValueError(f"unrecognized trajectory ontology value: {value}")


def standardize_trajectory(
    recorded_class: str | None,
    statcast_class: str | None,
    angle: float | None,
    *,
    pairing_complete: bool,
) -> StandardizedTrajectory:
    if angle is not None and (not math.isfinite(angle) or not -90 <= angle <= 90):
        raise ValueError("invalid launch angle")
    recorded = broad_type(recorded_class)
    reference = broad_type(statcast_class)
    bunt = (
        None if recorded_class in {None, "Unknown"} else "Bunt" in str(recorded_class)
    )
    origin: Literal[
        "agreement", "recorded_only", "statcast_only", "conflict", "unknown"
    ] = "unknown"
    if recorded and reference:
        origin = "agreement" if recorded == reference else "conflict"
    elif recorded:
        origin = "recorded_only"
    elif reference:
        origin = "statcast_only"
    broad = recorded or reference
    trajectory: Trajectory | None = None
    status: Literal[
        "ground_preserved",
        "air_angle_standardized",
        "air_angle_missing",
        "air_angle_conflict",
        "broad_source_conflict",
        "broad_unresolved",
        "pairing_unresolved",
    ]
    if not pairing_complete:
        status = "pairing_unresolved"
    elif origin == "conflict":
        status = "broad_source_conflict"
        broad = None
    elif broad == "Ground":
        status = "ground_preserved"
        trajectory = "GroundBall"
    elif broad is None:
        status = "broad_unresolved"
    elif angle is None:
        status = "air_angle_missing"
    elif angle < 10:
        status = "air_angle_conflict"
    else:
        status = "air_angle_standardized"
        trajectory = "LineDrive" if angle <= 25 else "Fly" if angle <= 50 else "PopUp"
    return StandardizedTrajectory(
        trajectory=trajectory,
        broad_type=broad,
        recorded_broad_type=recorded,
        statcast_broad_type=reference,
        recorded_bunt=bunt,
        status=status,
        broad_origin=origin,
        angle_boundary=angle in {10, 25, 50},
    )
