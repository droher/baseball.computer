from __future__ import annotations

import csv
import io
import math
import zipfile
from collections import Counter
from pathlib import Path
from typing import Literal

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

TEAM_IDS = {
    "ANA": 108,
    "LAA": 108,
    "ARI": 109,
    "ATL": 144,
    "BAL": 110,
    "BOS": 111,
    "CHA": 145,
    "CHN": 112,
    "CIN": 113,
    "CLE": 114,
    "COL": 115,
    "DET": 116,
    "HOU": 117,
    "KCA": 118,
    "LAN": 119,
    "MIA": 146,
    "FLA": 146,
    "MIL": 158,
    "MIN": 142,
    "NYA": 147,
    "NYN": 121,
    "OAK": 133,
    "ATH": 133,
    "PHI": 143,
    "PIT": 134,
    "SDN": 135,
    "SEA": 136,
    "SFN": 137,
    "SLN": 138,
    "TBA": 139,
    "TEX": 140,
    "TOR": 141,
    "WAS": 120,
    "WSN": 120,
}
BB_TYPES = {
    "ground_ball": "GroundBall",
    "line_drive": "LineDrive",
    "fly_ball": "Fly",
    "popup": "PopUp",
}


class SelectedGame(BaseModel):
    model_config = ConfigDict(extra="ignore")
    game_id: str = Field(pattern=r"^[A-Z0-9]{3}[0-9]{8}0$")
    season: int
    park_id: str
    date: str = Field(pattern=r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
    home_team_id: str
    away_team_id: str
    primary_fold: Literal["TRAIN"]
    acquisition_fold: Literal["fitting", "modern_angle_evaluation"]
    is_smoke: bool


class TeamId(BaseModel):
    id: int


class ScheduleSide(BaseModel):
    team: TeamId


class ScheduleTeams(BaseModel):
    home: ScheduleSide
    away: ScheduleSide


class ScheduleGame(BaseModel):
    gamePk: int
    gameNumber: int
    doubleHeader: str
    gameType: str
    teams: ScheduleTeams


class ScheduleDate(BaseModel):
    date: str
    games: list[ScheduleGame]


class Schedule(BaseModel):
    dates: list[ScheduleDate]


class Ball(BaseModel):
    game_pk: int
    game_date: str
    at_bat_number: int
    inning: int
    inning_topbot: Literal["Top", "Bot"]
    batter: str
    pitcher: str
    bb_type: str
    launch_angle: str
    launch_speed: str
    events: str
    description: str
    type: str


def resolve_game(payload: bytes, selected: SelectedGame) -> int:
    schedule = Schedule.model_validate_json(payload)
    home = TEAM_IDS[selected.home_team_id]
    away = TEAM_IDS[selected.away_team_id]
    candidates = [
        game
        for day in schedule.dates
        if day.date == selected.date
        for game in day.games
        if game.teams.home.team.id == home
        and game.teams.away.team.id == away
        and game.doubleHeader == "N"
        and game.gameNumber == 1
        and game.gameType == "R"
    ]
    if len(candidates) != 1:
        raise ValueError("game resolution is absent or ambiguous")
    return candidates[0].gamePk


def numeric(value: str, *, angle: bool = False) -> float | None:
    if not value:
        return None
    number = float(value)
    if (
        not math.isfinite(number)
        or (angle and not -90 <= number <= 90)
        or (not angle and number < 0)
    ):
        raise ValueError("invalid tracking measurement")
    return number


def angle_class(angle: float | None) -> str | None:
    if angle is None:
        return None
    if not math.isfinite(angle) or not -90 <= angle <= 90:
        raise ValueError("invalid launch angle")
    if angle < 10:
        return "GroundBall"
    if angle <= 25:
        return "LineDrive"
    if angle <= 50:
        return "Fly"
    return "PopUp"


def read_balls(payload: bytes, selected: SelectedGame, game_pk: int) -> list[Ball]:
    pitches = list(csv.DictReader(io.StringIO(payload.decode("utf-8-sig"))))
    if not pitches:
        raise ValueError("empty Statcast response")
    if any(
        row.get("game_pk") != str(game_pk) or row.get("game_date") != selected.date
        for row in pitches
    ):
        raise ValueError("unexpected Statcast game")
    balls = [
        Ball.model_validate(row)
        for row in pitches
        if row.get("type") == "X" or row.get("bb_type")
    ]
    if any(ball.bb_type and ball.bb_type not in BB_TYPES for ball in balls):
        raise ValueError("unknown Statcast batted-ball type")
    return balls


def read_crosswalk(path: Path) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    with zipfile.ZipFile(path) as archive:
        for name in archive.namelist():
            if "/data/people-" not in name or not name.endswith(".csv"):
                continue
            for row in csv.DictReader(
                io.StringIO(archive.read(name).decode("utf-8-sig"))
            ):
                if row["key_retro"] and row["key_mlbam"]:
                    mapping.setdefault(row["key_retro"], set()).add(row["key_mlbam"])
    if not mapping:
        raise ValueError("empty identity crosswalk")
    return mapping


def pair_game(
    local: pl.DataFrame, balls: list[Ball], crosswalk: dict[str, set[str]]
) -> tuple[pl.DataFrame, dict[str, object]]:
    if local.is_empty() or local["event_key"].n_unique() != local.height:
        raise ValueError("empty or duplicate local event keys")
    local_counts = Counter(int(value) for value in local["retrosheet_pa_ordinal"])
    remote_counts = Counter(ball.at_bat_number for ball in balls)
    remote = {ball.at_bat_number: ball for ball in balls}
    output: list[dict[str, object]] = []
    for event in local.to_dicts():
        ordinal = int(event["retrosheet_pa_ordinal"])
        ball = remote.get(ordinal)
        status = "matched"
        if local_counts[ordinal] != 1 or remote_counts[ordinal] > 1:
            status = "ambiguous_pa"
        elif ball is None:
            status = "missing_remote"
        elif crosswalk.get(str(event["batter_id"]), set()) != {
            ball.batter
        } or crosswalk.get(str(event["pitcher_id"]), set()) != {ball.pitcher}:
            status = "identity_mismatch_or_missing"
        elif (
            event["inning_start"] != ball.inning
            or event["frame_start"]
            != {"Top": "Top", "Bot": "Bottom"}[ball.inning_topbot]
        ):
            status = "inning_mismatch"
        angle = (
            numeric(ball.launch_angle, angle=True)
            if ball and status == "matched"
            else None
        )
        speed = numeric(ball.launch_speed) if ball and status == "matched" else None
        output.append(
            {
                **event,
                "match_status": status,
                "mlbam_batter_id": ball.batter if ball else None,
                "mlbam_pitcher_id": ball.pitcher if ball else None,
                "statcast_at_bat_number": ball.at_bat_number if ball else None,
                "statcast_bb_type": BB_TYPES.get(ball.bb_type)
                if ball and status == "matched"
                else None,
                "launch_angle": angle,
                "launch_speed": speed,
                "launch_angle_raw": ball.launch_angle
                if ball and status == "matched"
                else None,
                "launch_speed_raw": ball.launch_speed
                if ball and status == "matched"
                else None,
                "angle_standardized_class": angle_class(angle),
                "boundary_rounded_angle": angle in {10, 25, 50},
                "measurement_origin": "tracking_or_estimate_not_distinguished",
            }
        )
    counts = Counter(str(row["match_status"]) for row in output)
    remote_only = sorted(set(remote_counts) - set(local_counts))
    exact = (
        counts["matched"] == local.height
        and not remote_only
        and all(n == 1 for n in remote_counts.values())
    )
    frame = pl.DataFrame(output, infer_schema_length=None).with_columns(
        pl.lit(exact).alias("game_pairing_complete")
    )
    return frame, {
        "local_events": local.height,
        "remote_batted_ball_rows": len(balls),
        "match_counts": dict(counts),
        "remote_only_pa_ordinals": remote_only,
        "unique_local_pa": len(local_counts) == local.height,
        "unique_remote_pa": len(remote_counts) == len(balls),
        "game_pairing_complete": exact,
        "matched_angle_available": frame.filter(
            pl.col("launch_angle").is_not_null()
        ).height,
        "standardized_reliability_established": False,
    }
