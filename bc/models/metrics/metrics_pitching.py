"""Pitching metrics — career, season-league, team-season scopes."""

from __future__ import annotations

from python_models.metrics.registration import register_metric_model

for _scope in ("player_career", "player_season_league", "team_season"):
    register_metric_model("pitching", _scope)
