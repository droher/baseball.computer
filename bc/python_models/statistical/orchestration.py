"""Idempotent step runner + status tracking."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

_log = logging.getLogger(__name__)

StepStatus = Literal["pending", "running", "completed", "skipped", "failed"]


@dataclass
class StepResult:
    name: str
    status: StepStatus
    artifact_id: str | None = None
    message: str | None = None


def run_step(name: str, fn: Callable[[], StepResult]) -> StepResult:
    """Run a step function and log start/stop transitions."""
    _log.info("step_start step=%s", name)
    try:
        result = fn()
    except Exception:
        _log.exception("step_failed step=%s", name)
        raise
    _log.info("step_done step=%s status=%s artifact_id=%s", name, result.status, result.artifact_id)
    return result
