"""Structured stdlib logging config for statistical jobs."""

from __future__ import annotations

import logging
import time
from collections.abc import Generator
from contextlib import contextmanager

_DEFAULT_FORMAT = "%(asctime)s %(name)s %(levelname)s %(message)s"


def configure(level: int = logging.INFO, *, fmt: str = _DEFAULT_FORMAT) -> None:
    logging.basicConfig(level=level, format=fmt)


_log = logging.getLogger(__name__)


@contextmanager
def logged_step(step: str, **fields: object) -> Generator[None, None, None]:
    started = time.perf_counter()
    _log.info("step_start step=%s %s", step, fields)
    try:
        yield
    except Exception:
        _log.exception("step_failed step=%s %s", step, fields)
        raise
    elapsed = time.perf_counter() - started
    _log.info("step_done step=%s elapsed_seconds=%.3f %s", step, elapsed, fields)
