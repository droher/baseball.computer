"""Instrumented full-build harness for SQLMesh.

Forces ``BC_PERF_MODE=1`` (config.py uses that to pin the DuckDB pool
to a single connection and to enable JSON profiling) and wraps every
``SnapshotEvaluator.evaluate`` call with timing, RSS sampling, DuckDB
temp-dir spill measurement, and a per-snapshot copy of the JSON
profile tree (DuckDB profiling overwrites a single output file per
query, so we redirect ``profile_output`` to a unique path before each
evaluate so the model's final query plan survives).

Run with::

    uv run --group migration python scripts/perf_run.py

Output:
    logs/perf/perf_<UTCstamp>.jsonl   per-snapshot metrics
    logs/perf/perf_<UTCstamp>.log     human log
    logs/perf/profiles/<name>.json    DuckDB JSON plan tree per snapshot
"""

from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = Path(__file__).resolve().parent
import sys

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))
BC_DIR = PROJECT_ROOT / "bc"
DB_PATH = PROJECT_ROOT / "bc.db"
DB_TEMP_DIR = PROJECT_ROOT / "bc.db.tmp"
LOG_DIR = PROJECT_ROOT / "logs" / "perf"
PROFILE_DIR = LOG_DIR / "profiles"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROFILE_DIR.mkdir(parents=True, exist_ok=True)

# Pin perf-mode BEFORE importing sqlmesh so config.py picks it up.
os.environ["BC_PERF_MODE"] = "1"

import psutil  # noqa: E402
from sqlmesh import Context  # noqa: E402
from sqlmesh.core.snapshot.evaluator import SnapshotEvaluator  # noqa: E402

RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
JSONL_PATH = LOG_DIR / f"perf_{RUN_TS}.jsonl"
TEXT_LOG = LOG_DIR / f"perf_{RUN_TS}.log"

_NAME_SAFE_RE = re.compile(r"[^A-Za-z0-9_.-]+")

logger = logging.getLogger("perf_run")


def dir_size_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            pass
    return total


def file_size_bytes(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


class RSSSampler(threading.Thread):
    """Sample own-process RSS at fixed interval; record peak.

    DuckDB executes in-process, so own-process RSS captures it.
    """

    stop_event: threading.Event
    interval: float
    peak_bytes: int
    start_bytes: int
    _proc: psutil.Process

    def __init__(self, stop_event: threading.Event, interval: float = 0.5) -> None:
        super().__init__(daemon=True)
        self.stop_event = stop_event
        self.interval = interval
        self.peak_bytes = 0
        self.start_bytes = 0
        self._proc = psutil.Process()

    def run(self) -> None:  # noqa: D401
        try:
            self.start_bytes = int(self._proc.memory_info().rss)
            self.peak_bytes = self.start_bytes
        except psutil.Error:
            pass
        while not self.stop_event.is_set():
            try:
                rss = int(self._proc.memory_info().rss)
                if rss > self.peak_bytes:
                    self.peak_bytes = rss
            except psutil.Error:
                pass
            self.stop_event.wait(self.interval)


def _safe_kind(snapshot: Any) -> str:
    try:
        return type(snapshot.model.kind).__name__
    except AttributeError:
        return "unknown"


def _safe_model_name(snapshot: Any) -> str:
    val = getattr(snapshot, "name", None)
    if val:
        return str(val)
    return repr(snapshot)


def _sanitize(name: str) -> str:
    return _NAME_SAFE_RE.sub("_", name).strip("._-")


def install_audit_skip() -> None:
    """No-op SnapshotEvaluator.audit.

    Audits like ``relationships(... to_model := main_models.people)`` render
    cross-model references against the virtual layer (``bc.main_models.people``)
    which only exists after the backfill stage finishes. On a fresh state DB
    that virtual schema is absent during backfill, so audits fail with
    ``schema "main_models" does not exist``. We're measuring build cost, not
    audit cost, so just skip them for the perf run.
    """

    def noop_audit(self: SnapshotEvaluator, snapshot: Any, **kwargs: Any) -> list[Any]:
        del self, snapshot, kwargs
        return []

    SnapshotEvaluator.audit = noop_audit  # type: ignore[method-assign]


def install_evaluator_hook(jsonl_path: Path) -> None:
    """Wrap SnapshotEvaluator.evaluate with per-snapshot instrumentation."""
    original = SnapshotEvaluator.evaluate

    def wrapped(self: SnapshotEvaluator, snapshot: Any, *args: Any, **kwargs: Any) -> Any:
        name = _safe_model_name(snapshot)
        kind = _safe_kind(snapshot)
        safe = _sanitize(name)
        profile_path = PROFILE_DIR / f"{safe}.json"

        # Redirect profiling to a per-snapshot file. DuckDB overwrites
        # profile_output every query; since `evaluate` runs at most a
        # CTAS plus framing DDL, the last write captures the build query
        # for SQL models. Multi-query Python/Ibis snapshots only retain
        # their final query's plan — flagged in followups.
        try:
            adapter = self.adapter
            adapter.execute(f"SET profile_output = '{profile_path.as_posix()}'")
        except Exception as exc:
            logger.warning("could not set profile_output for %s: %r", name, exc)

        temp_before = dir_size_bytes(DB_TEMP_DIR)
        db_before = file_size_bytes(DB_PATH)
        stop = threading.Event()
        sampler = RSSSampler(stop)
        sampler.start()
        t0 = time.perf_counter()
        wall_start = datetime.now(timezone.utc)
        err: str | None = None
        try:
            return original(self, snapshot, *args, **kwargs)
        except Exception as exc:
            err = repr(exc)
            raise
        finally:
            elapsed = time.perf_counter() - t0
            stop.set()
            sampler.join()
            temp_after = dir_size_bytes(DB_TEMP_DIR)
            db_after = file_size_bytes(DB_PATH)
            record = {
                "model": name,
                "kind": kind,
                "started_at": wall_start.isoformat(),
                "duration_s": round(elapsed, 3),
                "rss_start_mb": round(sampler.start_bytes / (1024**2), 1),
                "rss_peak_mb": round(sampler.peak_bytes / (1024**2), 1),
                "rss_growth_mb": round(
                    (sampler.peak_bytes - sampler.start_bytes) / (1024**2), 1
                ),
                "temp_before_mb": round(temp_before / (1024**2), 1),
                "temp_after_mb": round(temp_after / (1024**2), 1),
                "temp_growth_mb": round((temp_after - temp_before) / (1024**2), 1),
                "db_before_mb": round(db_before / (1024**2), 1),
                "db_after_mb": round(db_after / (1024**2), 1),
                "db_growth_mb": round((db_after - db_before) / (1024**2), 1),
                "profile_path": (
                    str(profile_path) if profile_path.exists() else None
                ),
                "error": err,
            }
            with jsonl_path.open("a") as f:
                f.write(json.dumps(record) + "\n")
            logger.info(
                "evaluated %s (%s): %.1fs rss_peak=%.1fGB spill=%+.0fMB db=%+.0fMB%s",
                name,
                kind,
                elapsed,
                sampler.peak_bytes / (1024**3),
                (temp_after - temp_before) / (1024**2),
                (db_after - db_before) / (1024**2),
                f" ERROR={err}" if err else "",
            )

    SnapshotEvaluator.evaluate = wrapped  # type: ignore[method-assign]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(TEXT_LOG),
            logging.StreamHandler(),
        ],
    )
    logger.info(
        "perf run starting; jsonl=%s text_log=%s db=%s",
        JSONL_PATH,
        TEXT_LOG,
        DB_PATH,
    )

    install_audit_skip()
    install_evaluator_hook(JSONL_PATH)

    # Pre-populate bc.db with parallel parquet loads before SQLMesh
    # opens its (single-writer) gateway. The init_db macro then sees
    # CREATE TABLE IF NOT EXISTS as no-ops.
    from preload_sources import DEFAULT_SOURCE_ROOTS, preload

    preload_t0 = time.monotonic()
    preload(
        workers=int(os.environ.get("BC_INIT_DB_PARALLELISM", "8")),
        force_reload=False,
        roots=DEFAULT_SOURCE_ROOTS,
    )
    logger.info("preload complete in %.1fs", time.monotonic() - preload_t0)

    os.chdir(BC_DIR)
    ctx = Context(paths=[BC_DIR], concurrent_tasks=1)
    gateway_conn = ctx.config.gateways["bc"].connection
    logger.info(
        "Context loaded; concurrent_tasks=%s pool=%s perf_mode=%s",
        ctx.concurrent_tasks,
        getattr(gateway_conn, "concurrent_tasks", "?"),
        os.environ.get("BC_PERF_MODE"),
    )

    plan = ctx.plan(
        environment="dev",
        auto_apply=True,
        no_prompts=True,
        skip_tests=True,
        skip_linter=True,
    )
    logger.info(
        "plan complete: missing_intervals=%d directly_modified=%d",
        len(plan.missing_intervals or []),
        len(plan.directly_modified or []),
    )

    # Force a final WAL flush. Without this the bc.db.wal still contains
    # the last batch of ALTER COLUMN statements (from `alter_types`) that
    # reference custom ENUM types; subsequent reopens crash on WAL replay
    # because DuckDB cannot resolve those types without bc.db's user
    # catalog already loaded. Checkpointing collapses the WAL into the
    # main file so the database opens cleanly afterwards.
    try:
        ctx.engine_adapter.execute("CHECKPOINT bc")
        logger.info("final CHECKPOINT bc done")
    except Exception as exc:
        logger.warning("final checkpoint failed: %r", exc)


if __name__ == "__main__":
    main()
