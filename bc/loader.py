"""Custom SQLMesh loader for the baseball.computer dbt-import path.

Subclasses `sqlmesh.dbt.loader.DbtLoader` to fix one upstream defect:
seed CSVs are loaded via pandas with `keep_default_na=True` hardcoded
(`sqlmesh/dbt/seed.py:82-86`), which silently coerces literal `"NA"`,
`"NULL"`, `"N/A"`, etc. to NULL — even when those tokens carry meaning
to the data. dbt's own seed loader uses agate, which preserves them.

`bc/seeds/misc/seed_franchises.csv` uses `"NA"` for the National
Association (1871-1875) in the `league` column. Without this patch,
SQLMesh-built tables disagree with dbt-built tables on every NA-bearing
row — and that divergence cascades through `game_start_info` into park
factors, standings, and metrics.

The patch wraps `SeedConfig.to_sqlmesh` to flip `keep_default_na=False`
on the constructed `CsvSettings`. The wrapper is idempotent (guarded by
a sentinel attribute) and only the empty string + the existing single-
space sentinel are then treated as null — matching dbt-agate behavior.
"""

from __future__ import annotations

import logging

from sqlmesh.dbt.loader import DbtLoader
from sqlmesh.dbt.seed import SeedConfig

logger = logging.getLogger(__name__)


def _patch_seed_loader() -> None:
    if getattr(SeedConfig.to_sqlmesh, "_bc_patched", False):
        return

    original = SeedConfig.to_sqlmesh

    def patched(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        model = original(self, *args, **kwargs)
        kind = getattr(model, "kind", None)
        csv_settings = getattr(kind, "csv_settings", None)
        if csv_settings is None:
            return model
        # Disable pandas' default-NA stripping so literal "NA"/"NULL"/etc.
        # survive as strings (matches dbt-agate). Then re-add "" and " " to
        # na_values so empty fields and the single-space sentinel still
        # become NULL — agate treats both as null.
        if csv_settings.keep_default_na is not False:
            csv_settings.keep_default_na = False
        existing = list(csv_settings.na_values or [])
        for sentinel in ("", " "):
            if sentinel not in existing:
                existing.append(sentinel)
        csv_settings.na_values = existing
        logger.debug(
            "PatchedDbtLoader: applied seed csv_settings overrides for %s",
            model.name,
        )
        return model

    patched._bc_patched = True  # type: ignore[attr-defined]
    SeedConfig.to_sqlmesh = patched  # type: ignore[method-assign]


class PatchedDbtLoader(DbtLoader):
    """DbtLoader variant that disables pandas' default-NA stripping for seeds."""

    def __init__(self, *args, **kwargs) -> None:  # type: ignore[no-untyped-def]
        _patch_seed_loader()
        super().__init__(*args, **kwargs)
