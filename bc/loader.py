"""Custom SQLMesh loader for the baseball.computer project (Phase 1.5).

Subclasses the stock `SqlMeshLoader` to plug a project-local jinja builtins
module into the `JinjaMacroRegistry`. The reason: the existing dbt-style
macros under `bc/macros/` (e.g. `stat_lists.sql`, `metric_col_lists.sql`,
`metric_calcs.sql`) use `{{ return([...]) }}` to surface lists/dicts back to
callers. SQLMesh's native jinja env does not register `return` by default —
only the dbt-import path does. Rather than rewrite every macro, we point the
registry at `bc/jinja_globals.py`, which augments the default builtin set.

Phase 1 used a different `loader.py` that monkey-patched seed loading. That
patch is gone in Phase 1.5 because SqlMeshLoader does not load seeds at all
(seeds are now materialized via `@load_seeds()` `before_all` hook in
`bc/macros/_init_db.py`).
"""

from __future__ import annotations

from sqlmesh.core.loader import SqlMeshLoader


class BcSqlMeshLoader(SqlMeshLoader):
    """SqlMeshLoader variant that registers project-local jinja builtins."""

    def _load_scripts(self):  # type: ignore[override]
        macros, jinja_macros = super()._load_scripts()
        jinja_macros.create_builtins_module = "jinja_globals"
        return macros, jinja_macros
