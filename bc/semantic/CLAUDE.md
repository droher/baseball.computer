# Semantic layer (BSL)

- Exposes 6 BSL `SemanticTable` factories: offense / pitching / fielding × event / season.
- The `SemanticTable` factories run under `uv --group bsl` only. The `bsl` group's xorq dep pins `sqlglot <28`; SQLMesh needs 30+. The two groups are mutually exclusive in one env. `_tables_common`, `_layout`, and `_views` import no BSL and run under the build group.
- The Pydantic `Metric` registry is shared between build and BSL paths; the import paths are not.
- `bc/semantic/` must not import any `sqlmesh` module.
- `_views.py` renders the six semantic tables as plain SQL views for the published DuckLake catalog. It imports only `_tables_common` and the metric constants, so it runs under the build group too. Keep it in step with `season_with_league` / `event_with_game_info`; `bc/tests/test_semantic_views.py` diffs the two on real data.
