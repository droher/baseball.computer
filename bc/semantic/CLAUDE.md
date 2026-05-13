# Semantic layer (BSL)

- Exposes 6 BSL `SemanticTable` factories: offense / pitching / fielding × event / season.
- Runs under `uv --group bsl` only. The `bsl` group's xorq dep pins `sqlglot <28`; SQLMesh needs 30+. The two groups are mutually exclusive in one env.
- The Pydantic `Metric` registry is shared between build and BSL paths; the import paths are not.
- `bc/semantic/` must not import any `sqlmesh` module.
