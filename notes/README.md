# Research notes and plans

[Documentation guide](../docs/README.md)

These notes include active follow-ups, historical investigations, proposals, and
writing drafts. Dates and status statements belong to each document; a listed
proposal or handoff is not evidence that its implementation is still pending.

## Follow-ups and implementation

- [Open follow-ups](followups.md): the shared backlog and recorded dispositions.
- [Data coverage implementation plan](data-coverage-implementation/README.md): the six-phase plan and its detailed contracts.
- [Implementation checklist](data-coverage-implementation/implementation-checklist.md) and [implementation review](data-coverage-implementation/implementation-review.md): progress and review evidence for that plan.
- [Modeling documentation](../docs/modeling/README.md): contracts, protocols, and dated experiment reports.
- [Geometry handoff](../docs/geometry-modeling-handoff.md): continuation context and evidence boundaries.

## Performance investigations

- [Build performance deep dive](perf-deep-dive.md): build time, memory, disk, and recorded changes.
- [Per-model profiling report](perf-profile-report.md): hot operators and implementation status.
- [Performance settings](../.claude/rules/performance.md): configuration guidance for running the pipeline.

## Data and modeling investigations

- [Synthetic-lineup optimizer handoff](synthetic-lineup-optimizer-handoff.md): a checkpoint with branch-specific continuation instructions.
- [Synthetic-lineup backtest, 1871–1910](synthetic-lineup-backtest-1871-1910.md): methodology, coverage caveats, results, and artifacts.
- [Measuring fielding from season data](measuring_fielding.md): exploratory fielding methodology.
- [Custom test ideas](custom_test_todos.md): older candidate data checks; consult the shared backlog before treating them as open work.
- [QA notes](../bc/qa_notes.md): data-quality findings near the model code.

## Writing and earlier proposals

- [Research paper](paper/README.md): manuscript, sections, reviews, and supporting tables.
- [LLM metadata proposal](llm-metadata.md): the earlier remote-database proposal. Use the [LLM context guide](../docs/llm/README.md) and [DuckLake publication guide](../docs/ducklake-production.md) for the implemented publication workflow.
- [Blog notes](blog.md) and [possible posts](possible_posts.md): writing ideas and older project notes.
