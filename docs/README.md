# Documentation guide

[Project overview and setup](../README.md)

## Find a starting point

| Task | Read first |
| --- | --- |
| Build or change the database | [Setup](../README.md#build-engine), [build conventions](../.claude/rules/sqlmesh.md), and [justfile](../justfile) |
| Query tables and metrics | [Published model reference](https://docs.baseball.computer), [semantic layer guide](../bc/semantic/CLAUDE.md), and [LLM schema context](llm/README.md) |
| Publish or verify production | [DuckLake publication](ducklake-production.md) and [script guide](../scripts/CLAUDE.md) |
| Understand estimated data | [Estimated-model guide](estimated-models.md) and [modeling documentation](modeling/README.md) |
| Complete historical imputation | [Full-history completion plan](../notes/full-history-imputation-plan.md) |
| Continue geometry research | [Geometry handoff](geometry-modeling-handoff.md), then its linked protocols and results |
| Find plans, investigations, or writing | [Research notes](../notes/README.md) |
| Pick up outstanding work | [Follow-ups](../notes/followups.md) |
| Work as an agent | [Agent guide](../CLAUDE.md) |

## Development and operations

- [SQLMesh conventions](../.claude/rules/sqlmesh.md): development and production databases, branch environments, source loading, and promotion.
- [Performance settings](../.claude/rules/performance.md): thread and worker settings and source preloading.
- [Script guide](../scripts/CLAUDE.md): publication, uploads, and training utilities.
- [Machine-learning guide](../bc/python_models/ml/CLAUDE.md) and [statistical-model library guide](../bc/python_models/statistical/CLAUDE.md): implementation conventions near their code.
- [QA notes](../bc/qa_notes.md): recorded data-quality investigations.

## Where documentation belongs

| Location | Contents |
| --- | --- |
| [docs/](.) | Operational guides, modeling contracts, protocols, and dated evidence reports. [The modeling index](modeling/README.md) groups the existing reports by topic. |
| [docs/llm/](llm/README.md) | LSF-1 specification, authored supplement, and generated schema context. |
| [notes/](../notes/README.md) | Implementation plans, exploratory investigations, writing drafts, and the follow-up backlog. |
| [bc/models/](../bc/models/) | Model descriptions and shared doc blocks consumed by the model documentation tooling. |
| [CLAUDE.md](../CLAUDE.md) and [scoped guides](../CLAUDE.md#where-to-find-more) | Agent instructions maintained alongside the relevant code. |

Keep the root README focused on setup and entry points. Add new material to the
relevant topic index, and record outstanding work in [follow-ups](../notes/followups.md).
Dated reports describe evidence from a particular run; consult the handoff and
applicable contracts before treating a result as current guidance.

Existing research filenames and companion artifacts stay at their original paths.
Experiments can bind protocol contents and artifact hashes, so navigation changes
should not rewrite those records. Edit authored sources for generated documentation:
the [LLM guide](llm/README.md) identifies its inputs, and generated agent files
identify their Claude sources and synchronization command.
