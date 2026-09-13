# Research paper

[Research notes](../README.md) · [Documentation guide](../../docs/README.md)

The manuscript was revised on September 13, 2026 to distinguish the full-history
PBP imputation candidate from historical Bayesian experiments and their outdated
validation labels. The PDF is built from the current sections. This is a revised
research manuscript, with no verified journal submission or external acceptance.

## Read and reproduce

- [Manuscript](data-coverage-paper.md) and [PDF](data-coverage-paper.pdf)
- [Evidence ledger](EVIDENCE.md) and [report snapshot](evidence-snapshot-2026-09-13.json)
- [Verified references](reference-verification.md)
- [Source sections](sections/), [historical tables](tables/), and [historical queries](queries/)
- [Revision record](REVISION-2026-09-13.md)

From the repository root, with Node.js, Pandoc, and Typst installed:

```sh
node notes/paper/build.mjs
```

The build assembles the ordered sections and renders the PDF; it does not rerun
statistical experiments or change a database. To refresh the checked-in evidence
extract from the retained local reports:

```sh
node notes/paper/refresh-evidence.mjs
```

The reports under `artifacts/` are local and Git-ignored. The snapshot records
what was inspected, but does not replace a full public replication archive.

## Historical drafting records

[Outline](OUTLINE.md), [review](REVIEW.md), [referee-style report](REFEREE-REPORT.md),
[original revision plan](REVISION-PLAN.md), and [response](RESPONSE-TO-REVIEWERS.md)
are internal drafting history. Their earlier claims and tasks are superseded by
the current manuscript and evidence ledger. They do not document external peer
review. Later research context lives in the [modeling index](../../docs/modeling/README.md)
and [geometry handoff](../../docs/geometry-modeling-handoff.md).
