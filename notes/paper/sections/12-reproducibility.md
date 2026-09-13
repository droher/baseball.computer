## Reproducibility and availability

This manuscript is assembled from the ordered Markdown files in
`notes/paper/sections/`. Its build command is `node notes/paper/build.mjs` from
the repository root. The build uses Pandoc and Typst, removes internal drafting
comments, and writes the assembled Markdown and PDF. The accompanying evidence
ledger records the source generation, report identities, and limitations of the
numbers used in the current results. Historical query files and table snapshots
remain under `notes/paper/queries/` and `notes/paper/tables/`; their dates matter.

The current candidate's source database has SHA-256
`fc94ec74507ceb0e7ab60296006de534253d364b0c9d5b64f352c82749c23f9c`.
The release manifest selects the PBP root and binds its release evidence. The
selected component manifest binds eleven data files, their SQL, frozen
implementation dependencies, source identity, and validation reports. Its geometry
input is `geometry_v2.parquet`; earlier geometry outputs remain diagnostic
artifacts and are not the selected candidate. The namespace revision changed
metadata and output mappings while preserving the eleven component payloads
byte for byte. The release code snapshot is commit `98da62c`.

Candidate paths and the exact report digests are recorded in the paper's
machine-readable evidence snapshot. The source reports are local research
artifacts, not all checked into Git or distributed with the public database.
This repository supplies code and an auditable summary; it does not yet supply
a self-contained public replication archive. In particular, several older
ablation and smoke-backtest run artifacts are not retained, and the legacy
migration cannot recover missing prior-predictive files or missing dependency
lineage. Quoted historical summaries are documentary evidence, not a claim that
every experiment can currently be regenerated from an intact frozen bundle.

The imputation builder reads its source database without modifying it. A small
sample is the default; a full build must explicitly request the full population.
The commands and artifact-selection environment variables are documented in
`docs/pbp-imputation.md`. SQLMesh materialization targets an isolated database
and state copy for candidate testing. Verification checks the actual consumer
views and artifact identities, because changing an artifact-root environment
variable alone need not change a model fingerprint. Neither a successful build
nor this paper's PDF export is a production release.

The recorded-context stress test retains its query, predictions, script, and
report. Entire selected seasons are removed from each field's donor pool before
predicting the 1,000 sampled games. The reported error denominators are the
numbers with recorded truth for each field, not 1,000 for every metric. This
experiment is separate from the older Bayesian and deep partitions described
in Section 2, and from sealed geometry confirmation data. The 121 modern-angle
games and 6,105 historical reserve games were not opened or rescored for the
PBP candidate or this manuscript revision.

The operational database is built through SQLMesh. The current public
distribution pipeline copies approved model and seed tables into a DuckLake
catalog with immutable Parquet data files, semantic views, and metric macros;
the website attaches that catalog read-only. Older standalone database objects
are retained for compatibility and rollback. This distribution mechanism does
not imply that the September 13 candidate has been released or that its local
research artifacts are downloadable from the public catalog.
