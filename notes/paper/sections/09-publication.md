## Publication policy

Publication status and evidential support are separate properties. The model
registry distinguishes official values, deterministic derivations, estimates,
synthetic data, and withheld outputs. The current PBP layer uses the explicit
`pbp_imputed_*` names even where a view combines recorded values with estimates.
The source columns remain available, and estimates carry their method,
artifact identity, and applicable uncertainty or conflict fields. These names
make estimated status visible at the point of use.

Legacy probabilistic surfaces carry eight common provenance fields:
`artifact_id`, `model_name`, `model_version`, `source_snapshot_id`, `method`,
`observed_status`, `confidence_status`, and `weak_identification_flag`.
A posterior mean or HDI is conditional on the fitted model. Donor dispersion,
a transported interval, or a neutral fallback in the PBP layer is a different
uncertainty summary and must retain its own method label. The PBP layer is not
uniformly Bayesian and does not promise calibrated intervals for every target.

The September 11 evidence contract, gate version 3, reports six dimensions:
numerical, predictive, calibration, transport, identification, and provenance.
Each dimension can pass, fail, or remain unsupported. Default validated
publication requires current numerical, predictive, calibration, and provenance
passes. Even that status leaves transport and identification as separate
judgments. Finite metrics and positive evaluated sample counts are required;
a missing comparator, missing calibration evidence, or incomplete fitted-stage
lineage cannot be treated as a pass.

An explicit exploratory publication mode permits a research pointer with failed
or unsupported evidence and a recorded reason. It preserves the failed result.
The September 13 migration creates 24 such pointers bound to retained legacy
payloads; all have failed overall reports and unsupported provenance. Nineteen
missing prior-predictive files and other lineage gaps remain disclosed. A strict
pointer check passing means that this exploratory status and its content binding
are internally consistent. It does not mean the model passed validation.

The older paper reported version-2 `passed` labels after the September 4
restatement. Those labels describe a historical materialization under an older
policy. Updating validation code does not revise rows already stored in
production. The isolated candidate's ten populated legacy statistical families
carry failed confidence; two optional statistical surfaces remain typed empty.
This revision therefore reports the policy version, artifact generation, and
consumer environment whenever it reports a verdict.

For the full PBP candidate, publication checks enforce exact population
coverage, field mappings, artifact identities, normalization, and pitch and
fielding reconciliation. All sixteen PBP surfaces passed their structural audits in an isolated
candidate schema. Explicit conflicts remain in the data; checks require their disclosure
rather than erasing them. The publisher repeats the relevant checks against
production consumers before creating the public catalog. No production
promotion or public upload of this candidate had occurred at the paper's cutoff.

Official tables retain their meaning. Probabilistic legacy outputs remain
additive siblings of deterministic tables, and the rough PBP reconstructions are
another explicit interface. Unknown people are represented by candidate support
or unresolved slots, never fabricated person identifiers. Unidentified physical
or scoring quantities may be withheld, while practical proxies can be supplied
under declared assumptions. Neither a selected class nor a narrow conditional
interval is permission to present an unrecorded value as an observed fact.
