## A taxonomy of missingness

Missingness assumptions must specify a population and conditioning set.
Assigning a single field-wide label of missing completely at random, missing at
random, or missing not at random is inadequate for this record. A single event can be complete for basic
batting, treated under an MAR working model for trajectory given scorer and result,
exposed to an MNAR risk for hit location, structurally absent for pitch sequence, and
merely aggregate-only for fielding credit, all at the same time. The unit that
carries a mechanism is the event-dimension, not the field and not the row, and
the mechanism is a statement about a process, not a flag.

Two coupled generative processes produce every value. The baseball process
generates the latent state: a ball has a latent contact class and geometry,
fielders handle it, runners advance, runs score, and an official scorer assigns
credit. The observation process generates the record: a source, scorer,
inputter, translator, and parser record or omit parts of that state
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Written as a schema, the latent baseball state maps to official credit and
outcome, and the latent state together with the scorer-and-source process maps
to the recorded label or its absence
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Most heuristics blur these two maps; separating them is the point of the
taxonomy.

Under that separation, ten missingness classes recur across the record, each
with its own detector and its own defensible imputation family
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

| Class | What it means |
|---|---|
| Structural absence | The target grain does not exist though a coarser one does — no event rows for a gamelog-only game. |
| Source-family block absence | The grain exists for some games but a whole source family or file block was never acquired. |
| Aggregate-only coverage | An official or parser aggregate exists but event attribution does not. |
| Field-level unknown | The event exists but a parsed field is `Unknown`, `0`, `Default`, or null. |
| Selection-biased detail | Detail is recorded only for non-random subsets, keyed to result salience or scorer habit. |
| Cross-source disagreement | Event-derived and box-derived totals conflict. |
| State reconstruction gap | The event exists but the state needed to interpret it is derived indirectly. |
| Official scoring convention gap | The raw facts are known but official credit follows a scoring convention, not event logic. |
| Taxonomy collapse | Raw codes exist but the wanted category is a coarser, more stable recode. |
| Sparse-context estimation | Detail exists but the conditioning bucket for adjustment is thin. |

<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->

These classes call for different responses, and conflating them destroys the
model. Structural absence and aggregate-only coverage are not imputation
targets in the event namespace at all; they stay at aggregate grain with source
flags. Taxonomy collapse is deterministic recode, not inference. Cross-source
disagreement requires an explicit authority rule or a retained conflict, rather
than a fill that silently discards contrary evidence. Field-level unknowns
take deterministic inference first, empirical priors second, and model
predictions last, always preserving the raw and imputed values separately. The
sentinels themselves must stay distinct: null, `Unknown`, `Default`, `0`,
not-applicable, aggregate-only, and known-source-issue are different markers, and
flattening them into one missing indicator discards the missingness model
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

Every imputed value therefore carries companion facts for source, method,
observed status, and confidence or weight; without them a downstream aggregation
cannot tell an official total from a deterministic derivation from a posterior
draw from a block-missing gap
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

The default working assumption for the observation models is missingness at
random conditional on context — source, scorer, inputter, translator, result,
hit-or-out state, leverage, era, and base-out state
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
Two dimensions are flagged as risks to that assumption. Hit location and detailed
contact type are treated as missing-not-at-random risks, because whether the
label was recorded may depend on what the label would have been
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
These are the selection-biased-detail class in its sharpest form, and they are
handled not by an MAR fill but by pattern-mixture sensitivity — letting the
missing values take distributions shifted from the observed ones within bounded
plausibility — the subject of the section on selection that never recorded
itself. A hierarchical model cannot rescue an unidentified estimand: where
scorer, park, team, source, and era are inseparable in a slice, the output is
tagged weakly identified, withheld, or supplied only as an explicit
assumption-based proxy
<!-- src: notes/data-coverage-implementation/statistical-modeling-coverage-design.md -->.
