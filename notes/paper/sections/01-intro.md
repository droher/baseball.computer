## Introduction

A historical play-by-play account can identify the batter, the play result, and
the change in base-out state while omitting the pitches, the ball's trajectory,
or the fielder who made an out. These omissions limit comparisons across eras:
a statistic computed only where detail survives describes a selected part of
the archive. An estimate can make the remaining record usable, but its meaning
depends on why the detail is missing and on the assumptions used to fill it.

This paper studies 18,141,020 events in 205,886 acquired play-by-play (PBP) games
from 1903 through 2025. Of these, 119,335 games and 10,308,542 events precede
1989. The earliest decade contributes only 41 games; the calendar span is not a
claim of complete historical acquisition. We target fields on actual PBP rows.
Reconstruction from box scores or season totals is a separate problem because
it requires inventing the event spine as well as its missing details.

Two coupled processes produce the archive. The game generates a trajectory,
location, sequence of pitches, and sequence of fielding actions. Scorers,
sources, translators, and parsers determine which aspects become recorded data.
A source may omit a label precisely when another part of the account makes it
redundant. In the historical geometry dataset, the pre-1950 unrecorded slice
contains 763,993 ground balls deducible from fielding strings, compared with
712,469 explicitly recorded trajectories of all classes. The derived cases
impose a lower bound on the unrecorded ground-ball share; they do not make that
slice a random sample or identify its remaining composition. Section 5 develops
this argument, and Section 7 identifies the dated evidence behind the counts.

The work has two connected parts. The first is a family of hierarchical
coverage models for observation propensity, geometry, fielding credit, handlers,
park factors, run expectancy, transitions, and pitch summaries. These models
make assumptions and uncertainty explicit, but the available experiments do not
establish historical identification or calibrated reconstruction across eras.
The second is an additive imputation layer that covers the acquired PBP schema
using those retained estimates where available and declared rule-based,
empirical, transported, or neutral fallbacks elsewhere. This second part accepts
rough estimates while requiring their estimated status to remain visible.

The contributions are a taxonomy at event-dimension grain; a selection-model
sensitivity formulation that separates lower bounds from point identification;
a full-history PBP field registry and imputation implementation; and an evidence
contract that separates numerical convergence, predictive performance,
calibration, transport, identification, and provenance. The results include
failed and unsupported evidence. Passing a conservation check, or successfully
materializing a database table, does not turn an assumption into an observation.

The manuscript's evidence cutoff is September 13, 2026. Earlier fit summaries
are retained as dated research observations. The new PBP surfaces and migrated
legacy wrappers have been exercised in isolated database copies; production
promotion and public publication of this candidate remain pending. Throughout,
"imputed" describes estimated data, including rough reconstructions, and
"observed" is reserved for recorded source evidence.
