# Negro Leagues batting: Lahman vs Retrosheet

This note compares the current Lahman release on R2 (`baseballdatabank/batting`, `people`, `teams`) with Retrosheet's `negroleagues.zip` (`nlb_stats`) for Negro Leagues batting only. MLB rows are excluded throughout. It does not cover pitching or fielding. The data was pulled 2026-10-13.

The two are separate research efforts:
- Lahman's Negro Leagues data is one season line per player and team.
- Retrosheet's is game-level lines, sourced from individual box scores and play-by-play.

## How to read the comparison

Two facts change the raw numbers a lot.

**1. Retrosheet games without a box score carry no batting stats.** Of the 5,214 Retrosheet regular-season games with player lines:

| Tier | Games | AB known |
|---|---|---|
| Box score with play-by-play | 1,406 | 100% |
| Box score only | 1,864 | 100% |
| No box score (appearances only) | 1,944 | 3% |

A raw count of Retrosheet games overstates its stat coverage. Every comparison below uses only games with a box score.

**2. Lahman's season stats include games Retrosheet calls exhibitions.**
- Lahman's team standings (G/W/L) and its batting totals cover different game sets. 1941 Baltimore Elite Giants: the team has 41 games, while one player has 60 and the team has 2,028 AB.
- Lahman's batting lines match Retrosheet once Retrosheet's exhibitions between two clubs that Lahman also lists for that season ("major-club exhibitions") are added in:

| Season | Lahman AB | Retro regular AB | Retro major-club exhibition AB | Lahman ÷ (regular + major-club exhibitions) |
|---|---|---|---|---|
| 1933 | 11,267 | 9,913 | 1,671 | 0.97 |
| 1935 | 12,454 | 14,427 | 783 | 0.82 |
| 1937 | 16,503 | 16,913 | 1,733 | 0.89 |
| 1940 | 16,267 | 12,312 | 4,061 | 0.99 |
| 1941 | 15,100 | 10,577 | 3,934 | 1.04 |
| 1942 | 16,207 | 10,656 | 6,145 | 0.96 |
| 1944 | 14,693 | 14,036 | 2,468 | 0.89 |
| 1946 | 14,945 | 14,552 | 2,417 | 0.88 |
| 1948 | 14,270 | 14,223 | 1,231 | 0.92 |

Exhibitions against other opponents (barnstorming and semipro clubs) are small, under 900 AB in any season, and do not seem to be in Lahman. "Matched scope" below means Retrosheet regular-season games plus major-club exhibitions, box scores only.

## Coverage by era

| Era | Lahman player-seasons | Lahman AB | Retrosheet player-seasons (matched scope) | Retrosheet AB |
|---|---|---|---|---|
| 1891–1919 | 1,959 | 84,091 | 49 | 213 |
| 1920–1932 | 4,440 | 352,719 | 1,161 | 7,331 |
| 1933–1948 | 5,038 | 237,306 | 5,871 | 259,890 |
| 1949+ | — | — | 233 | 10,423 |

- **Before 1933, only Lahman has meaningful coverage.** Retrosheet has about 2% of Lahman's at-bats for 1920–32, and no other Retrosheet file fills the gap: the event and box-score files have under 200 regular-season Negro Leagues team-games before 1930.
- **1933–48:** both sources have coverage, and Retrosheet is about 10% larger overall.
- **1949 (NAL):** only Retrosheet has data, because Lahman stops at 1948.

## 1933–48 by team-season (matched scope)

| Lahman vs Retrosheet AB | Team-seasons | Lahman AB | Retro AB |
|---|---|---|---|
| Within 10% | 126 | 166,663 | 170,899 |
| Retrosheet more than 10% higher | 55 | 61,825 | 75,459 |
| Lahman 10–50% higher | 7 | 7,191 | 6,062 |
| Lahman more than 50% higher | 3 | 1,445 | 916 |
| Lahman only | 2 | 845 | — |

Team-seasons where Lahman has more than 10% more AB:

| Season | Team | Lahman AB | Retro AB | Ratio |
|---|---|---|---|---|
| 1933 | Kansas City Monarchs | 184 | — | — |
| 1934 | Homestead Grays | 661 | — | — |
| 1935 | Kansas City Monarchs | 438 | 268 | 1.63 |
| 1935 | New York Black Yankees | 426 | 269 | 1.58 |
| 1936 | Chicago American Giants | 581 | 379 | 1.53 |
| 1942 | Jacksonville Red Caps | 473 | 330 | 1.43 |
| 1941 | Baltimore Elite Giants | 2,028 | 1,671 | 1.21 |
| 1942 | Cincinnati-Cleveland Buckeyes | 1,495 | 1,234 | 1.21 |
| 1942 | Chicago American Giants | 1,472 | 1,290 | 1.14 |
| 1939 | Indianapolis ABCs / Atlanta Black Crackers | 724 | 639 | 1.13 |
| 1941 | Memphis Red Sox | 663 | 594 | 1.12 |
| 1933 | Akron Grays / Cleveland Giants | 336 | 304 | 1.11 |

The 1933 Monarchs and 1934 Grays were independent that season. Their Retrosheet games are all exhibitions against clubs Lahman does not list, so they fall outside matched scope.

## 1933–48 by player-season (matched scope)

| Category | Player-seasons | Lahman AB | Retro AB |
|---|---|---|---|
| Both, Retrosheet more AB | 2,206 | 149,548 | 170,509 |
| Both, same AB | 1,132 | 18,800 | 18,800 |
| Both, Lahman more AB | 1,003 | 59,076 | 54,159 |
| Retrosheet only | 762 | — | 10,637 |
| Lahman only | 697 | 9,882 | — |

**Most single-source player-seasons are ID-linkage gaps, not missing players.**
- 355 of the 1,492 Lahman people with 1933–48 Negro Leagues rows have no `retro_id`.
- 546 of the 697 Lahman-only player-seasons (7,979 of 9,882 AB) match a Retrosheet-only player by last name, team, and season, with similar AB. Examples: Ted Strong (`stronte01` / `strot101`), Lázaro Salazar, Curt Roberts, Sylvester Owens.
- Only 58 Retrosheet-only player-seasons have 50 or more AB.

The current `stg_people` join uses Lahman's `retro_id`, so these players would not link without a crosswalk fix.

**Where both sources have the same AB, other stats still disagree.** Of the 1,064 player-seasons with equal, nonzero AB:

| Stat | Player-seasons that differ | Share |
|---|---|---|
| H | 109 | 10% |
| 2B | 73 | 7% |
| HR | 11 | 1% |
| BB | 273 | 26% |
| G | 113 | 11% |

Equal AB with different hits or walks points to different box-score sources or different reconciliation for the same game. It is not a coverage difference.

## Crosswalk issues found

**Lahman `teams.team_id_retro` disagrees with the team ids in the Retrosheet NLB file:**

| Season | Lahman team | Lahman `team_id_retro` | Retrosheet NLB id |
|---|---|---|---|
| 1935 | CEG | CB3 | NSH |
| 1942 | CCB | CCA | CCB |
| 1946–47 | CIC | CIA | IN9 |

**54 Lahman Negro Leagues team-seasons have no `team_id_retro`.** They are mostly pre-1920 IND, EAS, and WES clubs, plus 1931 CC2 and 1932 COT. `stg_databank_batting` and `stg_databank_pitching` have a `not_null(team_id)` audit, so these fail the build.

**People:** 355 Lahman Negro Leagues people from 1933–48 have no `retro_id`; most can be matched by name, team, and season (see above).

## Summary

- **1891–1932:** Lahman is effectively the only source.
- **1949:** Retrosheet is the only source.
- **1933–48:** Retrosheet has more at-bats overall and in most team-seasons. Lahman is larger in about 10 team-seasons, mostly 1935–36 and 1941–42. Where both have a player-season, they agree on at-bats in only about a quarter of cases. Even with equal at-bats, hits differ 10% of the time and walks 26%.
- **Scope difference:** Retrosheet's regular season is narrower than Lahman's season. Matching Lahman means adding major-club exhibitions, which changes what "regular season" means in this warehouse for these years.
