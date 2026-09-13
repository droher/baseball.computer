- Pitches
    - A no-play or baserunning play event may have a sequence but does not have to.
    - A plate appearance event must have a sequence and must have a pitch as part of that sequence.
    - The number of balls in the count must equal the number of balls in the sequence prior to the final pitch.
    - Strikes are the same except that an unlimited number of fouls are allowed if there are 2 strikes in the count.
- Fielding
  - Any out must have a putout.

## Standings daily snapshots: 2026-09-12

Standings now accumulates completed regular-season games before joining the date
spine. Each date selects its final game explicitly, and off days inherit that
state. Last-ten records use ten completed game rows, including ties, ordered by
completion date, season game number, and game ID. Streaks come from the selected
team-game result.

The previous calendar-row windows zeroed last-ten records on 138,873 off-day
snapshots and omitted a game result on 34,779 multiple-game days in the local
1871–2025 database. SQLGlot 30.4.3 also removed `IGNORE NULLS` from the windowed
`LAST` expression, causing off-day streaks to reset. Computing game states before
the calendar join removes that expression entirely.

Validation compared all 593,699 snapshots with an independent game replay across
22 cumulative and recent-form fields, with zero mismatches and no change to row
count or grain. All 3,336 final team-season snapshots retain a nonzero last-ten
decision count. SQLMesh rendered and built the model in an isolated database
copy. Regression coverage includes off days, doubleheaders, tripleheaders,
suspended-game ordering, repeated game numbers, ties, and team/season isolation.

Verified corrections include the 2025 Yankees on September 29 (94–68, last ten
9–1, eight-game winning streak) and Cleveland on September 20 (84–71 after both
doubleheader wins, last ten 10–0, ten-game winning streak).
