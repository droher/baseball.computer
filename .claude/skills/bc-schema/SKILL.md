---
name: bc-schema
description: Use when answering natural-language questions about the baseball.computer database (bc.db) — finding tables, columns, metrics, joins, enums, ambiguities, or verified example queries; writing SQL against main_models.* / main_seeds.* / the BSL semantic tables (offense_seasons, pitching_seasons, fielding_seasons, offense_events, pitching_events, fielding_events); resolving baseball jargon (HR, AVG, OBP, ERA, OPS, ISO, etc.) to specific columns or metrics; understanding which tables back which semantic views; or checking project rules and warnings about non-additive rate stats. Trigger on any baseball-data NL→SQL request even when the user does not name a specific table. Reads selectively from `docs/llm/baseball.lsf` (LSF-1 format, ~7k one-line records) using grep — never load the whole file into context.
allowed-tools: Read, Grep, Glob, LS, Bash
---

# bc-schema

Retrieval surface for the LSF-1 schema/semantics packet at
`docs/llm/baseball.lsf`. Each record in that file is one line, so `grep`
returns exactly the rows you want; load only matched records into context.

## Bootstrap

If `docs/llm/baseball.lsf` is missing, regenerate it:

```sh
just gen-llm-context
```

(Read-only against prod `bc.db`; takes ~10s.) The file is gitignored; every
fresh checkout needs the bootstrap.

The spec for the format itself is `docs/llm/lsf_1_spec.md` — consult it
when a record's field layout is unclear, but most fields are obvious from
context.

## How to use it

Pick the recipe that matches the question, run the `grep`, drop the
matched lines into your reasoning. **Never `cat` the whole file** —
6.7k lines blows the context budget for no benefit.

The pipe-delimited columns by record type:

| Record   | Columns |
|----------|---------|
| `TABLE`  | id, physical_name, grain, description, synonyms |
| `COL`    | table_id, name, type, role, ref, desc, examples, tags |
| `METRIC` | id, name, table_id, expr, agg, time_dim, filters, allowed_dims, desc, tags |
| `REL`    | id, src_col, dst_col, cardinality, fanout, join_template, desc |
| `ENUM`   | id, column_ref, value, label/payload, tags |
| `VQ`     | id, question, notes (the SQL body lives in a following `SQL\|<id><<` … `>>SQL` block) |
| `AMBIG`  | term, meanings (`;`-list), default, clarify_when |
| `RULE`   | severity (must/should/warn), scope, text |
| `WARN`   | subject_id, text |

Roles on `COL`: `PK` / `DIM` / `MEASURE` / `DERIVED` / `TIME` / `FK`.
`MEASURE` is additive; `DERIVED` and ratio metrics are not — see the WARNs.

## Recipes

Set this once per session if you want to skip retyping the path:

```sh
LSF=docs/llm/baseball.lsf
```

### find a table by name
```sh
grep -i '^TABLE|.*<keyword>' "$LSF"
```

### all columns for a table
```sh
grep '^COL|<table_id>|' "$LSF"
```
The `<table_id>` is the id from the TABLE row (e.g. `table.offense_seasons`,
`table.main_models_player_team_season_offense_stats`).

### find a metric by name
```sh
grep -i '^METRIC|.*<keyword>' "$LSF"
```
Returns the formula in the `expr` field. For ratios it is already
`sum(num) / nullif(sum(den), 0)`-shaped — copy into SQL directly.

### check whether a metric is safe to aggregate
```sh
grep '^WARN|<metric_id>|' "$LSF"
```
If a WARN exists, the metric is non-additive (rate / derived) — recompute
from counting stats when grouping; do not `SUM` or `AVG` the stored value.

### enum values for a column
```sh
grep '^ENUM|.*<column_ref>' "$LSF"
```
e.g. `grep '^ENUM|.*game_types\.game_type' "$LSF"` for the game-type taxonomy.

### relationships from / to a table
```sh
grep '^REL|.*<table_id>\.' "$LSF"           # both directions
grep '^REL|[^|]*|<table_id>\.' "$LSF"        # outbound (src side)
grep -E "^REL\|[^|]*\|[^|]*\|<table_id>\." "$LSF"   # inbound (dst side)
```

### verified queries matching a keyword (and their SQL)
```sh
grep -i '^VQ|.*<keyword>' "$LSF"
```
For the SQL body of `vq.foo`, use `awk` to pull the block:
```sh
awk '/^SQL\|vq\.foo<<$/{flag=1;next} /^>>SQL$/{flag=0} flag' "$LSF"
```

### dump all rules
```sh
grep '^RULE|' "$LSF"
```

### resolve an ambiguous term
```sh
grep -i '^AMBIG|<term>|' "$LSF"
```
Pipe-2 is `;`-delimited possible meanings; pipe-3 is the default.

### domain / coverage / timezone (one-time orient)
```sh
grep -E '^(DOMAIN|TZ|COVERAGE)\|' "$LSF"
```

## Standing rules baked into this DB

The full list lives in the `RULE|` records — grep them at the start of any
non-trivial NL→SQL session. Highlights:

- `must` — read-only `SELECT` against `bc.db`; never reference columns not
  present in the LSF.
- `should` — prefer the BSL semantic tables (`offense_seasons`,
  `pitching_seasons`, …) over `main_models.metrics_*` directly; they apply
  consistent league joining and regular-season filtering.
- `should` — prefer published `main_models.*` over staging `stg_*`.
- `should` — for rate stats (BA, OBP, SLG, ERA, WHIP, …), recompute from
  counting stats when grouping. Never `SUM`/`AVG` a stored rate column.
- `warn` — season-grain BSL tables filter to regular-season `game_type` by
  default; pass `game_type` explicitly when postseason data is wanted.
- `warn` — event-grain data is sparse before 1912; filter `season >= 1912`
  unless the question targets the early era.

## Tips

- BSL semantic tables (`table.offense_seasons` etc.) are **virtual** — the
  `physical_name` field is the principal backing model, but the BSL view
  also joins league from `main_seeds.seed_franchises` and applies a
  game-type filter. If you write SQL directly against the physical model,
  apply those join+filter steps yourself.
- `metric.<name>_<kind>_<grain>` IDs are the canonical metric handles; the
  bare measure column on a BSL table has the same name without the
  suffix.
- When unsure which of two same-named columns to use (e.g. `home_runs` on
  offense vs pitching), check `AMBIG` first.
- The validator is part of the generator: if you ever see a malformed
  packet, run `just gen-llm-context` (or
  `uv run --group build python scripts/generate_llm_context.py --validate`)
  to regenerate and structurally re-check it.
