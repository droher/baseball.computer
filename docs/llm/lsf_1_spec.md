# LSF-1: LLM Schema Format v1.0

**Status:** Draft 1  
**Intended use:** Compact database and analytics-context serialization for LLM prompt input  
**Version:** 1.0  
**Canonical short name:** LSF-1  
**File extension:** `.lsf` or `.dbctx`  
**MIME type suggestion:** `text/vnd.lsf+plain; version=1`

---

## 1. Abstract

LSF-1, or **LLM Schema Format v1**, is a compact, line-oriented serialization for supplying large language models with database context for analytics query generation, schema linking, and natural-language-to-SQL systems.

LSF-1 is designed for **LLM prompt consumption**, not human-friendly authoring. It favors predictable structure, low syntactic overhead, explicit semantic metadata, and deterministic parsing. It is intended to represent database tables, columns, relationships, metrics, enumerations, ambiguity notes, generation rules, and verified SQL examples.

LSF-1 is not a replacement for a warehouse catalog, semantic layer, dbt project, LookML model, or database DDL. It is a prompt-time interchange/rendering format that can be generated from those sources.

---

## 2. Design Goals

LSF-1 has the following goals:

1. **LLM-efficient context**  
   Minimize punctuation and redundant field names while preserving enough structure for reliable schema linking.

2. **Explicit analytics semantics**  
   Represent grain, primary keys, foreign keys, relationships, dimensions, measures, metrics, filters, caveats, and verified queries as first-class data.

3. **Deterministic parsing**  
   Enable simple line-by-line parsing without requiring JSON, YAML, XML, or indentation-sensitive parsing.

4. **Retrieval friendliness**  
   Support object-level chunking and selective rendering of only the relevant database context.

5. **Prompt composability**  
   Allow LSF-1 blocks to be embedded safely inside larger prompts using explicit outer tags.

6. **Vendor neutrality**  
   Support common relational and analytical warehouses while allowing dialect-specific type annotations.

---

## 3. Non-Goals

LSF-1 does not attempt to:

1. Fully describe all database constraints, indexes, grants, storage details, or physical execution properties.
2. Replace SQL DDL.
3. Replace a production semantic layer.
4. Guarantee SQL correctness by itself.
5. Serve as the required output format from an LLM.
6. Optimize for direct analyst authoring.
7. Encode arbitrary nested objects.
8. Represent all possible graph or ontology structures.

For LLM output, systems SHOULD use JSON Schema, function calling, tool calls, or another constrained output mechanism.

---

## 4. Terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, **MAY**, and **OPTIONAL** are to be interpreted as described in RFC 2119.

**Context packet**  
A complete LSF-1 document rendered for an LLM prompt.

**Object**  
A named semantic item, such as a table, column, relationship, metric, enum value, rule, or verified query.

**Object ID**  
A stable identifier used to reference an object inside an LSF-1 context packet.

**Physical name**  
The warehouse/database name of an object, such as `mart_finance.fct_orders`.

**Semantic name**  
The logical name exposed to the LLM, such as `table.orders` or `metric.net_revenue`.

**Grain**  
The level of detail represented by one row in a table or model.

**Metric**  
A governed analytical measure with a definition, base table, aggregation behavior, default time column, default filter, allowed dimensions, and optional warnings.

**Verified query**  
A canonical SQL example known to be correct for a representative natural-language question.

---

## 5. Document Structure

An LSF-1 context packet MUST be enclosed in a single `DB_CONTEXT` wrapper:

```text
<DB_CONTEXT version="LSF-1" dialect="duckdb">
...
</DB_CONTEXT>
```

The opening wrapper MUST appear on the first non-empty line. The closing wrapper MUST appear on the last non-empty line.

The wrapper attributes are:

| Attribute | Required | Description |
|---|---:|---|
| `version` | Yes | MUST be `LSF-1` for this version. |
| `dialect` | Yes | SQL dialect, such as `duckdb`, `snowflake`, `bigquery`, `postgres`, `redshift`, `databricks`, `trino`, or `mysql`. |

The content inside the wrapper consists of metadata records and named sections.

A valid LSF-1 document has the following high-level order:

```text
<DB_CONTEXT version="LSF-1" dialect="{dialect}">
DOMAIN|{domain_id}|{description}
TZ|{iana_timezone}
COVERAGE|{coverage_summary}

TABLES
...

RELATIONSHIPS
...

METRICS
...

ENUMS
...

AMBIGUITIES
...

RULES
...

VERIFIED_QUERIES
...
</DB_CONTEXT>
```

The sections `TABLES` and `RELATIONSHIPS` are REQUIRED. Other sections are OPTIONAL unless required by local conformance profiles.

The metadata records `DOMAIN`, `TZ`, and `COVERAGE` MUST appear before the first section header. A `CURRENCY` record MAY appear in the same metadata block when the domain has monetary metrics; it is omitted otherwise.

---

## 6. Encoding and Lexical Rules

### 6.1 Character Encoding

Documents MUST be encoded as UTF-8.

### 6.2 Line Endings

Line endings MUST be normalized to `\n` before parsing.

### 6.3 Whitespace

Leading and trailing whitespace around complete lines SHOULD be ignored except inside SQL literal blocks.

Field values MUST NOT rely on leading or trailing spaces for meaning.

### 6.4 Empty Lines

Empty lines MAY appear between records and sections. Parsers SHOULD ignore empty lines except inside SQL literal blocks.

### 6.5 Comments

Lines beginning with `#` are comments and SHOULD be ignored by parsers.

Comments MUST NOT appear inside SQL literal blocks unless they are intended to be part of the SQL.

### 6.6 Field Separator

The pipe character `|` is the field separator for all regular records.

### 6.7 Null Values

A single hyphen `-` denotes null, unavailable, unknown, not applicable, or intentionally omitted.

### 6.8 Escaping

The following escape sequences MUST be supported inside fields:

| Escape | Meaning |
|---|---|
| `\|` | Literal pipe character |
| `\\` | Literal backslash |
| `\n` | Literal newline |
| `\;` | Literal semicolon inside a list value |

A parser MUST interpret escape sequences after splitting the record into fields.

A writer MUST escape literal `|` and `\` characters inside fields.

A writer SHOULD escape literal semicolons inside list items.

### 6.9 Lists

List-valued fields use semicolon-separated items:

```text
enterprise;mid_market;smb
```

An empty list MUST be represented as `-`.

### 6.10 Literal Blocks

LSF-1 supports SQL literal blocks only in the `VERIFIED_QUERIES` section.

A SQL literal block begins with:

```text
SQL|{vq_id}<<
```

and ends with:

```text
>>SQL
```

All content between these markers MUST be preserved exactly, including whitespace and comments.

---

## 7. Identifiers

### 7.1 Object IDs

Object IDs MUST be stable and SHOULD be lowercase.

Object IDs SHOULD follow this pattern:

```text
[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*
```

Recommended prefixes:

| Object type | Prefix example |
|---|---|
| Domain | `domain.revenue` |
| Table | `table.orders` |
| Column | `table.orders.order_date` or `col.orders.order_date` |
| Relationship | `rel.orders_customers` |
| Metric | `metric.net_revenue` |
| Enum | `enum.order_status` |
| Verified query | `vq.revenue_by_month` |

### 7.2 Column References

Column references in LSF-1 SHOULD use:

```text
{table_id}.{column_name}
```

Example:

```text
table.orders.customer_id
```

This avoids requiring every column to have a separate explicit object ID.

### 7.3 Physical Names

Physical names MAY use dialect-specific casing and quoting conventions, but SHOULD be emitted without quotes unless required to disambiguate.

Examples:

```text
mart_finance.fct_orders
analytics.public.orders
`project.dataset.table`
```

---

## 8. Metadata Records

### 8.1 DOMAIN

The `DOMAIN` record defines the subject area of the context packet.

Syntax:

```text
DOMAIN|{domain_id}|{description}
```

Example:

```text
DOMAIN|domain.baseball|Major-league play-by-play, season, and career analytics.
```

A document MUST contain exactly one `DOMAIN` record.

### 8.2 TZ

The `TZ` record defines the default timezone for time filtering, bucketing, and relative dates.

Syntax:

```text
TZ|{iana_timezone}
```

Example:

```text
TZ|America/New_York
```

A document SHOULD contain exactly one `TZ` record. If omitted, consumers SHOULD assume the database/session default timezone.

### 8.3 COVERAGE

The `COVERAGE` record summarizes the temporal/breadth coverage of the data. Its purpose is to give the LLM a one-line caveat about era or scope so individual `WARN` records do not have to repeat it.

Syntax:

```text
COVERAGE|{coverage_summary}
```

Example:

```text
COVERAGE|Event-level play-by-play 1910+ (sparse 1871-1909); season/career stats 1871+; biographical data 1871+.
```

A document SHOULD contain at most one `COVERAGE` record. Use `-` when not applicable.

### 8.4 CURRENCY

The `CURRENCY` record defines the default currency for monetary metrics.

Syntax:

```text
CURRENCY|{iso_currency_or_-}
```

Example:

```text
CURRENCY|USD
```

A document MAY contain at most one `CURRENCY` record. Producers SHOULD omit the record entirely when the domain has no monetary metrics.

---

## 9. TABLES Section

The `TABLES` section defines logical tables or models and their columns.

The section begins with:

```text
TABLES
```

### 9.1 TABLE Record

Syntax:

```text
TABLE|{table_id}|{physical_name}|{grain}|{description}|{synonyms}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `table_id` | Yes | Stable table object ID. |
| `physical_name` | Yes | Fully qualified or otherwise resolvable warehouse table name. |
| `grain` | Yes | One-row meaning of the table. |
| `description` | Yes | Short semantic description. |
| `synonyms` | No | Semicolon-separated alternative names. |

Example:

```text
TABLE|table.orders|mart_finance.fct_orders|one row per order_id|Customer order fact table.|orders;purchases;transactions
```

### 9.2 COLS Header Record

A `COLS` record declares the fixed field order used by following `COL` records for a table.

Syntax:

```text
COLS|{table_id}|name|type|role|ref|desc|examples|tags
```

In LSF-1, the column field order MUST be exactly:

```text
name|type|role|ref|desc|examples|tags
```

The `COLS` record is included to help the LLM interpret tuple rows and to support future versioning.

A `TABLE` record MUST be followed by exactly one `COLS` record before any `COL` records for that table.

### 9.3 COL Record

Syntax:

```text
COL|{table_id}|{name}|{type}|{role}|{ref}|{desc}|{examples}|{tags}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `table_id` | Yes | ID of the parent table. |
| `name` | Yes | Column name, preferably physical column name. |
| `type` | Yes | Normalized type with optional dialect annotation. |
| `role` | Yes | Column role or `+`-separated roles. |
| `ref` | Conditional | Referenced column for foreign keys; otherwise `-`. |
| `desc` | Yes | Short semantic description. |
| `examples` | No | Semicolon-separated sample values. |
| `tags` | No | Semicolon-separated metadata tags. |

Example:

```text
COL|table.orders|customer_id|string|FK|table.customers.customer_id|Billing customer id.|-|-
```

### 9.4 Column Roles

Allowed role tokens are:

| Role | Meaning |
|---|---|
| `PK` | Primary key. |
| `FK` | Foreign key. |
| `TIME` | Time dimension. |
| `DIM` | Dimension usable for grouping/filtering. |
| `MEASURE` | Numeric measure suitable for aggregation. |
| `ATTR` | Descriptive attribute, usually not grouped by by default. |
| `DERIVED` | Computed or derived column. Includes pre-computed rate/ratio measures (e.g. batting average, OBP) that MUST NOT be summed or naively averaged across rows. |
| `SYSTEM` | Technical/system column. |
| `PII` | Personally identifiable information. |

A column representing an already-computed rate (e.g. on-base percentage stored on a season-level table) SHOULD use `DERIVED`, not `MEASURE`, because naive aggregation is incorrect: the LLM must treat them as atoms or rebuild them from underlying counts. Counting stats (hits, plate appearances) stay `MEASURE`.

Multiple roles MUST be joined with `+` and no spaces:

```text
DIM+PII
```

### 9.5 Type Values

Normalized type values SHOULD be one of:

```text
string
int
float
number
boolean
date
timestamp
time
json
array
geography
variant
```

Dialect-specific annotations MAY be included using angle brackets:

```text
number<NUMBER(38,2)>
timestamp<TIMESTAMP_NTZ>
string<VARCHAR>
```

### 9.6 Column Examples

Column examples SHOULD be small. Writers SHOULD include no more than 10 examples per column.

For large categorical domains, writers SHOULD use the `ENUMS` section instead of overloading the `examples` field.

---

## 10. RELATIONSHIPS Section

The `RELATIONSHIPS` section defines join relationships between columns.

The section begins with:

```text
RELATIONSHIPS
```

The section MUST exist, even if no relationships are known.

### 10.1 REL Record

Syntax:

```text
REL|{rel_id}|{left_col}|{right_col}|{cardinality}|{fanout}|{join_hint}|{description}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `rel_id` | Yes | Stable relationship ID. |
| `left_col` | Yes | Left-side column reference. |
| `right_col` | Yes | Right-side column reference. |
| `cardinality` | Yes | Relationship cardinality. |
| `fanout` | Yes | Whether the join preserves the likely analysis grain. |
| `join_hint` | No | Suggested SQL join predicate. |
| `description` | No | Human-language caveat or semantic explanation. |

Example:

```text
REL|rel.orders_customers|table.orders.customer_id|table.customers.customer_id|many_to_one|safe|left join customers on orders.customer_id = customers.customer_id|Safe dimension join; preserves order grain.
```

### 10.2 Cardinality Values

Allowed values:

```text
one_to_one
one_to_many
many_to_one
many_to_many
unknown
```

### 10.3 Fanout Values

Allowed values:

```text
safe
unsafe
conditional
unknown
```

A relationship SHOULD be marked `unsafe` if joining through it can duplicate rows of a common fact table.

A relationship SHOULD be marked `conditional` if it is safe only under pre-aggregation, filtering, or other constraints.

---

## 11. METRICS Section

The `METRICS` section defines governed analytical measures.

The section begins with:

```text
METRICS
```

### 11.1 METRIC Record

Syntax:

```text
METRIC|{metric_id}|{name}|{base_table}|{expr}|{agg}|{time_col}|{default_filter}|{allowed_dims}|{description}|{synonyms}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `metric_id` | Yes | Stable metric object ID. |
| `name` | Yes | Metric display/name token. |
| `base_table` | Yes | Table ID on which the metric is defined. |
| `expr` | Yes | SQL expression or semantic expression before aggregation. |
| `agg` | Yes | Aggregation behavior. |
| `time_col` | No | Default time column. |
| `default_filter` | No | Filter normally applied to this metric. |
| `allowed_dims` | No | Semicolon-separated allowed dimension column references. |
| `description` | Yes | Business definition. |
| `synonyms` | No | Semicolon-separated alternative names. |

Example:

```text
METRIC|metric.obp_offense_season|obp|table.offense_seasons|(sum(hits) + sum(walks) + sum(hbp)) / nullif(sum(plate_appearances) - sum(sac_hits),0)|ratio|table.offense_seasons.season|-|table.offense_seasons.player_id;table.offense_seasons.team_id;table.offense_seasons.season;table.offense_seasons.league;table.offense_seasons.game_type|On-base percentage: (H + BB + HBP) / (PA - SH).|on base;on-base;on base percentage;on-base pct
```

### 11.2 Aggregation Values

Allowed values:

```text
sum
avg
count
count_distinct
min
max
median
pXX
ratio
derived
none
```

For percentile metrics, use `pXX`, such as `p50`, `p90`, or `p99`.

For ratio metrics, the `expr` field SHOULD include the full aggregate expression where possible:

```text
METRIC|metric.batting_average_offense_season|batting_average|table.offense_seasons|sum(hits) / nullif(sum(at_bats),0)|ratio|table.offense_seasons.season|-|table.offense_seasons.player_id;table.offense_seasons.team_id;table.offense_seasons.season;table.offense_seasons.league|Batting average: hits divided by at-bats.|avg;ba
```

### 11.3 WARN Record

A `WARN` record attaches a caveat to a metric.

Syntax:

```text
WARN|{object_id}|{warning_text}
```

Example:

```text
WARN|metric.obp_offense_season|Non-additive: cannot SUM or AVG across rows. Recompute from underlying counting stats when grouping.
```

A `WARN` record MAY refer to a metric, table, relationship, or column.

---

## 12. ENUMS Section

The `ENUMS` section defines known values for categorical columns.

The section begins with:

```text
ENUMS
```

### 12.1 ENUM Record

Syntax:

```text
ENUM|{enum_id}|{column_ref}|{value}|{meaning}|{aliases}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `enum_id` | Yes | Stable enum object ID. |
| `column_ref` | Yes | Column to which this value belongs. |
| `value` | Yes | Literal value as stored or queried. |
| `meaning` | No | Business meaning. |
| `aliases` | No | Semicolon-separated natural-language aliases. |

Example:

```text
ENUM|enum.order_status|table.orders.status|completed|Order completed and eligible for revenue reporting.|complete;done
```

### 12.2 Enum Size Guidance

Writers SHOULD include high-value or ambiguous enum values, not exhaustive high-cardinality domains.

For a categorical column with many possible values, writers SHOULD include top values, business-critical values, and values likely to appear in user questions.

In the baseball.computer profile, the source for `ENUM` records is the small seed tables under `bc/seeds/` (game types, pitch types, plate-appearance result types — typically 5–50 rows each). Larger seed-driven taxonomies — `seed_franchises` is the canonical example, hundreds of rows — are surfaced as regular dimension `TABLE` records instead, so the LLM can join to them rather than memorize them.

---

## 13. AMBIGUITIES Section

The `AMBIGUITIES` section defines terms that may be ambiguous in natural-language questions.

The section begins with:

```text
AMBIGUITIES
```

### 13.1 AMBIG Record

Syntax:

```text
AMBIG|{term}|{possible_meanings}|{default_resolution}|{clarify_when}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `term` | Yes | Natural-language term. |
| `possible_meanings` | Yes | Semicolon-separated object IDs or descriptions. |
| `default_resolution` | No | Default object or interpretation. |
| `clarify_when` | No | Condition under which the model should ask a clarification question. |

Example:

```text
AMBIG|HR|metric.home_runs_offense_season;metric.home_runs_pitching_season|metric.home_runs_offense_season|Clarify when the question contrasts hitters and pitchers (e.g. "HR allowed").
AMBIG|average|metric.batting_average_offense_season;arithmetic mean|metric.batting_average_offense_season|Clarify when the question is about non-batting quantities (salaries, attendance).
```

---

## 14. RULES Section

The `RULES` section defines query-generation constraints and preferences.

The section begins with:

```text
RULES
```

### 14.1 RULE Record

Syntax:

```text
RULE|{severity}|{scope}|{rule_text}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `severity` | Yes | `must`, `should`, or `warn`. |
| `scope` | Yes | `global` or an object ID. |
| `rule_text` | Yes | Natural-language rule. |

Example:

```text
RULE|must|global|Generate read-only SELECT queries only.
RULE|should|global|Prefer the BSL semantic tables (offense_seasons, pitching_seasons, ...) over main_models.metrics_* directly; the semantic tables apply consistent league/regular-season filtering.
RULE|should|global|Prefer published main_models.* tables over staging stg_* tables.
```

### 14.2 Severity Values

| Severity | Meaning |
|---|---|
| `must` | Hard constraint. The model or system must obey. |
| `should` | Strong preference. The model should obey unless contradicted by the question. |
| `warn` | Caveat to surface or consider. |

Systems SHOULD enforce `must` rules outside the LLM where possible.

---

## 15. VERIFIED_QUERIES Section

The `VERIFIED_QUERIES` section defines canonical question-SQL examples.

The section begins with:

```text
VERIFIED_QUERIES
```

### 15.1 VQ Record

Syntax:

```text
VQ|{vq_id}|{question}|{notes}
```

Fields:

| Field | Required | Description |
|---|---:|---|
| `vq_id` | Yes | Stable verified-query ID. |
| `question` | Yes | Natural-language question. |
| `notes` | No | Usage notes or caveats. |

Example:

```text
VQ|vq.revenue_by_month|What was net revenue by month?|Canonical monthly revenue query.
```

### 15.2 SQL Literal Block

Each `VQ` SHOULD be followed by a SQL literal block:

```text
SQL|vq.revenue_by_month<<
select
  date_trunc('month', order_date) as month,
  sum(gross_revenue - discounts - refunds) as net_revenue
from mart_finance.fct_orders
where status = 'completed'
group by 1
order by 1
>>SQL
```

The SQL block ID MUST match the associated `vq_id`.

---

## 16. Validation Rules

An LSF-1 document is structurally valid if all of the following are true:

1. The document has exactly one opening `DB_CONTEXT` tag.
2. The document has exactly one closing `</DB_CONTEXT>` tag.
3. The wrapper `version` attribute is `LSF-1`.
4. The wrapper has a non-empty `dialect` attribute.
5. The document contains exactly one `DOMAIN` record.
6. The document contains a `TABLES` section.
7. The document contains a `RELATIONSHIPS` section.
8. The document contains at most one `COVERAGE` record.
9. Every `TABLE` record has the required number of fields.
10. Every `COL` record refers to a defined table.
11. Every table has at least one column.
12. Every `COLS` record uses the LSF-1 column header order.
13. Every `FK` column either has a valid `ref` or an explicit `-`.
14. Every `REL` record references defined columns.
15. Every `METRIC` record references a defined base table.
16. Every metric allowed dimension references a defined column.
17. Every enum references a defined column.
18. Every `WARN` record references a defined object when the target uses an object ID.
19. Every SQL literal block is terminated by `>>SQL`.
20. Every SQL literal block ID matches a defined `VQ` ID.
21. No field contains an unescaped pipe character except as a separator.

A document is semantically valid if, in addition:

1. Table grains are specific enough to reason about joins.
2. Fanout-unsafe relationships are marked `unsafe` or `conditional`.
3. Metrics define default filters when business definitions require them.
4. Metrics define allowed dimensions where not all dimensions are valid.
5. Ambiguous business terms are represented in `AMBIGUITIES` when known.
6. Security-sensitive columns are tagged with `PII` or equivalent local tags.

---

## 17. Conformance Profiles

### 17.1 Minimal Profile

A Minimal LSF-1 context packet MUST contain:

1. `DB_CONTEXT` wrapper.
2. `DOMAIN` record.
3. `TABLES` section.
4. At least one `TABLE` record.
5. At least one `COL` record per table.
6. `RELATIONSHIPS` section, even if empty.

### 17.2 Analytics Profile

An Analytics LSF-1 context packet MUST contain everything in the Minimal profile and SHOULD contain:

1. `METRICS` section.
2. `RULES` section.
3. `VERIFIED_QUERIES` section.
4. `AMBIGUITIES` section when known ambiguous terms exist.
5. `ENUMS` section for important categorical columns.

### 17.3 Production NL-to-SQL Profile

A Production NL-to-SQL LSF-1 context packet SHOULD contain:

1. All Analytics profile elements.
2. At least one verified query for each high-value metric family.
3. Fanout annotations for all relationships.
4. PII tags or policy tags for sensitive columns.
5. Query-generation rules for read-only behavior.
6. Default timezone.
7. Default currency when financial metrics exist.
8. Warnings for non-additive metrics and unsafe joins.

### 17.4 baseball.computer profile

The baseball.computer profile is generated by `scripts/generate_llm_context.py` from three sources:

1. **SQLMesh `Context`** (`bc/config.py`) — primary source for `TABLE`, `COL`, and `REL` records. Yields per model: `name`, `description`, `grain`, `columns_to_types`, `column_descriptions`, `audits_with_args` (including the `relationships(...)` audit), `tags`.
2. **`python_models.metrics.registry`** (`metrics_for(kind, source)`) — primary source for the BSL semantic tables and their `METRIC` records. Each metric's `Metric.classification` (`sum`, `ratio`, `derived`) maps to LSF-1's `agg` column (`sum`, `ratio`, `derived`).
3. **`docs/llm/supplement.yaml`** — hand-curated `RULES`, `AMBIGUITIES`, `VERIFIED_QUERIES`, the `COVERAGE` string, and optional table synonyms. This is the only file someone maintains by hand.

Conformance requirements specific to this profile:

- `dialect` MUST be `duckdb`.
- The packet MUST emit one `TABLE` per BSL semantic table (`offense_seasons`, `offense_events`, `pitching_seasons`, `pitching_events`, `fielding_seasons`, `fielding_events`). BSL semantic tables are virtual: `physical_name` carries the principal underlying model, but the table also exposes columns from joined dimension tables (`seed_franchises`, `team_game_start_info`). Producers MUST document the joined columns in the table `description` so consumers do not assume they can issue raw SQL against the bare physical model.
- The packet SHOULD emit `RULE|should|global|Prefer the BSL semantic tables ... over main_models.metrics_* directly.` so the LLM routes through the semantic layer.
- Pre-computed rate stats (BSL `[calc]` measures) MUST be tagged `DERIVED` in `COL` records and MUST carry a `WARN` if also emitted as a `METRIC`.
- The `CURRENCY` record MUST be omitted (the domain has no monetary metrics).
- Project user-defined ENUM types (`PARK_ID`, `TEAM_ID`, `GAME_ID`, `PLAYER_ID`, `HAND`) MUST be reflected in the `type` field as `string<{ENUM_TYPE}>`.

---

## 18. Prompt Integration

A recommended prompt wrapper is:

```text
<SYSTEM_TASK>
Generate analytics SQL using only the provided DB_CONTEXT.
Prefer METRICS over raw column arithmetic.
Respect RELATIONSHIPS, RULES, ENUMS, WARN records, and AMBIGUITIES.
If the question is ambiguous, return a clarification request instead of SQL.
</SYSTEM_TASK>

{DB_CONTEXT}

<USER_QUESTION>
{natural_language_question}
</USER_QUESTION>
```

Systems SHOULD place LSF-1 context before the user question when the context is small enough to fit comfortably in the model context window.

Systems MAY retrieve and render only selected objects when the source catalog is large.

Systems SHOULD avoid rendering unrelated schemas, unrelated metrics, and large enum lists.

---

## 19. Recommended LLM Output Contract

LSF-1 is an input format. LLMs SHOULD NOT be asked to output LSF-1 for query execution.

For SQL generation, a recommended output object is:

```json
{
  "action": "generate_sql",
  "needs_clarification": false,
  "clarification_question": null,
  "used_objects": [
    "metric.net_revenue",
    "table.orders",
    "table.customers",
    "rel.orders_customers"
  ],
  "assumptions": [
    "Using order_date as the default time dimension.",
    "Using net_revenue for revenue."
  ],
  "sql": "select ..."
}
```

Production systems SHOULD validate output using JSON Schema or tool-call argument schemas.

---

## 20. Security Considerations

LSF-1 can expose sensitive database structure and business logic. Systems using LSF-1 SHOULD consider the following:

1. Do not include tables or columns unavailable to the requesting user.
2. Tag PII and sensitive columns.
3. Enforce access control outside the LLM.
4. Do not rely on `RULE|must|global|...` records alone for security.
5. Validate generated SQL using a parser or AST validator.
6. Block DDL, DML, external functions, stored procedure calls, and unsafe SQL constructs unless explicitly allowed.
7. Use read-only database roles.
8. Apply row limits, timeouts, and cost controls.
9. Log used context, generated SQL, validation outcomes, execution results, and user feedback.
10. Consider redacting sample values from sensitive columns.

---

## 21. Retrieval and Chunking Guidance

LSF-1 performs best when rendered as a small context packet rather than a full enterprise catalog.

Recommended retrieval units:

1. Domain summary.
2. Table object with selected columns.
3. Metric object plus dependencies.
4. Relationship object.
5. Enum values for selected columns.
6. Verified queries similar to the user question.
7. Ambiguity notes matching user terms.
8. Rules with global scope and matching object scope.

A retrieved packet SHOULD include all dependencies necessary to interpret a metric, including:

1. Base table.
2. Referenced columns in the metric expression.
3. Default time column.
4. Default filter columns.
5. Allowed dimension columns requested or likely requested.
6. Relationships needed to join allowed dimensions.

---

## 22. Complete Example

```text
<DB_CONTEXT version="LSF-1" dialect="duckdb">
DOMAIN|domain.baseball|Major-league play-by-play, season, and career analytics.
TZ|America/New_York
COVERAGE|Event-level play-by-play 1910+ (sparse 1871-1909); season/career stats 1871+; biographical data 1871+.

TABLES
TABLE|table.offense_seasons|main_models.player_offense_season_stats|one row per (player_id, team_id, season, game_type)|Player offense, one row per player-team-season-game_type. Backed by the BSL semantic table that joins league info from seed_franchises and filters to regular-season game types by default.|batters;hitters
COLS|table.offense_seasons|name|type|role|ref|desc|examples|tags
COL|table.offense_seasons|player_id|string<PLAYER_ID>|DIM|-|Retrosheet player id.|-|-
COL|table.offense_seasons|team_id|string<TEAM_ID>|DIM+FK|table.seed_franchises.team_id|Retrosheet team id.|-|-
COL|table.offense_seasons|season|int<USMALLINT>|DIM|-|Season year.|2018;2024|-
COL|table.offense_seasons|league|string|DIM|-|League id (AL/NL/...).|AL;NL|-
COL|table.offense_seasons|game_type|string|DIM|-|Game-type code; defaults filter to regular season.|-|enum.game_type
COL|table.offense_seasons|plate_appearances|int|MEASURE|-|Plate appearances.|-|-
COL|table.offense_seasons|at_bats|int|MEASURE|-|At-bats.|-|-
COL|table.offense_seasons|hits|int|MEASURE|-|Hits.|-|-
COL|table.offense_seasons|home_runs|int|MEASURE|-|Home runs.|-|-
COL|table.offense_seasons|walks|int|MEASURE|-|Walks (BB).|-|-
COL|table.offense_seasons|hbp|int|MEASURE|-|Hit-by-pitch.|-|-
COL|table.offense_seasons|sac_hits|int|MEASURE|-|Sacrifice hits.|-|-
COL|table.offense_seasons|batting_average|float|DERIVED|-|Pre-computed batting average; non-additive across rows.|-|-
COL|table.offense_seasons|obp|float|DERIVED|-|Pre-computed on-base percentage; non-additive across rows.|-|-

TABLE|table.seed_franchises|main_seeds.seed_franchises|one row per (team_id, date_start)|Retrosheet franchise dimension. League, division, city for each team era.|franchises;teams
COLS|table.seed_franchises|name|type|role|ref|desc|examples|tags
COL|table.seed_franchises|team_id|string<TEAM_ID>|PK|-|Retrosheet team id.|-|-
COL|table.seed_franchises|date_start|date|PK|-|Era start date.|-|-
COL|table.seed_franchises|league|string|DIM|-|League at this era.|AL;NL|-

RELATIONSHIPS
REL|rel.offense_seasons_team|table.offense_seasons.team_id|table.seed_franchises.team_id|many_to_one|conditional|join on team_id with overlap on season vs (date_start.year, date_end.year)|Franchise dim has time-bounded rows; uniqueness on team_id alone does not hold.

METRICS
METRIC|metric.batting_average_offense_season|batting_average|table.offense_seasons|sum(hits) / nullif(sum(at_bats),0)|ratio|table.offense_seasons.season|-|table.offense_seasons.player_id;table.offense_seasons.team_id;table.offense_seasons.season;table.offense_seasons.league|Batting average: hits divided by at-bats.|avg;ba
METRIC|metric.obp_offense_season|obp|table.offense_seasons|(sum(hits) + sum(walks) + sum(hbp)) / nullif(sum(plate_appearances) - sum(sac_hits),0)|ratio|table.offense_seasons.season|-|table.offense_seasons.player_id;table.offense_seasons.team_id;table.offense_seasons.season;table.offense_seasons.league|On-base percentage: (H + BB + HBP) / (PA - SH).|on base;on-base;on base percentage
WARN|metric.batting_average_offense_season|Non-additive: cannot SUM or AVG across rows. Recompute from underlying counts when grouping.
WARN|metric.obp_offense_season|Non-additive: cannot SUM or AVG across rows. Recompute from underlying counts when grouping.

ENUMS
ENUM|enum.game_type|table.offense_seasons.game_type|RegularSeason|Regular-season game.|-
ENUM|enum.game_type|table.offense_seasons.game_type|WildCardSeries|Wild Card postseason series.|-
ENUM|enum.game_type|table.offense_seasons.game_type|WorldSeries|World Series.|ws

AMBIGUITIES
AMBIG|HR|metric.home_runs_offense_season;metric.home_runs_pitching_season|metric.home_runs_offense_season|Clarify when the question contrasts hitters and pitchers ("HR allowed").
AMBIG|average|metric.batting_average_offense_season;arithmetic mean|metric.batting_average_offense_season|Clarify when the subject is not a hitting context.

RULES
RULE|must|global|Generate read-only SELECT queries only against bc.db.
RULE|should|global|Prefer BSL semantic tables (offense_seasons, pitching_seasons, ...) over main_models.metrics_* directly.
RULE|should|global|Prefer published main_models.* tables over staging stg_* tables.
RULE|warn|table.offense_seasons|BSL season tables are pre-filtered to regular-season game_types; pass game_type explicitly to override.

VERIFIED_QUERIES
VQ|vq.trout_2018_obp|What was Mike Trout's OBP in 2018?|Single-player season query against the offense_seasons semantic table.
SQL|vq.trout_2018_obp<<
select obp
from main_models.metrics_player_season_league_offense
where player_id = 'troum001' and season = 2018 and league = 'AL'
>>SQL
</DB_CONTEXT>
```

---

## 23. Suggested Parser Behavior

A parser SHOULD:

1. Normalize line endings to `\n`.
2. Validate wrapper tags.
3. Remove comments and empty lines outside literal blocks.
4. Split regular records by unescaped `|`.
5. Unescape field values.
6. Track current section.
7. Build symbol tables for tables, columns, metrics, relationships, enums, rules, and verified queries.
8. Validate references after parsing the full document.
9. Preserve SQL literal blocks exactly.

A parser SHOULD NOT silently accept unknown record types unless explicitly configured for forward compatibility.

---

## 24. Versioning

This document defines `LSF-1`.

Future versions SHOULD preserve the following compatibility principles:

1. Existing record names SHOULD retain their current field order.
2. New optional fields SHOULD be added only through new record types or versioned variants.
3. Parsers SHOULD reject unsupported major versions.
4. Parsers MAY ignore unknown sections if configured to do so.
5. Producers SHOULD emit the lowest LSF version that supports the required features.

The wrapper version is intentionally coarse:

```text
<DB_CONTEXT version="LSF-1" ...>
```

Patch-level changes to this document SHOULD NOT change the wrapper version unless record syntax changes.

---

## 25. Implementation Checklist

A minimal producer should implement:

1. Table extraction from information schema or catalog metadata.
2. Column extraction with normalized types.
3. Primary-key and foreign-key detection when available.
4. Table descriptions from catalog, dbt, BI layer, or manual docs.
5. Column descriptions from catalog, dbt, BI layer, or manual docs.
6. Relationship records.
7. Safe escaping of `|`, `\`, `\n`, and semicolon list items.
8. Wrapper generation.

An analytics producer should additionally implement:

1. Metric extraction from semantic layer.
2. Default filters.
3. Allowed dimensions.
4. Fanout warnings.
5. Enum/top-value extraction.
6. Verified query extraction.
7. Ambiguity records.
8. Rules.

A production consumer should implement:

1. Structural validation.
2. Reference validation.
3. Prompt rendering.
4. SQL output validation.
5. Read-only execution controls.
6. Logging and evaluation.

---

## 26. Summary

LSF-1 is a compact, regular, LLM-oriented schema-context format. Its core shape is:

```text
outer DB_CONTEXT fence
metadata records
table records
column tuple rows
relationship records
metric records
enum records
ambiguity records
rule records
verified SQL examples
```

The essential principle is that LLMs should receive not merely database structure, but **analytics semantics**: grain, metrics, joins, safe dimensions, caveats, examples, and rules.

