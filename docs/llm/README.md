# LLM schema context

[Documentation guide](../README.md)

The LSF-1 context packet describes tables, columns, metrics, relationships, and
querying rules for schema retrieval and natural-language SQL tools.

| File | Role |
| --- | --- |
| [lsf_1_spec.md](lsf_1_spec.md) | Format specification, including the baseball.computer profile. |
| [supplement.yaml](supplement.yaml) | Authored rules, ambiguities, verified queries, coverage notes, and synonyms. |
| [baseball.lsf](baseball.lsf) | Generated context packet; retrieve the relevant records instead of reading it all. |

## Updating the packet

Edit model metadata, the [metric registry](../../bc/python_models/metrics/registry.py), or the supplement
according to the information being changed. The
[generator](../../scripts/generate_llm_context.py) combines those inputs.
From the repository root, regenerate and validate with:

```bash
just gen-llm-context
```

The recipe reads the production database and writes the packet. See
[DuckLake publication](../ducklake-production.md) for regeneration during export,
upload validation, and the public packet location. The
[earlier metadata proposal](../../notes/llm-metadata.md) is design history.
