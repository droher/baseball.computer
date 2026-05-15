"""Adversarial source/scorer probes against deep artifacts.

PR2 implements the held-out source_family probe described in doc-04
§"Embeddings". PR1 only exposes a typed stub so other deep modules can
reference it without circular-import gymnastics later.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, ConfigDict


class ProbeResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    held_out_family: str
    auc: float
    n_rows: int


def source_probe_held_out(*args: object, **kwargs: object) -> ProbeResult:
    del args, kwargs
    raise NotImplementedError("held-out source_family probe lands in PR2")
