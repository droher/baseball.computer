"""Adversarial source/scorer probes against deep artifacts.

The held-out source_family probe answers "given the deep artifact's
embeddings (or per-row outputs), how well can a downstream linear model
identify whether a row comes from one specific source family?".

The semantics:

- Binary target = ``1`` when row's ``source_family == held_out_family``,
  else ``0``.
- Stratified ``train_test_split`` over the full row set so the test split
  contains both classes (AUC needs both labels present).
- LR trained on the train split, AUC measured on the test split.

AUC thresholds (doc-04 lines 235-241):

- ``>= 0.75`` → ``publication_tier = 'diagnostic_only'`` — embedding
  encodes the source family well enough that the Bayes layer must
  exclude this artifact from ``gamma_dl_shrunk`` runs.
- ``< 0.65`` → ``'full'`` — publication-eligible.
- ``[0.65, 0.75)`` → ``'manual_review'`` — recorded in manifest
  metadata for human triage.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar, Literal

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict

PublicationTier = Literal["full", "manual_review", "diagnostic_only"]


class ProbeResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    held_out_family: str
    auc: float
    n_rows: int
    publication_tier: PublicationTier


def classify_publication_tier(auc: float) -> PublicationTier:
    if auc >= 0.75:
        return "diagnostic_only"
    if auc < 0.65:
        return "full"
    return "manual_review"


def source_probe_held_out(
    embeddings: NDArray[np.float64],
    source_labels: Sequence[str],
    held_out_family: str,
    *,
    test_size: float = 0.3,
    random_state: int = 20260514,
    max_iter: int = 1000,
) -> ProbeResult:
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    if embeddings.ndim != 2:
        raise ValueError(
            f"embeddings must be 2-d (N x D), got shape {embeddings.shape}"
        )
    if len(source_labels) != embeddings.shape[0]:
        raise ValueError(
            f"len(source_labels)={len(source_labels)} != n_rows={embeddings.shape[0]}"
        )

    y = np.fromiter(
        (1 if s == held_out_family else 0 for s in source_labels),
        dtype=np.int64,
        count=len(source_labels),
    )
    if int(y.sum()) == 0:
        raise ValueError(
            f"held_out_family {held_out_family!r} not present in source_labels"
        )
    if int(y.sum()) == y.size:
        raise ValueError(
            f"held_out_family {held_out_family!r} is the only family in source_labels; AUC is undefined"
        )
    if y.size < 4:
        raise ValueError(
            f"need at least 4 rows for a stratified probe; got {y.size}"
        )

    split = train_test_split(
        embeddings,
        y,
        test_size=test_size,
        random_state=random_state,
        stratify=y,
    )
    x_train = np.asarray(split[0], dtype=np.float64)
    x_test = np.asarray(split[1], dtype=np.float64)
    y_train = np.asarray(split[2], dtype=np.int64)
    y_test = np.asarray(split[3], dtype=np.int64)

    clf = LogisticRegression(max_iter=max_iter)
    _ = clf.fit(x_train, y_train)
    probs = clf.predict_proba(x_test)[:, 1]
    auc = float(roc_auc_score(y_test, probs))
    return ProbeResult(
        held_out_family=held_out_family,
        auc=auc,
        n_rows=int(y_test.shape[0]),
        publication_tier=classify_publication_tier(auc),
    )
