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
  encodes the source family well enough that downstream consumers must
  exclude this artifact from any DL covariate flow.
- ``< 0.65`` → ``'full'`` — publication-eligible.
- ``[0.65, 0.75)`` → ``'manual_review'`` — recorded in manifest
  metadata for human triage.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import ClassVar, Literal

import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, ConfigDict

_log = logging.getLogger(__name__)

PublicationTier = Literal["full", "manual_review", "diagnostic_only"]


class ProbeResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    held_out_family: str
    auc: float
    n_rows: int
    publication_tier: PublicationTier


class ConfoundProbeResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    confound: str
    macro_auc: float
    random_baseline_auc: float
    delta_vs_random: float
    n_entities: int
    n_classes: int


class ConfoundLeakReport(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    entity_type: str
    embed_dim: int
    n_entities: int
    per_confound: tuple[ConfoundProbeResult, ...]


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


def _multiclass_macro_auc(
    X: NDArray[np.float64],
    y: NDArray[np.int64],
    *,
    test_size: float,
    random_state: int,
    max_iter: int,
) -> tuple[float, int]:
    """Train multinomial LR and return one-vs-rest macro AUC plus n_classes.

    Filters classes with <2 occurrences (stratified split unable to handle them).
    """
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import roc_auc_score
    from sklearn.model_selection import train_test_split

    classes, counts = np.unique(y, return_counts=True)
    keep_classes = classes[counts >= 2]
    if keep_classes.size < 2:
        return float("nan"), int(classes.size)
    keep_mask = np.isin(y, keep_classes)
    X_f = X[keep_mask]
    y_f = y[keep_mask]

    if y_f.size < 4:
        return float("nan"), int(keep_classes.size)
    x_train, x_test, y_train, y_test = train_test_split(
        X_f, y_f, test_size=test_size, random_state=random_state, stratify=y_f
    )
    clf = LogisticRegression(max_iter=max_iter, solver="lbfgs")
    clf.fit(x_train, y_train)
    probs = clf.predict_proba(x_test)
    try:
        if probs.shape[1] == 2:
            auc = float(roc_auc_score(y_test, probs[:, 1]))
        else:
            auc = float(
                roc_auc_score(
                    y_test,
                    probs,
                    multi_class="ovr",
                    average="macro",
                    labels=clf.classes_,
                )
            )
    except ValueError:
        auc = float("nan")
    return auc, int(keep_classes.size)


def confound_probe(
    embeddings: NDArray[np.float64],
    confounds: dict[str, Sequence[int]],
    *,
    test_size: float = 0.25,
    random_state: int = 20260515,
    max_iter: int = 200,
    random_baseline_seed: int = 20260515,
) -> tuple[ConfoundProbeResult, ...]:
    """Multiclass linear probe AUC per confound.

    For each confound (named -> per-entity integer label), trains a
    multinomial LR on a random split of ``embeddings``, reports macro
    one-vs-rest AUC. Also reports a random-baseline AUC by repeating
    the probe on a same-shape Gaussian matrix using the same labels;
    ``delta_vs_random`` lets the caller distinguish "embedding really
    encodes the confound" from "any 96-D feature would do this well".

    Rows with label code ``-1`` are skipped (unknown / NULL).
    """
    if embeddings.ndim != 2:
        raise ValueError(
            f"embeddings must be 2-d (N x D), got shape {embeddings.shape}"
        )

    rng = np.random.default_rng(seed=random_baseline_seed)
    n_rows = embeddings.shape[0]
    random_baseline = rng.standard_normal(embeddings.shape).astype(np.float64)

    results: list[ConfoundProbeResult] = []
    for name, raw_labels in confounds.items():
        y_all = np.asarray(list(raw_labels), dtype=np.int64)
        if y_all.size != n_rows:
            raise ValueError(
                f"confound {name!r}: len(labels)={y_all.size} != n_rows={n_rows}"
            )
        mask = y_all >= 0
        if int(mask.sum()) < 32:
            _log.warning(
                "confound_probe: skipping %s (only %d labeled entities)",
                name,
                int(mask.sum()),
            )
            continue
        X = np.asarray(embeddings[mask], dtype=np.float64)
        y = y_all[mask]
        macro_auc, n_classes = _multiclass_macro_auc(
            X, y, test_size=test_size, random_state=random_state, max_iter=max_iter
        )
        rand_auc, _ = _multiclass_macro_auc(
            random_baseline[mask],
            y,
            test_size=test_size,
            random_state=random_state,
            max_iter=max_iter,
        )
        results.append(
            ConfoundProbeResult(
                confound=name,
                macro_auc=macro_auc,
                random_baseline_auc=rand_auc,
                delta_vs_random=macro_auc - rand_auc,
                n_entities=int(mask.sum()),
                n_classes=n_classes,
            )
        )
    return tuple(results)
