from __future__ import annotations

import hashlib
from collections import defaultdict
from typing import NamedTuple

import numpy as np
import numpy.typing as npt
import polars as pl

FEATURES = (
    "era",
    "result_family",
    "base_state_start",
    "outs_start",
    "alignment_regime",
    "batter_hand",
)
MISSING = "__MISSING__"
FloatArray = npt.NDArray[np.float64]


class BaselineResult(NamedTuple):
    probabilities: FloatArray
    routes: list[str]


def stable_uniform(value: str, seed: int) -> float:
    raw = hashlib.sha256(f"{seed}:{value}".encode()).digest()
    return (int.from_bytes(raw[:8], "big") >> 11) / 2**53


def game_digest(games: list[str]) -> str:
    digest = hashlib.sha256()
    for game in sorted(set(games)):
        raw = game.encode()
        digest.update(len(raw).to_bytes(4, "big"))
        digest.update(raw)
    return digest.hexdigest()


def verify_exclusions(
    train: pl.DataFrame, test: pl.DataFrame, reserve: set[str]
) -> dict[str, object]:
    train_games = set(str(value) for value in train["game_id"])
    test_games = set(str(value) for value in test["game_id"])
    if train_games & test_games:
        raise ValueError("TRAIN and TEST games overlap")
    if (train_games | test_games) & reserve:
        raise ValueError("reserved games entered the stress experiment")
    if set(train["primary_fold"].unique().to_list()) != {"TRAIN"}:
        raise ValueError("fitting rows must belong to primary TRAIN")
    if set(test["primary_fold"].unique().to_list()) != {"TEST"}:
        raise ValueError("evaluation rows must belong to primary TEST")
    return {
        "train_game_count": len(train_games),
        "test_game_count": len(test_games),
        "train_games_sha256": game_digest(list(train_games)),
        "test_games_sha256": game_digest(list(test_games)),
        "train_test_overlap": 0,
        "reserve_overlap": 0,
    }


def contextual_baseline(
    train: pl.DataFrame, test: pl.DataFrame, labels: tuple[str, ...]
) -> BaselineResult:
    if train.is_empty() or not labels or len(set(labels)) != len(labels):
        raise ValueError("baseline requires nonempty training and unique classes")
    lookup = {label: index for index, label in enumerate(labels)}
    cells: dict[tuple[str, str], FloatArray] = {}
    results: dict[str, FloatArray] = {}
    eras: dict[str, FloatArray] = {}
    marginal = np.ones(len(labels), dtype=np.float64)
    for era, result, label, count in (
        train.group_by("era", "result_family", "target_class")
        .len()
        .sort("era", "result_family", "target_class")
        .iter_rows()
    ):
        if label not in lookup:
            raise ValueError(f"TRAIN class outside vocabulary: {label}")
        index = lookup[str(label)]
        key = (str(era), str(result))
        cells.setdefault(key, np.ones(len(labels), dtype=np.float64))[index] += count
        results.setdefault(key[1], np.ones(len(labels), dtype=np.float64))[index] += (
            count
        )
        eras.setdefault(key[0], np.ones(len(labels), dtype=np.float64))[index] += count
        marginal[index] += count
    probabilities = np.empty((test.height, len(labels)), dtype=np.float64)
    routes: list[str] = []
    for row, (era, result) in enumerate(
        test.select("era", "result_family").iter_rows()
    ):
        key = (str(era), str(result))
        if key in cells:
            counts, route = cells[key], "era_result"
        elif key[1] in results and key[1] != MISSING:
            counts, route = results[key[1]], "result"
        elif key[0] in eras and key[0] != MISSING:
            counts, route = eras[key[0]], "era"
        else:
            counts, route = marginal, "marginal"
        probabilities[row] = counts / counts.sum()
        routes.append(route)
    return BaselineResult(probabilities, routes)


def apply_joint_masks(
    frame: pl.DataFrame,
    profile: pl.DataFrame,
    *,
    seed: int,
    target: str,
) -> pl.DataFrame:
    required = {"era", "scorer", "mask_pattern", "rows"}
    if required - set(profile.columns):
        raise ValueError("mask profile needs era, scorer, mask_pattern, rows")
    distributions: dict[tuple[str, str], dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    for era, scorer, pattern, count in profile.select(
        "era", "scorer", "mask_pattern", "rows"
    ).iter_rows():
        if len(str(pattern)) != len(FEATURES) or set(str(pattern)) - {"0", "1"}:
            raise ValueError(f"invalid joint mask: {pattern}")
        if not np.isfinite(float(count)) or float(count) <= 0:
            raise ValueError("mask profile counts must be positive")
        distributions[(str(era), str(scorer))][str(pattern)] += float(count)
        if scorer == "*":
            distributions[("*", "*")][str(pattern)] += float(count)
    if ("*", "*") not in distributions:
        raise ValueError("natural-unknown mask profile needs exact era pools")
    game_patterns: dict[str, str] = {}
    game_routes: dict[str, str] = {}
    game_rows = frame.select("game_id", "era", "scorer").unique().sort("game_id")
    if game_rows["game_id"].n_unique() != game_rows.height:
        raise ValueError("game has conflicting era or scorer metadata")
    for game, era, scorer in game_rows.iter_rows():
        exact, pooled = (str(era), str(scorer)), (str(era), "*")
        key = exact if exact in distributions else pooled
        route = "era_scorer" if key == exact else "era"
        if key not in distributions:
            key, route = ("*", "*"), "global"
        counts = distributions[key]
        threshold = stable_uniform(f"mask:{target}:{game}", seed) * sum(counts.values())
        cumulative = 0.0
        for pattern, count in sorted(counts.items()):
            cumulative += count
            if threshold < cumulative:
                game_patterns[str(game)] = pattern
                break
        game_routes[str(game)] = route
    out = frame.with_columns(
        pl.col("game_id")
        .replace_strict(game_patterns, return_dtype=pl.String)
        .alias("mask_pattern"),
        pl.col("game_id")
        .replace_strict(game_routes, return_dtype=pl.String)
        .alias("mask_profile_route"),
    )
    return out.with_columns(
        [
            pl.when(pl.col("mask_pattern").str.slice(index, 1) == "1")
            .then(pl.lit(MISSING))
            .otherwise(pl.col(feature))
            .alias(feature)
            for index, feature in enumerate(FEATURES)
        ]
    )


def feature_frame(
    frame: pl.DataFrame, target: str
) -> tuple[pl.DataFrame, tuple[str, ...]]:
    if target == "trajectory":
        out = frame.with_columns(
            pl.concat_str("era", "result_family", separator="|").alias("era_result")
        )
        return out, ("era_result", *FEATURES[2:])
    if target == "location_side":
        return frame, FEATURES
    raise ValueError(f"unsupported target: {target}")


def support_summary(
    frame: pl.DataFrame, levels: dict[str, tuple[str, ...]]
) -> tuple[pl.Series, dict[str, object]]:
    unseen = [~pl.col(feature).is_in(values) for feature, values in levels.items()]
    supported = frame.select((~pl.any_horizontal(unseen)).alias("supported"))[
        "supported"
    ]
    return supported, {
        "rows": frame.height,
        "fully_supported_rows": int(supported.sum()),
        "unseen_feature_rows": {
            feature: frame.filter(~pl.col(feature).is_in(values)).height
            for feature, values in levels.items()
        },
    }
