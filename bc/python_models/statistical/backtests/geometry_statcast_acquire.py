from __future__ import annotations

import argparse
import logging
import platform
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from typing import cast
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb
import polars as pl

from python_models.statistical.backtests.geometry_reliability_data import (
    BASE,
    DATABASE,
    REPOSITORY,
    SCHEMA,
    read_json,
    write_json,
)
from python_models.statistical.backtests.geometry_statcast_bridge import (
    TEAM_IDS,
    SelectedGame,
    pair_game,
    read_balls,
    read_crosswalk,
    resolve_game,
)
from python_models.statistical.evidence_binding import file_digest

LOGGER = logging.getLogger(__name__)
SMOKE_ROOT = BASE / "geometry_reliability/20260911-statcast-bridge-smoke-v1"
RESERVE = BASE / "historical_stress/20260911-reserve-v1/reserved_games.parquet"
PA_INCREMENT_SQL = """CASE WHEN plate_appearance_result IS NOT NULL
    OR (plate_appearance_result IS NULL AND NOT no_play_flag AND outs + outs_on_play >= 3)
    THEN 1 ELSE 0 END"""
LOCAL_QUERY = f"""
WITH ordered AS (
    SELECT game_id, event_key,
        sum({PA_INCREMENT_SQL})
        OVER (PARTITION BY game_id ORDER BY event_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS retrosheet_pa_ordinal
    FROM main_models.stg_events WHERE game_id IN (SELECT game_id FROM acquisition_games)
)
SELECT g.event_key, g.game_id, o.retrosheet_pa_ordinal, g.class AS recorded_class,
    g.raw_value, g.is_observed_class, g.observed_status, g.source_family,
    g.source_acquisition_status, g.model_input_eligible, g.data_error_risk,
    g.season, g.park_id, g.scorer, g.batter_id, g.pitcher_id,
    g.inning_start, g.frame_start, g.base_state_start, g.outs_start,
    g.batter_hand, g.pitcher_hand, g.result_family, g.plate_appearance_result,
    g.batted_to_fielder, g.outs_on_play, g.fielder_chain, g.primary_fold,
    g.source_snapshot_id, g.geometry_target_contract
FROM {SCHEMA}.model_input_geometry AS g
JOIN ordered AS o USING (game_id, event_key)
WHERE g.primary_fold = 'TRAIN' AND g.geometry_dimension = 'trajectory'
ORDER BY g.game_id, o.retrosheet_pa_ordinal
"""


def fetch(url: str, path: Path) -> None:
    request = Request(url, headers={"User-Agent": "baseball.computer research"})
    with urlopen(request, timeout=45) as response:
        payload = cast(bytes, response.read())
    if not payload:
        raise ValueError("empty download")
    path.write_bytes(payload)


def validate_mechanics(
    cache: Path,
    fitting_games: pl.DataFrame,
    *,
    expected_manifest_sha256: str,
    crosswalk: dict[str, set[str]],
) -> None:
    if file_digest(cache / "manifest.json") != expected_manifest_sha256:
        raise ValueError("mechanics manifest differs from accepted binding")
    manifest = read_json(cache / "manifest.json")
    inventory = cast(dict[str, str], manifest["files_sha256"])
    declared = set(fitting_games.filter(pl.col("is_smoke"))["game_id"].to_list())
    required = {
        "report.json",
        "selected_fitting_games.parquet",
        "local_fitting_events.parquet",
        "plan.json",
    }
    required.update(
        f"games/{game}/{name}"
        for game in declared
        for name in (
            "report.json",
            "paired_events.parquet",
            "schedule.json",
            "statcast.csv",
        )
    )
    if not required <= set(inventory):
        raise ValueError("mechanics acceptance evidence is incomplete")
    for name, expected in inventory.items():
        path = cache / name
        if (
            not path.resolve().is_relative_to(cache.resolve())
            or file_digest(path) != expected
        ):
            raise ValueError("mechanics artifact content differs")
    acquired = pl.read_parquet(cache / "selected_fitting_games.parquet")
    report = read_json(cache / "report.json")
    game_reports = cast(list[dict[str, object]], report["reports"])
    reported = {
        str(cast(dict[str, object], item["game"])["game_id"]) for item in game_reports
    }
    events = sum(int(str(item["local_events"])) for item in game_reports)
    angles = sum(
        int(str(item.get("matched_angle_available", 0))) for item in game_reports
    )
    if (
        len(declared) != 12
        or declared != set(acquired["game_id"].to_list())
        or declared != reported
        or len(game_reports) != 12
        or any(item["status"] != "paired" for item in game_reports)
        or events == 0
        or angles / events < 0.95
        or report["modern_angle_evaluation_acquired"] is not False
    ):
        raise ValueError(
            "predeclared mechanics acceptance failed; full acquisition refused"
        )
    local = pl.read_parquet(cache / "local_fitting_events.parquet")
    if set(local["game_id"].to_list()) != declared or set(
        local["primary_fold"].to_list()
    ) != {"TRAIN"}:
        raise ValueError("mechanics local population mismatch")
    reproduced_events = 0
    reproduced_angles = 0
    for row in fitting_games.filter(pl.col("is_smoke")).to_dicts():
        game = SelectedGame.model_validate({**row, "date": str(row["date"])})
        root = cache / "games" / game.game_id
        game_pk = resolve_game((root / "schedule.json").read_bytes(), game)
        balls = read_balls((root / "statcast.csv").read_bytes(), game, game_pk)
        paired, recomputed = pair_game(
            local.filter(pl.col("game_id") == game.game_id), balls, crosswalk
        )
        saved = pl.read_parquet(root / "paired_events.parquet")
        keys = [
            "event_key",
            "statcast_at_bat_number",
            "mlbam_batter_id",
            "mlbam_pitcher_id",
            "match_status",
            "launch_angle",
            "angle_standardized_class",
        ]
        if not recomputed["game_pairing_complete"] or not paired.select(keys).sort(
            "event_key"
        ).equals(saved.select(keys).sort("event_key")):
            raise ValueError("mechanics pairing evidence does not reproduce")
        reproduced_events += paired.height
        reproduced_angles += paired.filter(pl.col("launch_angle").is_not_null()).height
    if (
        reproduced_events != events
        or reproduced_angles != angles
        or reproduced_angles / reproduced_events < 0.95
    ):
        raise ValueError("mechanics availability evidence does not reproduce")


def acquire_game(
    game: SelectedGame,
    local: pl.DataFrame,
    crosswalk: dict[str, set[str]],
    output: Path,
    cache: Path | None,
) -> dict[str, object]:
    root = output / "games" / game.game_id
    root.mkdir(parents=True, exist_ok=False)
    schedule_url = "https://statsapi.mlb.com/api/v1/schedule?" + urlencode(
        {
            "sportId": 1,
            "date": game.date,
            "teamId": TEAM_IDS[game.home_team_id],
            "fields": "dates,date,games,gamePk,gameNumber,doubleHeader,gameType,teams,home,away,team,id",
        }
    )
    report: dict[str, object] = {
        "game": game.model_dump(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "started",
        "local_events": local.height,
        "schedule_url": schedule_url,
    }
    write_json(root / "report.json", report)
    try:
        previous = cache / "games" / game.game_id if cache else None
        previous_report = (
            read_json(previous / "report.json")
            if previous and (previous / "report.json").exists()
            else None
        )
        if previous_report:
            raw_hashes = cast(dict[str, str], previous_report["raw_files_sha256"])
            for name, expected in raw_hashes.items():
                if (
                    name not in {"schedule.json", "statcast.csv"}
                    or previous is None
                    or file_digest(previous / name) != expected
                ):
                    raise ValueError("cached acquisition content mismatch")
                shutil.copy2(previous / name, root / name)
        else:
            fetch(schedule_url, root / "schedule.json")
        game_pk = resolve_game((root / "schedule.json").read_bytes(), game)
        url = f"https://baseballsavant.mlb.com/statcast_search/csv?all=true&type=details&game_pk={game_pk}"
        report.update({"game_pk": game_pk, "statcast_url": url})
        write_json(root / "report.json", report)
        if not (root / "statcast.csv").exists():
            fetch(url, root / "statcast.csv")
        balls = read_balls((root / "statcast.csv").read_bytes(), game, game_pk)
        paired, matching = pair_game(local, balls, crosswalk)
        paired = paired.with_columns(
            pl.lit(game_pk).alias("game_pk"),
            pl.lit(game.acquisition_fold).alias("acquisition_fold"),
        )
        paired.write_parquet(root / "paired_events.parquet")
        report.update(matching)
        report["status"] = (
            "paired" if matching["game_pairing_complete"] else "pairing_incomplete"
        )
        report["paired_events_sha256"] = file_digest(root / "paired_events.parquet")
    except (OSError, ValueError, KeyError) as error:
        LOGGER.exception("Acquisition failed for %s", game.game_id)
        report.update({"status": "failed", "error": str(error)})
    finally:
        report["raw_files_sha256"] = {
            name: file_digest(root / name)
            for name in ("schedule.json", "statcast.csv")
            if (root / name).exists()
        }
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_json(root / "report.json", report)
    LOGGER.info("Saved %s status=%s", game.game_id, report["status"])
    return report


def run(output: Path, *, smoke: bool, cache: Path | None) -> None:
    inputs_path = REPOSITORY / "docs/geometry-statcast-development-inputs.json"
    inputs = read_json(inputs_path)
    amendment = REPOSITORY / "docs/geometry-statcast-matching-amendment.md"
    if file_digest(amendment) != inputs["matching_amendment_sha256"]:
        raise ValueError("matching amendment changed")
    if (
        file_digest(REPOSITORY / "docs/geometry-statcast-development-protocol.md")
        != inputs["protocol_sha256"]
    ):
        raise ValueError("acquisition protocol changed")
    selection = REPOSITORY / str(inputs["selection_root"])
    if (
        file_digest(selection / "selected_games.parquet")
        != inputs["selected_games_sha256"]
        or file_digest(selection / "manifest.json") != inputs["manifest_sha256"]
    ):
        raise ValueError("frozen game selection changed")
    if file_digest(RESERVE) != inputs["reserve_file_sha256"]:
        raise ValueError("confirmation reserve changed")
    games = pl.read_parquet(selection / "selected_games.parquet").filter(
        pl.col("acquisition_fold") == "fitting"
    )
    if smoke:
        games = games.filter(pl.col("is_smoke"))
    if (
        games.is_empty()
        or games["game_id"].n_unique() != games.height
        or set(games["primary_fold"].to_list()) != {"TRAIN"}
    ):
        raise ValueError("invalid fitting-only acquisition set")
    if set(games["game_id"].to_list()) & set(
        pl.read_parquet(RESERVE)["game_id"].to_list()
    ):
        raise ValueError("reserve entered acquisition")
    selected = [
        SelectedGame.model_validate({**row, "date": str(row["date"])})
        for row in games.to_dicts()
    ]
    crosswalk_path = SMOKE_ROOT / "bc-chadwick-register.zip"
    expected = str(inputs["chadwick_zip_sha256"])
    if file_digest(crosswalk_path) != expected:
        raise ValueError("pinned crosswalk content changed")
    commit = read_json(SMOKE_ROOT / "bc-chadwick-commit.json")
    if commit["sha"] != inputs["chadwick_commit"]:
        raise ValueError("crosswalk commit differs from frozen inputs")
    crosswalk = read_crosswalk(crosswalk_path)
    if not smoke:
        acceptance = cast(dict[str, object] | None, inputs.get("mechanics_acceptance"))
        if (
            cache is None
            or acceptance is None
            or str(cache.resolve())
            != str((REPOSITORY / str(acceptance["root"])).resolve())
        ):
            raise ValueError(
                "full acquisition requires a bound accepted mechanics artifact"
            )
        validate_mechanics(
            cache,
            games,
            expected_manifest_sha256=str(acceptance["manifest_sha256"]),
            crosswalk=crosswalk,
        )
    output.mkdir(parents=True, exist_ok=False)
    for path in (
        inputs_path,
        amendment,
        REPOSITORY / "docs/geometry-statcast-development-protocol.md",
        Path(__file__),
        Path(__file__).with_name("geometry_statcast_bridge.py"),
    ):
        shutil.copy2(path, output / path.name)
    (output / "local_query.sql").write_text(LOCAL_QUERY)
    games.write_parquet(output / "selected_fitting_games.parquet")
    with duckdb.connect(str(DATABASE), read_only=True) as connection:
        connection.register("acquisition_games", games.select("game_id"))
        folds = connection.execute(
            f"SELECT DISTINCT primary_fold FROM {SCHEMA}.model_input_geometry WHERE game_id IN (SELECT game_id FROM acquisition_games)"
        ).fetchall()
        if folds != [("TRAIN",)]:
            raise ValueError("live game partition mismatch")
        local = connection.execute(LOCAL_QUERY).pl()
    local.write_parquet(output / "local_fitting_events.parquet")
    plan = {
        "status": "fitting_acquisition_only",
        "smoke": smoke,
        "games": games.height,
        "local_events": local.height,
        "reserve_overlap": 0,
        "modern_angle_evaluation_acquired": False,
        "crosswalk_sha256": expected,
        "inputs": inputs,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "runtime": {
            "python": platform.python_version(),
            **{name: version(name) for name in ("duckdb", "polars", "pydantic")},
        },
        "files_sha256": {
            p.name: file_digest(p) for p in output.iterdir() if p.is_file()
        },
    }
    write_json(output / "plan.json", plan)
    reports: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(
                acquire_game,
                game,
                local.filter(pl.col("game_id") == game.game_id),
                crosswalk,
                output,
                cache,
            )
            for game in selected
        ]
        for future in as_completed(futures):
            reports.append(future.result())
            write_json(
                output / "progress.json",
                {
                    "completed_games": len(reports),
                    "planned_games": len(selected),
                    "game_reports": reports,
                },
            )
    paths = sorted((output / "games").glob("*/paired_events.parquet"))
    if paths:
        pl.concat(
            [pl.read_parquet(p) for p in paths], how="diagonal_relaxed"
        ).write_parquet(output / "paired_events.parquet")
    write_json(
        output / "report.json",
        {
            "games_planned": len(selected),
            "games_paired_complete": sum(r["status"] == "paired" for r in reports),
            "games_incomplete": sum(
                r["status"] == "pairing_incomplete" for r in reports
            ),
            "games_failed": sum(r["status"] == "failed" for r in reports),
            "local_events_planned": local.height,
            "modern_angle_evaluation_acquired": False,
            "reserve_overlap": 0,
            "standardized_reliability_established": False,
            "reports": reports,
        },
    )
    write_json(
        output / "manifest.json",
        {
            "files_sha256": {
                str(p.relative_to(output)): file_digest(p)
                for p in sorted(output.rglob("*"))
                if p.is_file()
            }
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--cache", type=Path)
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    run(
        cast(Path, args.output),
        smoke=bool(args.smoke),
        cache=cast(Path | None, args.cache),
    )


if __name__ == "__main__":
    main()
