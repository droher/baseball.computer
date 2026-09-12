"""DuckLake catalog metadata reflects the tables published for the site."""

from __future__ import annotations

import importlib.util
import json
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "upload_ducklake.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"upload_ducklake_test_{uuid.uuid4().hex}", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _create_catalog(catalog_path: Path, data_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{catalog_path}' AS bc_publish (DATA_PATH '{data_path}/')"
    )
    _ = con.execute("CREATE SCHEMA bc_publish.main_models")
    _ = con.execute(
        "CREATE TABLE bc_publish.main_models.players "
        "AS SELECT 1 AS player_id, 'Ada' AS name"
    )
    con.close()


def test_catalog_metadata_uses_published_ducklake_schema(tmp_path: Path) -> None:
    script = _load_script()
    catalog_path = tmp_path / "catalog.ducklake"
    data_path = tmp_path / "data"
    _create_catalog(catalog_path, data_path)

    metadata = script.catalog_metadata(catalog_path, data_path)

    assert metadata == {
        "nodes": {
            "main_models.players": {
                "metadata": {"schema": "main_models", "name": "players"},
                "columns": {
                    "player_id": {"name": "player_id"},
                    "name": {"name": "name"},
                },
            }
        }
    }


def test_write_catalog_metadata_writes_legacy_shape(tmp_path: Path) -> None:
    script = _load_script()
    catalog_path = tmp_path / "catalog.ducklake"
    data_path = tmp_path / "data"
    output_path = tmp_path / "catalog.json"
    _create_catalog(catalog_path, data_path)

    result = script.write_catalog_metadata(catalog_path, data_path, output_path)

    assert result == output_path
    assert json.loads(output_path.read_text()) == script.catalog_metadata(
        catalog_path, data_path
    )


def test_catalog_and_metadata_revalidate() -> None:
    script = _load_script()

    assert script.CATALOG_CACHE_CONTROL == "public, max-age=0, must-revalidate"
    assert script.CATALOG_METADATA_CACHE_CONTROL == script.CATALOG_CACHE_CONTROL


def test_wrangler_command_uses_documented_remote_upload_arguments(
    tmp_path: Path,
) -> None:
    script = _load_script()
    local = tmp_path / "catalog.json"

    command = script.wrangler_upload_command(
        local,
        "baseball/v1/catalog.json",
        "public, max-age=0, must-revalidate",
        "application/json",
    )

    assert command == [
        "npx",
        "--yes",
        "wrangler@4.128.0",
        "r2",
        "object",
        "put",
        "timeball/baseball/v1/catalog.json",
        "--file",
        str(local),
        "--remote",
        "--cache-control",
        "public, max-age=0, must-revalidate",
        "--content-type",
        "application/json",
    ]


def test_wrangler_rejects_files_over_single_upload_limit() -> None:
    script = _load_script()

    assert script.WRANGLER_MAX_UPLOAD_BYTES == 300 * 1024 * 1024
    with pytest.raises(SystemExit, match="exceeding Wrangler's single-upload limit"):
        script.assert_wrangler_upload_size(
            script.WRANGLER_MAX_UPLOAD_BYTES + 1,
            "baseball/v1/too-large.parquet",
        )


def test_wrangler_preflight_checks_every_file_before_upload(tmp_path: Path) -> None:
    script = _load_script()
    small = tmp_path / "small.parquet"
    small.write_bytes(b"x")
    large = tmp_path / "large.parquet"
    with large.open("wb") as output:
        output.truncate(script.WRANGLER_MAX_UPLOAD_BYTES + 1)

    with pytest.raises(SystemExit, match="large.parquet.*single-upload limit"):
        script.preflight_uploads(
            script.WranglerUploader(),
            [
                (small, "baseball/v1/small.parquet"),
                (large, "baseball/v1/large.parquet"),
            ],
        )


def test_worker_count_is_bounded() -> None:
    script = _load_script()

    assert script.worker_count("8") == 8
    with pytest.raises(Exception, match="workers must be between 1 and 16"):
        script.worker_count("0")
    with pytest.raises(Exception, match="workers must be between 1 and 16"):
        script.worker_count("17")


class _RecordingS3Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, dict[str, str]]] = []

    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, str],
    ) -> None:
        self.calls.append((filename, bucket, key, ExtraArgs))


class _RecordingUploader:
    def __init__(self, failures: int = 0) -> None:
        self.failures = failures
        self.calls: list[str] = []

    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int:
        self.calls.append(key)
        if self.failures:
            self.failures -= 1
            raise RuntimeError("temporary upload failure")
        return local.stat().st_size


def test_boto3_uploader_preserves_json_content_type(tmp_path: Path) -> None:
    script = _load_script()
    local = tmp_path / "catalog.json"
    local.write_text("{}")
    client = _RecordingS3Client()
    uploader = script.Boto3Uploader(client)

    size = uploader.upload(
        local,
        "baseball/v1/catalog.json",
        script.CATALOG_METADATA_CACHE_CONTROL,
        "application/json",
    )

    assert size == 2
    assert client.calls == [
        (
            str(local),
            "timeball",
            "baseball/v1/catalog.json",
            {
                "CacheControl": "public, max-age=0, must-revalidate",
                "ContentType": "application/json",
            },
        )
    ]


def test_resume_skips_only_matching_immutable_data(tmp_path: Path) -> None:
    script = _load_script()
    local = tmp_path / "part.parquet"
    local.write_bytes(b"baseball")
    uploader = _RecordingUploader()
    looked_up: list[str] = []

    def lookup(key: str) -> Any:
        looked_up.append(key)
        return script.RemoteObjectInfo(8, '"276f8db0b86edaa7fc805516c852c889"')

    resumed = script.ResumeUploader(uploader, "baseball/v1", lookup)

    assert (
        resumed.upload(
            local,
            "baseball/v1/bc_publish_data/part.parquet",
            script.DATA_CACHE_CONTROL,
            "application/octet-stream",
        )
        == 8
    )
    assert uploader.calls == []
    assert looked_up == ["baseball/v1/bc_publish_data/part.parquet"]

    _ = resumed.upload(
        local,
        "baseball/v1/catalog.json",
        script.CATALOG_METADATA_CACHE_CONTROL,
        "application/json",
    )
    assert uploader.calls == ["baseball/v1/catalog.json"]
    assert looked_up == ["baseball/v1/bc_publish_data/part.parquet"]


def test_resume_uploads_when_remote_checksum_does_not_match(tmp_path: Path) -> None:
    script = _load_script()
    local = tmp_path / "part.parquet"
    local.write_bytes(b"baseball")
    uploader = _RecordingUploader()
    resumed = script.ResumeUploader(
        uploader,
        "baseball/v1",
        lambda _: script.RemoteObjectInfo(8, '"00000000000000000000000000000000"'),
    )

    _ = resumed.upload(
        local,
        "baseball/v1/bc_publish_data/part.parquet",
        script.DATA_CACHE_CONTROL,
        "application/octet-stream",
    )

    assert uploader.calls == ["baseball/v1/bc_publish_data/part.parquet"]


def test_upload_file_retries_transient_failure(tmp_path: Path) -> None:
    script = _load_script()
    local = tmp_path / "part.parquet"
    local.write_bytes(b"baseball")
    uploader = _RecordingUploader(failures=2)

    size = script.upload_file(
        uploader,
        local,
        "baseball/v1/bc_publish_data/part.parquet",
        script.DATA_CACHE_CONTROL,
        "application/octet-stream",
    )

    assert size == 8
    assert uploader.calls == [
        "baseball/v1/bc_publish_data/part.parquet",
        "baseball/v1/bc_publish_data/part.parquet",
        "baseball/v1/bc_publish_data/part.parquet",
    ]


def test_data_upload_failure_cancels_queued_files(tmp_path: Path) -> None:
    script = _load_script()
    first = tmp_path / "first.parquet"
    first.write_bytes(b"first")
    queued = tmp_path / "queued.parquet"
    queued.write_bytes(b"queued")

    class FailingUploader:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def upload(
            self,
            local: Path,
            key: str,
            cache_control: str,
            content_type: str,
        ) -> int:
            self.calls.append(key)
            raise RuntimeError("upload failed")

    uploader = FailingUploader()
    with pytest.raises(SystemExit, match="after 3 attempts"):
        script.upload_data_files(
            uploader,
            [
                (first, "baseball/v1/bc_publish_data/first.parquet"),
                (queued, "baseball/v1/bc_publish_data/queued.parquet"),
            ],
            workers=1,
        )

    assert uploader.calls == ["baseball/v1/bc_publish_data/first.parquet"] * 3


MINIMAL_PACKET = (
    '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
    "DOMAIN|domain.t|test\n"
    "TABLES\n"
    "TABLE|table.a|s.a|one row per id|test|-\n"
    "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
    "COL|table.a|id|int|PK|-|-|-|-\n"
    "RELATIONSHIPS\n"
    "</DB_CONTEXT>\n"
)


def test_assert_packet_valid_accepts_valid_and_rejects_invalid(tmp_path: Path) -> None:
    script = _load_script()
    packet = tmp_path / "baseball.lsf"
    packet.write_text(MINIMAL_PACKET, encoding="utf-8")
    script.assert_packet_valid(packet)
    packet.write_text(
        MINIMAL_PACKET.replace("COL|table.a|id|int|PK|-|-|-|-\n", ""), encoding="utf-8"
    )
    with pytest.raises(SystemExit, match="fails LSF-1 validation"):
        script.assert_packet_valid(packet)
    with pytest.raises(SystemExit, match="packet not found"):
        script.assert_packet_valid(tmp_path / "missing.lsf")


class _HeaderRecordingUploader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str]] = []

    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int:
        self.calls.append((key, cache_control, content_type))
        return local.stat().st_size


def test_upload_artifact_ships_packet_with_revalidating_headers_before_catalog(
    tmp_path: Path,
) -> None:
    script = _load_script()
    catalog_path = tmp_path / "bc_publish.ducklake"
    data_path = tmp_path / "bc_publish_data"
    _create_catalog(catalog_path, data_path)
    packet = tmp_path / "baseball.lsf"
    packet.write_text(MINIMAL_PACKET, encoding="utf-8")
    script.CATALOG_PATH = catalog_path
    script.DATA_PATH = data_path
    script.PACKET_PATH = packet
    script.CATALOG_METADATA_PATH = tmp_path / "catalog.json"
    uploader = _HeaderRecordingUploader()

    catalog_url, metadata_url, packet_url, _ = script.upload_artifact(
        "baseball/v9", uploader, workers=1
    )

    tail = uploader.calls[-3:]
    assert [key for key, _, _ in tail] == [
        "baseball/v9/catalog.json",
        "baseball/v9/baseball.lsf",
        "baseball/v9/baseball.ducklake",
    ]
    assert tail[1] == (
        "baseball/v9/baseball.lsf",
        script.CATALOG_METADATA_CACHE_CONTROL,
        "text/plain; charset=utf-8",
    )
    assert all(
        key.startswith("baseball/v9/bc_publish_data/")
        for key, _, _ in uploader.calls[:-3]
    )
    assert packet_url == f"https://{script.PUBLIC_HOST}/baseball/v9/baseball.lsf"
    assert {catalog_url, metadata_url, packet_url} == {
        f"https://{script.PUBLIC_HOST}/baseball/v9/{name}"
        for name in ("baseball.ducklake", "catalog.json", "baseball.lsf")
    }


class _FakeCloudflare:
    def __init__(
        self,
        rules: list[dict[str, Any]] | None,
        ruleset_id: str = "rs1",
        not_found: type[Exception] = KeyError,
    ) -> None:
        self.rules = rules
        self.ruleset_id = ruleset_id
        self.not_found = not_found
        self.writes: list[tuple[str, str, dict[str, Any] | None]] = []

    def __call__(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        if method == "GET":
            if self.rules is None:
                raise self.not_found("no entrypoint ruleset")
            return {
                "success": True,
                "result": {"id": self.ruleset_id, "rules": self.rules},
            }
        self.writes.append((method, path, body))
        return {"success": True, "result": {}}


def test_cache_rule_expression_covers_every_metadata_object() -> None:
    script = _load_script()
    expression = script.metadata_cache_rule_expression()
    assert f'http.host eq "{script.PUBLIC_HOST}"' in expression
    assert 'starts_with(http.request.uri.path, "/baseball/")' in expression
    for name in (
        script.CATALOG_OBJECT_NAME,
        script.CATALOG_METADATA_OBJECT_NAME,
        script.PACKET_OBJECT_NAME,
    ):
        assert any(
            name.endswith(suffix.lstrip("/"))
            for suffix in script.metadata_path_suffixes()
        ), name
    assert 'ends_with(http.request.uri.path, ".lsf")' in expression
    assert ".parquet" not in expression


def test_ensure_cache_rule_leaves_matching_rule_alone() -> None:
    script = _load_script()
    desired = script.desired_metadata_cache_rule()
    other = {"id": "r0", "description": "something else", "expression": "true"}
    api = _FakeCloudflare([other, {"id": "r1", **desired}])
    assert script.ensure_metadata_cache_rule("zone", api) == "unchanged"
    assert api.writes == []


def test_ensure_cache_rule_patches_only_the_named_rule_when_it_differs() -> None:
    script = _load_script()
    desired = script.desired_metadata_cache_rule()
    stale = {
        "id": "r1",
        **desired,
        "expression": desired["expression"].replace(
            ' or ends_with(http.request.uri.path, ".lsf")', ""
        ),
    }
    other = {"id": "r0", "description": "something else", "expression": "true"}
    api = _FakeCloudflare([other, stale])
    assert script.ensure_metadata_cache_rule("zone", api) == "updated"
    assert api.writes == [("PATCH", "/zones/zone/rulesets/rs1/rules/r1", desired)]


def test_ensure_cache_rule_updates_a_disabled_rule() -> None:
    script = _load_script()
    desired = script.desired_metadata_cache_rule()
    api = _FakeCloudflare([{"id": "r1", **desired, "enabled": False}])
    assert script.ensure_metadata_cache_rule("zone", api) == "updated"
    assert [w[0] for w in api.writes] == ["PATCH"]


def test_ensure_cache_rule_creates_missing_rule_in_existing_ruleset() -> None:
    script = _load_script()
    api = _FakeCloudflare(
        [{"id": "r0", "description": "something else", "expression": "true"}]
    )
    assert script.ensure_metadata_cache_rule("zone", api) == "created"
    assert api.writes == [
        ("POST", "/zones/zone/rulesets/rs1/rules", script.desired_metadata_cache_rule())
    ]


def test_ensure_cache_rule_creates_ruleset_when_zone_has_none() -> None:
    script = _load_script()
    api = _FakeCloudflare(None, not_found=script.CloudflareNotFound)
    assert script.ensure_metadata_cache_rule("zone", api) == "created"
    assert api.writes == [
        (
            "PUT",
            f"/zones/zone/rulesets/phases/{script.CACHE_RULE_PHASE}/entrypoint",
            {"rules": [script.desired_metadata_cache_rule()]},
        )
    ]


def test_ensure_cache_rule_refuses_duplicate_named_rules() -> None:
    script = _load_script()
    desired = script.desired_metadata_cache_rule()
    api = _FakeCloudflare([{"id": "r1", **desired}, {"id": "r2", **desired}])
    with pytest.raises(SystemExit, match="2 cache rules named"):
        script.ensure_metadata_cache_rule("zone", api)
    assert api.writes == []


def test_purge_posts_urls_to_zone() -> None:
    script = _load_script()
    api = _FakeCloudflare([])
    script.cloudflare_purge(["https://x/a", "https://x/b"], "zone", api)
    assert api.writes == [
        ("POST", "/zones/zone/purge_cache", {"files": ["https://x/a", "https://x/b"]})
    ]


def test_edge_cache_check_fails_only_on_hit() -> None:
    script = _load_script()
    statuses = {
        "https://x/ok": "DYNAMIC",
        "https://x/miss": "MISS",
        "https://x/none": "",
    }
    script.assert_metadata_not_edge_cached(
        list(statuses), lambda url: {"cf-cache-status": statuses[url]}
    )
    with pytest.raises(SystemExit, match="served from the Cloudflare edge cache"):
        script.assert_metadata_not_edge_cached(
            ["https://x/hit"], lambda url: {"cf-cache-status": "hit"}
        )


def test_head_sends_a_named_user_agent_and_lowercases_headers() -> None:
    script = _load_script()
    seen: list[Any] = []

    class _Response:
        headers = {"CF-Cache-Status": "DYNAMIC", "Content-Length": "3"}

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, *args: object) -> None:
            return None

    def opener(req: Any, timeout: float) -> _Response:
        seen.append(req)
        return _Response()

    headers = script.http_head("https://x/baseball.ducklake", opener)
    assert headers == {"cf-cache-status": "DYNAMIC", "content-length": "3"}
    assert seen[0].get_method() == "HEAD"
    assert seen[0].get_header("User-agent") == script.VERIFY_USER_AGENT
    assert "urllib" not in script.VERIFY_USER_AGENT
