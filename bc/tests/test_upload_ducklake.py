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
    script.cloudflare_purge(["https://x/a", "https://x/b"], "zone", api, [])
    assert api.writes == [
        ("POST", "/zones/zone/purge_cache", {"files": ["https://x/a", "https://x/b"]})
    ]


def test_purge_sends_one_entry_per_origin_after_each_plain_url() -> None:
    script = _load_script()
    api = _FakeCloudflare([])

    script.cloudflare_purge(
        ["https://x/a", "https://x/b"],
        "zone",
        api,
        ["https://baseball.computer", "http://localhost:4173"],
    )

    assert api.writes == [
        (
            "POST",
            "/zones/zone/purge_cache",
            {
                "files": [
                    "https://x/a",
                    {
                        "url": "https://x/a",
                        "headers": {"Origin": "https://baseball.computer"},
                    },
                    {
                        "url": "https://x/a",
                        "headers": {"Origin": "http://localhost:4173"},
                    },
                    "https://x/b",
                    {
                        "url": "https://x/b",
                        "headers": {"Origin": "https://baseball.computer"},
                    },
                    {
                        "url": "https://x/b",
                        "headers": {"Origin": "http://localhost:4173"},
                    },
                ]
            },
        )
    ]


def test_purge_origins_default_to_the_site_and_local_preview() -> None:
    script = _load_script()

    assert script.parse_args([]).purge_origins == [
        "https://baseball.computer",
        "http://localhost:4173",
    ]
    assert script.parse_args(["--purge-origin", "https://x"]).purge_origins == [
        "https://x"
    ]
    assert script.DEFAULT_PURGE_ORIGINS == (
        "https://baseball.computer",
        "http://localhost:4173",
    )


def test_credentials_file_fills_only_absent_or_empty_variables(tmp_path: Path) -> None:
    script = _load_script()
    credentials = tmp_path / "cloudflare.env"
    credentials.write_text(
        "# Cloudflare\n"
        "\n"
        "CLOUDFLARE_API_TOKEN=from-file\n"
        "export CLOUDFLARE_ZONE_ID='quoted-zone'\n"
        '  R2_ACCOUNT_ID = "spaced"  \n'
        "R2_ACCESS_KEY_ID=already-set\n"
        "R2_SECRET_ACCESS_KEY=\n"
        "not an assignment\n",
        encoding="utf-8",
    )
    environ = {"CLOUDFLARE_API_TOKEN": "", "R2_ACCESS_KEY_ID": "from-environment"}

    loaded = script.load_credentials_file(credentials, environ)

    assert loaded == ["CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ZONE_ID", "R2_ACCOUNT_ID"]
    assert environ == {
        "CLOUDFLARE_API_TOKEN": "from-file",
        "CLOUDFLARE_ZONE_ID": "quoted-zone",
        "R2_ACCOUNT_ID": "spaced",
        "R2_ACCESS_KEY_ID": "from-environment",
    }


def test_credentials_file_missing_is_not_an_error(tmp_path: Path) -> None:
    script = _load_script()
    environ: dict[str, str] = {}

    assert script.load_credentials_file(tmp_path / "absent.env", environ) == []
    assert environ == {}


def test_credentials_file_path_prefers_the_environment_override(
    tmp_path: Path,
) -> None:
    script = _load_script()
    override = tmp_path / "other.env"

    assert script.credentials_file_path({}) == script.DEFAULT_CREDENTIALS_FILE
    assert script.DEFAULT_CREDENTIALS_FILE.name == "cloudflare.env"
    assert (
        script.credentials_file_path({script.CREDENTIALS_FILE_VAR: str(override)})
        == override
    )


def test_metadata_cache_only_fixes_the_rule_and_purges_without_uploading(
    tmp_path: Path,
) -> None:
    script = _load_script()
    version_file = tmp_path / "data_version.txt"
    version_file.write_text("9\n", encoding="utf-8")
    script.DATA_VERSION_FILE = version_file
    api = _FakeCloudflare([])
    headed: list[str] = []

    def head(url: str) -> dict[str, str]:
        headed.append(url)
        return {"cf-cache-status": "DYNAMIC"}

    def uploader_factory(args: Any, prefix: str) -> Any:
        raise AssertionError(f"built an uploader for {prefix}")

    exit_code = script.run(
        script.parse_args(["--metadata-cache-only"]),
        lambda: ("zone", api),
        head,
        uploader_factory,
    )

    urls = script.metadata_urls("baseball/v9")
    assert exit_code == 0
    assert len(urls) == 3
    assert headed == urls
    assert api.writes == [
        (
            "POST",
            "/zones/zone/rulesets/rs1/rules",
            script.desired_metadata_cache_rule(),
        ),
        (
            "POST",
            "/zones/zone/purge_cache",
            {"files": script.purge_files(urls, list(script.DEFAULT_PURGE_ORIGINS))},
        ),
    ]


def test_metadata_urls_match_the_objects_the_upload_publishes(tmp_path: Path) -> None:
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

    catalog_url, metadata_url, packet_url, _ = script.upload_artifact(
        "baseball/v9", _HeaderRecordingUploader(), workers=1
    )

    assert script.metadata_urls("baseball/v9") == [
        catalog_url,
        metadata_url,
        packet_url,
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


class _FakeStore:
    def __init__(self, keys: list[str]) -> None:
        self.keys = list(keys)
        self.listed: list[str] = []
        self.deleted: list[str] = []

    def list_keys(self, prefix: str) -> list[str]:
        self.listed.append(prefix)
        return [key for key in self.keys if key.startswith(prefix)]

    def delete_keys(self, keys: list[str]) -> None:
        self.deleted.extend(keys)
        self.keys = [key for key in self.keys if key not in set(keys)]


def test_prune_deletes_only_unreferenced_data_files_under_the_prefix() -> None:
    script = _load_script()
    data = "baseball/v9/bc_publish_data"
    store = _FakeStore(
        [
            f"{data}/main_models/a/new.parquet",
            f"{data}/main_models/a/old.parquet",
            f"{data}/main_models/dropped/old.parquet",
            "baseball/v9/baseball.ducklake",
            "baseball/v8/bc_publish_data/main_models/a/old.parquet",
            "event/events.parquet",
        ]
    )
    current = {f"{data}/main_models/a/new.parquet", f"{data}/main_models/b/new.parquet"}

    pruned = script.prune_stale_data(store, "baseball/v9", current)

    assert pruned == 2
    assert store.listed == [f"{data}/"]
    assert sorted(store.deleted) == [
        f"{data}/main_models/a/old.parquet",
        f"{data}/main_models/dropped/old.parquet",
    ]
    assert set(store.keys) == {
        f"{data}/main_models/a/new.parquet",
        "baseball/v9/baseball.ducklake",
        "baseball/v8/bc_publish_data/main_models/a/old.parquet",
        "event/events.parquet",
    }


@pytest.mark.parametrize(
    "current",
    [set(), {"baseball/v8/bc_publish_data/x.parquet"}],
)
def test_prune_refuses_an_empty_or_foreign_current_set(current: set[str]) -> None:
    script = _load_script()
    store = _FakeStore(["baseball/v9/bc_publish_data/x.parquet"])

    with pytest.raises(SystemExit, match="refusing to prune"):
        script.prune_stale_data(store, "baseball/v9", current)

    assert store.deleted == []
    assert store.listed == []


class _PagedS3Client:
    def __init__(self, keys: list[str], page_size: int) -> None:
        self.keys = keys
        self.page_size = page_size
        self.delete_batches: list[list[str]] = []

    def list_objects_v2(self, **kwargs: str) -> dict[str, Any]:
        matching = [key for key in self.keys if key.startswith(kwargs["Prefix"])]
        start = int(kwargs.get("ContinuationToken", "0"))
        page = matching[start : start + self.page_size]
        end = start + len(page)
        response: dict[str, Any] = {"Contents": [{"Key": key} for key in page]}
        if end < len(matching):
            response["IsTruncated"] = True
            response["NextContinuationToken"] = str(end)
        return response

    def delete_objects(self, *, Bucket: str, Delete: dict[str, Any]) -> dict[str, Any]:
        assert Bucket == "timeball"
        self.delete_batches.append([item["Key"] for item in Delete["Objects"]])
        return {}


def test_boto3_store_follows_pagination_and_batches_deletes() -> None:
    script = _load_script()
    keys = [f"p/{i:05d}.parquet" for i in range(2_345)]
    client = _PagedS3Client(keys + ["q/other.parquet"], page_size=1_000)
    store = script.Boto3ObjectStore(client)

    listed = store.list_keys("p/")
    store.delete_keys(listed)

    assert listed == keys
    assert [len(batch) for batch in client.delete_batches] == [1_000, 1_000, 345]
    assert [key for batch in client.delete_batches for key in batch] == keys


def test_boto3_store_fails_on_delete_errors() -> None:
    script = _load_script()

    class _FailingClient(_PagedS3Client):
        def delete_objects(
            self, *, Bucket: str, Delete: dict[str, Any]
        ) -> dict[str, Any]:
            return {
                "Errors": [{"Key": Delete["Objects"][0]["Key"], "Code": "AccessDenied"}]
            }

    store = script.Boto3ObjectStore(_FailingClient([], page_size=10))

    with pytest.raises(SystemExit, match="R2 delete failed for 1 objects"):
        store.delete_keys(["p/a.parquet"])


def _publish_fixture(script: Any, tmp_path: Path) -> None:
    catalog_path = tmp_path / "bc_publish.ducklake"
    data_path = tmp_path / "bc_publish_data"
    _create_catalog(catalog_path, data_path)
    current = data_path / "main_models" / "players" / "current.parquet"
    current.parent.mkdir(parents=True, exist_ok=True)
    current.write_bytes(b"PAR1")
    packet = tmp_path / "baseball.lsf"
    packet.write_text(MINIMAL_PACKET, encoding="utf-8")
    version_file = tmp_path / "data_version.txt"
    version_file.write_text("9\n", encoding="utf-8")
    script.CATALOG_PATH = catalog_path
    script.DATA_PATH = data_path
    script.PACKET_PATH = packet
    script.CATALOG_METADATA_PATH = tmp_path / "catalog.json"
    script.DATA_VERSION_FILE = version_file


def test_run_prunes_after_the_catalog_upload_and_purge(tmp_path: Path) -> None:
    script = _load_script()
    _publish_fixture(script, tmp_path)
    events: list[str] = []
    uploader = _HeaderRecordingUploader()
    stale = "baseball/v9/bc_publish_data/main_models/gone/old.parquet"
    kept = "baseball/v9/bc_publish_data/main_models/players/current.parquet"

    class _OrderedStore(_FakeStore):
        def delete_keys(self, keys: list[str]) -> None:
            events.append("prune")
            super().delete_keys(keys)

    store = _OrderedStore([stale, kept])

    def head(url: str) -> dict[str, str]:
        events.append("head")
        return {"cf-cache-status": "DYNAMIC"}

    exit_code = script.run(
        script.parse_args(["--skip-purge", "--skip-cache-rule"]),
        lambda: ("zone", _FakeCloudflare([])),
        head,
        lambda args, prefix: uploader,
        lambda: store,
    )

    uploaded = {key for key, _, _ in uploader.calls}
    assert exit_code == 0
    assert "baseball/v9/baseball.ducklake" in uploaded
    assert events[-1] == "prune"
    assert "head" in events[:-1]
    assert store.deleted == [stale]
    assert kept in uploaded
    assert store.keys == [kept]


def test_run_checks_prune_credentials_before_uploading(tmp_path: Path) -> None:
    script = _load_script()
    _publish_fixture(script, tmp_path)
    uploader = _HeaderRecordingUploader()

    def missing_credentials() -> Any:
        raise SystemExit("missing required env var: R2_ACCESS_KEY_ID")

    with pytest.raises(SystemExit, match="R2_ACCESS_KEY_ID"):
        script.run(
            script.parse_args(["--skip-purge", "--skip-cache-rule"]),
            lambda: ("zone", _FakeCloudflare([])),
            lambda url: {"cf-cache-status": "DYNAMIC"},
            lambda args, prefix: uploader,
            missing_credentials,
        )

    assert uploader.calls == []


def test_skip_prune_never_builds_the_store(tmp_path: Path) -> None:
    script = _load_script()
    _publish_fixture(script, tmp_path)
    uploader = _HeaderRecordingUploader()

    def store_factory() -> Any:
        raise AssertionError("built an object store")

    exit_code = script.run(
        script.parse_args(["--skip-purge", "--skip-cache-rule", "--skip-prune"]),
        lambda: ("zone", _FakeCloudflare([])),
        lambda url: {"cf-cache-status": "DYNAMIC"},
        lambda args, prefix: uploader,
        store_factory,
    )

    assert exit_code == 0
    assert "baseball/v9/baseball.ducklake" in {key for key, _, _ in uploader.calls}
