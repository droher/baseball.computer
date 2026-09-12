"""Upload the DuckLake publish artifact to R2 and purge Cloudflare cache.

Reads the locally-built artifact produced by scripts/publish_ducklake.py:

    bc/bc_publish.ducklake     DuckDB DuckLake metadata catalog
    bc/bc_publish_data/        parquet data files

The catalog stores the public HTTPS path of `bc_publish_data/`. Consumers
attach to the catalog URL and read parquet files from that path.

R2 prefix: s3://timeball/baseball/v<DATA_VERSION>/
  <prefix>/baseball.ducklake          (renamed catalog, the attach target)
  <prefix>/catalog.json               (schema sidebar metadata)
  <prefix>/baseball.lsf               (LSF-1 context packet for the site's NL query feature)
  <prefix>/bc_publish_data/*.parquet  (data files, immutable)

Required env vars:
  boto3: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
  Cloudflare cache rule + purge: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ZONE_ID
    (the token needs Cache Rules edit and Cache Purge on the zone)

Variables absent or empty in the environment are read from the credentials
file at BC_CREDENTIALS_FILE, defaulting to
~/.config/baseball.computer/cloudflare.env; a missing file is not an error.

Before uploading, the script makes sure the zone's "DuckLake metadata
revalidation" cache rule covers every metadata object it is about to
publish, so the catalog, schema, and packet are never edge-cached under
the legacy month-long page rule. After the purge it HEADs each metadata
URL and fails if Cloudflare reports a cache HIT. Cloudflare keys cached
responses by request Origin, so the purge sends the plain URL plus one
variant per `--purge-origin`.

`--wrangler` uses the active Cloudflare OAuth login instead of R2 credentials.
For a fresh version prefix, `--skip-purge` also avoids cache-purge credentials;
`--skip-cache-rule` skips the rule check when no Cloudflare token is available.
`--metadata-cache-only` uploads nothing and only repairs the cache rule and
purges the metadata URLs of the current data version.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib
import importlib.util
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any, Protocol, cast

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = PROJECT_ROOT / "bc" / "bc_publish.ducklake"
DATA_PATH = PROJECT_ROOT / "bc" / "bc_publish_data"
DATA_VERSION_FILE = PROJECT_ROOT / "bc" / "data_version.txt"
CATALOG_METADATA_PATH = PROJECT_ROOT / "bc" / "catalog.json"
PACKET_PATH = PROJECT_ROOT / "docs" / "llm" / "baseball.lsf"
GENERATOR_PATH = PROJECT_ROOT / "scripts" / "generate_llm_context.py"

R2_BUCKET = "timeball"
PUBLIC_HOST = "data.baseball.computer"
CATALOG_OBJECT_NAME = "baseball.ducklake"
CATALOG_METADATA_OBJECT_NAME = "catalog.json"
PACKET_OBJECT_NAME = "baseball.lsf"
DATA_DIR_NAME = DATA_PATH.name

DATA_CACHE_CONTROL = "public, max-age=31536000, immutable"
CATALOG_CACHE_CONTROL = "public, max-age=0, must-revalidate"
CATALOG_METADATA_CACHE_CONTROL = "public, max-age=0, must-revalidate"
PACKET_CACHE_CONTROL = CATALOG_METADATA_CACHE_CONTROL
PACKET_CONTENT_TYPE = "text/plain; charset=utf-8"
DATA_UPLOAD_WORKERS = 4
MIN_DATA_UPLOAD_WORKERS = 1
MAX_DATA_UPLOAD_WORKERS = 16
UPLOAD_ATTEMPTS = 3
CLOUDFLARE_API = "https://api.cloudflare.com/client/v4"
PUBLIC_PATH_ROOT = "/baseball/"
CACHE_RULE_DESCRIPTION = "DuckLake metadata revalidation"
CACHE_RULE_PHASE = "http_request_cache_settings"
CACHE_RULE_ACTION = "set_cache_settings"
CACHE_RULE_ACTION_PARAMETERS: dict[str, Any] = {
    "cache": False,
    "browser_ttl": {"mode": "respect_origin"},
}
EDGE_CACHE_STATUS_HEADER = "cf-cache-status"
VERIFY_USER_AGENT = "baseball.computer-upload-verify/1.0"
WRANGLER_VERSION = "4.128.0"
WRANGLER_MAX_UPLOAD_BYTES = 300 * 1024 * 1024
CREDENTIALS_FILE_VAR = "BC_CREDENTIALS_FILE"
DEFAULT_CREDENTIALS_FILE = (
    Path.home() / ".config" / "baseball.computer" / "cloudflare.env"
)
DEFAULT_PURGE_ORIGINS = ("https://baseball.computer", "http://localhost:4173")

_log = logging.getLogger("upload_ducklake")


def env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"missing required env var: {name}")
    return val


def credentials_file_path(environ: Mapping[str, str]) -> Path:
    override = environ.get(CREDENTIALS_FILE_VAR)
    return Path(override).expanduser() if override else DEFAULT_CREDENTIALS_FILE


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    return value


def load_credentials_file(path: Path, environ: MutableMapping[str, str]) -> list[str]:
    """Fill empty variables from a KEY=value file; return the names set.

    Placeholder lines with no value are ignored, so an unfilled credentials
    file still fails with the missing-variable error rather than an empty one.
    """
    if not path.is_file():
        return []
    loaded: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        entry = line.strip()
        if not entry or entry.startswith("#"):
            continue
        if entry.startswith("export "):
            entry = entry.removeprefix("export ").strip()
        name, separator, value = entry.partition("=")
        name = name.strip()
        setting = _unquote(value.strip())
        if not separator or not name or not setting or environ.get(name):
            continue
        environ[name] = setting
        loaded.append(name)
    return loaded


def read_data_version() -> str:
    text = DATA_VERSION_FILE.read_text().strip()
    if not text.isdigit() or int(text) < 1:
        raise SystemExit(
            f"{DATA_VERSION_FILE} must contain a positive integer, got {text!r}"
        )
    return text


class ObjectUploader(Protocol):
    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int: ...


class RemoteObject(Protocol):
    content_length: int
    etag: str


class S3Client(Protocol):
    def upload_file(
        self,
        filename: str,
        bucket: str,
        key: str,
        ExtraArgs: dict[str, str],
    ) -> None: ...


class Boto3Module(Protocol):
    def client(
        self,
        service_name: str,
        *,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
    ) -> S3Client: ...


class Boto3Uploader:
    def __init__(self, client: S3Client) -> None:
        self.client = client

    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int:
        extra = {"CacheControl": cache_control, "ContentType": content_type}
        self.client.upload_file(str(local), R2_BUCKET, key, ExtraArgs=extra)
        return local.stat().st_size


def wrangler_upload_command(
    local: Path,
    key: str,
    cache_control: str,
    content_type: str,
) -> list[str]:
    return [
        "npx",
        "--yes",
        f"wrangler@{WRANGLER_VERSION}",
        "r2",
        "object",
        "put",
        f"{R2_BUCKET}/{key}",
        "--file",
        str(local),
        "--remote",
        "--cache-control",
        cache_control,
        "--content-type",
        content_type,
    ]


def assert_wrangler_upload_size(size: int, key: str) -> None:
    if size > WRANGLER_MAX_UPLOAD_BYTES:
        raise SystemExit(
            f"{key} is {size} bytes, exceeding Wrangler's single-upload limit "
            f"of {WRANGLER_MAX_UPLOAD_BYTES} bytes"
        )


def preflight_uploads(
    uploader: ObjectUploader,
    files: list[tuple[Path, str]],
) -> None:
    if uses_wrangler(uploader):
        for local, key in files:
            assert_wrangler_upload_size(local.stat().st_size, key)


class WranglerUploader:
    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int:
        size = local.stat().st_size
        assert_wrangler_upload_size(size, key)
        command = wrangler_upload_command(local, key, cache_control, content_type)
        try:
            _ = subprocess.run(command, check=True, text=True, capture_output=True)
        except subprocess.CalledProcessError as error:
            detail = error.stderr.strip() or error.stdout.strip()
            raise RuntimeError(f"Wrangler upload failed for {key}: {detail}") from error
        return size


class RemoteObjectInfo:
    def __init__(self, content_length: int, etag: str) -> None:
        self.content_length = content_length
        self.etag = etag


def remote_object_info(key: str) -> RemoteObjectInfo | None:
    request = urllib.request.Request(
        f"https://{PUBLIC_HOST}/{key}",
        method="HEAD",
        headers={"User-Agent": "baseball.computer-publisher/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            content_length = response.headers.get("Content-Length")
            etag = response.headers.get("ETag")
    except (OSError, ValueError):
        return None
    if content_length is None or etag is None:
        return None
    try:
        return RemoteObjectInfo(int(content_length), etag)
    except ValueError:
        return None


def file_md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as input_file:
        while chunk := input_file.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def remote_matches_file(local: Path, remote: RemoteObject) -> bool:
    etag = remote.etag.strip('"')
    return (
        local.stat().st_size == remote.content_length
        and re.fullmatch(r"[0-9a-fA-F]{32}", etag) is not None
        and file_md5(local) == etag.lower()
    )


class ResumeUploader:
    def __init__(
        self,
        uploader: ObjectUploader,
        prefix: str,
        lookup: Callable[[str], RemoteObjectInfo | None] = remote_object_info,
    ) -> None:
        self.uploader = uploader
        self.data_prefix = f"{prefix}/{DATA_DIR_NAME}/"
        self.lookup = lookup

    def upload(
        self,
        local: Path,
        key: str,
        cache_control: str,
        content_type: str,
    ) -> int:
        if key.startswith(self.data_prefix):
            remote = self.lookup(key)
            if remote is not None and remote_matches_file(local, remote):
                _log.info("upload checkpoint: resumed %s", key)
                return local.stat().st_size
        _log.info("upload checkpoint: uploading %s", key)
        return self.uploader.upload(local, key, cache_control, content_type)


def uses_wrangler(uploader: ObjectUploader) -> bool:
    if isinstance(uploader, WranglerUploader):
        return True
    if isinstance(uploader, ResumeUploader):
        return uses_wrangler(uploader.uploader)
    return False


def r2_client() -> Boto3Uploader:
    account_id = env("R2_ACCOUNT_ID")
    boto3 = cast(Boto3Module, cast(object, importlib.import_module("boto3")))
    return Boto3Uploader(
        boto3.client(
            "s3",
            endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
            aws_access_key_id=env("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
        )
    )


def upload_file(
    uploader: ObjectUploader,
    local: Path,
    key: str,
    cache_control: str,
    content_type: str,
) -> int:
    for attempt in range(1, UPLOAD_ATTEMPTS + 1):
        try:
            size = uploader.upload(local, key, cache_control, content_type)
        except SystemExit:
            raise
        except Exception as error:
            if attempt == UPLOAD_ATTEMPTS:
                raise SystemExit(
                    f"upload failed for {key} after {UPLOAD_ATTEMPTS} attempts: {error}"
                ) from error
            _log.warning(
                "upload checkpoint: %s attempt %d/%d failed: %s",
                key,
                attempt,
                UPLOAD_ATTEMPTS,
                error,
            )
        else:
            _log.info(
                "uploaded %s -> s3://%s/%s (%.1f MB)",
                local.name,
                R2_BUCKET,
                key,
                size / 1e6,
            )
            return size
    raise RuntimeError(f"upload attempt loop exhausted for {key}")


def worker_count(value: str) -> int:
    workers = int(value)
    if not MIN_DATA_UPLOAD_WORKERS <= workers <= MAX_DATA_UPLOAD_WORKERS:
        raise argparse.ArgumentTypeError(
            f"workers must be between {MIN_DATA_UPLOAD_WORKERS} and {MAX_DATA_UPLOAD_WORKERS}"
        )
    return workers


def sql_literal(value: str) -> str:
    return value.replace("'", "''")


def catalog_metadata(
    catalog_path: Path = CATALOG_PATH,
    data_path: Path = DATA_PATH,
) -> dict[str, object]:
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        "ATTACH 'ducklake:"
        + sql_literal(str(catalog_path))
        + "' AS bc_publish (DATA_PATH '"
        + sql_literal(str(data_path))
        + "/', OVERRIDE_DATA_PATH true, READ_ONLY)"
    )
    tables = con.execute(
        "SELECT table_schema, table_name FROM information_schema.tables "
        "WHERE table_catalog = 'bc_publish' AND table_type IN ('BASE TABLE', 'VIEW') "
        "ORDER BY table_schema, table_name"
    ).fetchall()
    nodes: dict[str, object] = {}
    for schema, table in tables:
        columns = con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog = 'bc_publish' AND table_schema = ? AND table_name = ? "
            "ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        nodes[f"{schema}.{table}"] = {
            "metadata": {"schema": schema, "name": table},
            "columns": {name: {"name": name} for (name,) in columns},
        }
    con.close()
    return {"nodes": nodes}


def write_catalog_metadata(
    catalog_path: Path = CATALOG_PATH,
    data_path: Path = DATA_PATH,
    output_path: Path = CATALOG_METADATA_PATH,
) -> Path:
    output_path.write_text(
        json.dumps(catalog_metadata(catalog_path, data_path), sort_keys=True),
        encoding="utf-8",
    )
    return output_path


def assert_packet_valid(packet_path: Path = PACKET_PATH) -> None:
    """Refuse to upload a packet the LSF-1 validator rejects."""
    if not packet_path.exists():
        raise SystemExit(
            f"packet not found at {packet_path} — run publish_ducklake.py first"
        )
    spec = importlib.util.spec_from_file_location(
        "generate_llm_context", GENERATOR_PATH
    )
    if spec is None or spec.loader is None:
        raise SystemExit(f"cannot load {GENERATOR_PATH}")
    generator = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = generator
    spec.loader.exec_module(generator)
    validate = cast(Callable[[str], object], generator.validate)
    report = validate(packet_path.read_text(encoding="utf-8"))
    violations = cast(list[str], getattr(report, "violations"))
    if violations:
        listing = "\n  ".join(violations)
        raise SystemExit(f"packet {packet_path} fails LSF-1 validation:\n  {listing}")


def metadata_keys(prefix: str) -> tuple[str, str, str]:
    return (
        f"{prefix}/{CATALOG_OBJECT_NAME}",
        f"{prefix}/{CATALOG_METADATA_OBJECT_NAME}",
        f"{prefix}/{PACKET_OBJECT_NAME}",
    )


def metadata_urls(prefix: str) -> list[str]:
    return [f"https://{PUBLIC_HOST}/{key}" for key in metadata_keys(prefix)]


def upload_artifact(
    prefix: str,
    uploader: ObjectUploader,
    workers: int = DATA_UPLOAD_WORKERS,
) -> tuple[str, str, str, int]:
    if not CATALOG_PATH.exists():
        raise SystemExit(
            f"catalog not found at {CATALOG_PATH} — run publish_ducklake.py first"
        )
    if not DATA_PATH.is_dir():
        raise SystemExit(f"data dir not found at {DATA_PATH}")
    assert_packet_valid()

    data_files = sorted(p for p in DATA_PATH.rglob("*") if p.is_file())
    _log.info(
        "uploading %d data files under %s/%s/", len(data_files), prefix, DATA_DIR_NAME
    )
    data_uploads = [
        (
            file,
            f"{prefix}/{DATA_DIR_NAME}/{file.relative_to(DATA_PATH).as_posix()}",
        )
        for file in data_files
    ]
    metadata_path = write_catalog_metadata()
    catalog_key, metadata_key, packet_key = metadata_keys(prefix)
    preflight_uploads(
        uploader,
        data_uploads
        + [
            (metadata_path, metadata_key),
            (PACKET_PATH, packet_key),
            (CATALOG_PATH, catalog_key),
        ],
    )
    total_data_bytes = upload_data_files(uploader, data_uploads, workers)

    metadata_bytes = upload_file(
        uploader,
        metadata_path,
        metadata_key,
        CATALOG_METADATA_CACHE_CONTROL,
        "application/json",
    )
    packet_bytes = upload_file(
        uploader,
        PACKET_PATH,
        packet_key,
        PACKET_CACHE_CONTROL,
        PACKET_CONTENT_TYPE,
    )
    catalog_bytes = upload_file(
        uploader,
        CATALOG_PATH,
        catalog_key,
        CATALOG_CACHE_CONTROL,
        "application/octet-stream",
    )

    catalog_url, metadata_url, packet_url = metadata_urls(prefix)
    _log.info(
        "upload summary: data=%.1f MB across %d files, catalog=%.1f MB, packet=%.1f MB",
        total_data_bytes / 1e6,
        len(data_files),
        (catalog_bytes + metadata_bytes) / 1e6,
        packet_bytes / 1e6,
    )
    total = total_data_bytes + catalog_bytes + metadata_bytes + packet_bytes
    return catalog_url, metadata_url, packet_url, total


def upload_data_files(
    uploader: ObjectUploader,
    data_uploads: list[tuple[Path, str]],
    workers: int,
) -> int:
    executor = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
    pending_uploads = iter(data_uploads)

    def submit_next() -> concurrent.futures.Future[int] | None:
        try:
            file, key = next(pending_uploads)
        except StopIteration:
            return None
        return executor.submit(
            upload_file,
            uploader,
            file,
            key,
            DATA_CACHE_CONTROL,
            "application/octet-stream",
        )

    futures = {future for _ in range(workers) if (future := submit_next()) is not None}
    try:
        total = 0
        while futures:
            completed, _ = concurrent.futures.wait(
                futures,
                return_when=concurrent.futures.FIRST_COMPLETED,
            )
            for future in completed:
                futures.remove(future)
                total += future.result()
                if next_future := submit_next():
                    futures.add(next_future)
    except BaseException:
        for future in futures:
            _ = future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    executor.shutdown(wait=True)
    return total


class CloudflareNotFound(Exception):
    pass


class CloudflareRequest(Protocol):
    def __call__(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]: ...


def cloudflare_api(token: str) -> CloudflareRequest:
    """Bind a bearer token to a JSON request function against the v4 API."""

    def request(
        method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        req = urllib.request.Request(
            f"{CLOUDFLARE_API}{path}",
            method=method,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            data=None if body is None else json.dumps(body).encode("utf-8"),
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                payload = cast(dict[str, Any], json.loads(resp.read().decode("utf-8")))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")
            if e.code == 404:
                raise CloudflareNotFound(detail) from e
            raise SystemExit(
                f"Cloudflare {method} {path} failed: status={e.code} body={detail}"
            ) from e
        if not payload.get("success"):
            raise SystemExit(f"Cloudflare {method} {path} failed: body={payload}")
        return payload

    return request


def cloudflare_credentials() -> tuple[str, CloudflareRequest]:
    return env("CLOUDFLARE_ZONE_ID"), cloudflare_api(env("CLOUDFLARE_API_TOKEN"))


def metadata_path_suffixes() -> tuple[str, ...]:
    """Path endings of every revalidating object the upload publishes."""
    return (
        Path(CATALOG_OBJECT_NAME).suffix,
        f"/{CATALOG_METADATA_OBJECT_NAME}",
        Path(PACKET_OBJECT_NAME).suffix,
    )


def metadata_cache_rule_expression() -> str:
    endings = " or ".join(
        f'ends_with(http.request.uri.path, "{suffix}")'
        for suffix in metadata_path_suffixes()
    )
    return (
        f'(http.host eq "{PUBLIC_HOST}" and '
        f'starts_with(http.request.uri.path, "{PUBLIC_PATH_ROOT}") and ({endings}))'
    )


def desired_metadata_cache_rule() -> dict[str, Any]:
    return {
        "description": CACHE_RULE_DESCRIPTION,
        "expression": metadata_cache_rule_expression(),
        "action": CACHE_RULE_ACTION,
        "action_parameters": CACHE_RULE_ACTION_PARAMETERS,
        "enabled": True,
    }


def _rule_matches(rule: dict[str, Any], desired: dict[str, Any]) -> bool:
    return (
        rule.get("expression") == desired["expression"]
        and rule.get("action") == desired["action"]
        and rule.get("action_parameters") == desired["action_parameters"]
        and bool(rule.get("enabled", True))
    )


def ensure_metadata_cache_rule(zone_id: str, request: CloudflareRequest) -> str:
    """Create or update the zone's metadata cache rule; return what happened.

    Only the rule named CACHE_RULE_DESCRIPTION is ever written. Every other
    rule in the cache-settings ruleset is left exactly as it is.
    """
    desired = desired_metadata_cache_rule()
    entrypoint = f"/zones/{zone_id}/rulesets/phases/{CACHE_RULE_PHASE}/entrypoint"
    try:
        result = cast(dict[str, Any], request("GET", entrypoint)["result"])
    except CloudflareNotFound:
        _ = request("PUT", entrypoint, {"rules": [desired]})
        _log.info("created cache ruleset with rule %r", CACHE_RULE_DESCRIPTION)
        return "created"
    ruleset_id = str(result["id"])
    rules = cast(list[dict[str, Any]], result.get("rules") or [])
    matches = [r for r in rules if r.get("description") == CACHE_RULE_DESCRIPTION]
    if len(matches) > 1:
        raise SystemExit(
            f"zone has {len(matches)} cache rules named {CACHE_RULE_DESCRIPTION!r}; "
            "remove the duplicates in the Cloudflare dashboard first"
        )
    if not matches:
        _ = request("POST", f"/zones/{zone_id}/rulesets/{ruleset_id}/rules", desired)
        _log.info(
            "created cache rule %r: %s", CACHE_RULE_DESCRIPTION, desired["expression"]
        )
        return "created"
    rule = matches[0]
    if _rule_matches(rule, desired):
        _log.info(
            "cache rule %r already covers %s",
            CACHE_RULE_DESCRIPTION,
            metadata_path_suffixes(),
        )
        return "unchanged"
    _log.info(
        "updating cache rule %r\n  was: expression=%s action_parameters=%s enabled=%s\n  now: expression=%s action_parameters=%s",
        CACHE_RULE_DESCRIPTION,
        rule.get("expression"),
        rule.get("action_parameters"),
        rule.get("enabled", True),
        desired["expression"],
        desired["action_parameters"],
    )
    _ = request(
        "PATCH", f"/zones/{zone_id}/rulesets/{ruleset_id}/rules/{rule['id']}", desired
    )
    return "updated"


class HeadRequest(Protocol):
    def __call__(self, url: str) -> dict[str, str]: ...


def http_head(
    url: str, opener: Callable[..., Any] = urllib.request.urlopen
) -> dict[str, str]:
    """HEAD the public URL with a named user agent; Cloudflare answers 403
    to the default urllib one."""
    req = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": VERIFY_USER_AGENT}
    )
    with opener(req, timeout=30) as resp:
        return {k.lower(): v for k, v in resp.headers.items()}


def assert_metadata_not_edge_cached(
    urls: list[str], head: HeadRequest = http_head
) -> None:
    """Fail when Cloudflare served a metadata object from its edge cache."""
    for url in urls:
        status = head(url).get(EDGE_CACHE_STATUS_HEADER, "")
        _log.info("%s %s=%s", url, EDGE_CACHE_STATUS_HEADER, status or "absent")
        if status.upper() == "HIT":
            raise SystemExit(
                f"{url} was served from the Cloudflare edge cache; the "
                f"{CACHE_RULE_DESCRIPTION!r} rule is not covering it"
            )


def purge_files(urls: list[str], origins: list[str]) -> list[str | dict[str, Any]]:
    """Cloudflare caches per request Origin, so purge one entry per origin."""
    files: list[str | dict[str, Any]] = []
    for url in urls:
        files.append(url)
        files.extend({"url": url, "headers": {"Origin": origin}} for origin in origins)
    return files


def cloudflare_purge(
    urls: list[str],
    zone_id: str,
    request: CloudflareRequest,
    origins: list[str],
) -> None:
    files = purge_files(urls, origins)
    _ = request("POST", f"/zones/{zone_id}/purge_cache", {"files": files})
    _log.info(
        "Cloudflare purged %d entries for %s across origins %s",
        len(files),
        ", ".join(urls),
        ", ".join(origins) or "none",
    )


CredentialsProvider = Callable[[], tuple[str, CloudflareRequest]]
UploaderFactory = Callable[[argparse.Namespace, str], ObjectUploader]


def build_uploader(args: argparse.Namespace, prefix: str) -> ObjectUploader:
    uploader: ObjectUploader = WranglerUploader() if args.wrangler else r2_client()
    if args.resume:
        uploader = ResumeUploader(uploader, prefix)
    return uploader


def run(
    args: argparse.Namespace,
    credentials: CredentialsProvider = cloudflare_credentials,
    head: HeadRequest = http_head,
    uploader_factory: UploaderFactory = build_uploader,
) -> int:
    prefix = f"baseball/v{read_data_version()}"
    urls = metadata_urls(prefix)

    if args.skip_cache_rule:
        _log.info("--skip-cache-rule set; not checking the Cloudflare cache rule")
    else:
        zone_id, request = credentials()
        _ = ensure_metadata_cache_rule(zone_id, request)

    if args.metadata_cache_only:
        _log.info(
            "--metadata-cache-only set; uploading nothing under s3://%s/%s/",
            R2_BUCKET,
            prefix,
        )
    else:
        _log.info("uploading DuckLake artifact under s3://%s/%s/", R2_BUCKET, prefix)
        _ = upload_artifact(prefix, uploader_factory(args, prefix), args.workers)

    if args.skip_purge:
        _log.info("--skip-purge set; not calling Cloudflare API")
    else:
        zone_id, request = credentials()
        cloudflare_purge(urls, zone_id, request, args.purge_origins)
    assert_metadata_not_edge_cached(urls, head)

    _log.info("attach URL: ducklake:%s", urls[0])
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        help="Upload but skip the Cloudflare cache-purge step (useful in dry-run).",
    )
    parser.add_argument(
        "--skip-cache-rule",
        action="store_true",
        help="Do not check or update the Cloudflare metadata cache rule before uploading.",
    )
    parser.add_argument(
        "--wrangler",
        action="store_true",
        help="Upload with Wrangler using the active Cloudflare OAuth login.",
    )
    parser.add_argument(
        "--workers",
        type=worker_count,
        default=DATA_UPLOAD_WORKERS,
        help=f"Concurrent data uploads ({MIN_DATA_UPLOAD_WORKERS}-{MAX_DATA_UPLOAD_WORKERS}).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip matching immutable data files already present at the public URL.",
    )
    parser.add_argument(
        "--metadata-cache-only",
        action="store_true",
        help="Upload nothing; only fix the cache rule and purge the metadata URLs.",
    )
    parser.add_argument(
        "--purge-origin",
        metavar="ORIGIN",
        action="append",
        dest="purge_origins",
        help=(
            "Also purge the metadata URLs as seen from this Origin (repeatable; "
            f"default {' '.join(DEFAULT_PURGE_ORIGINS)})."
        ),
    )
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)
    if not args.purge_origins:
        args.purge_origins = list(DEFAULT_PURGE_ORIGINS)
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    path = credentials_file_path(os.environ)
    loaded = load_credentials_file(path, os.environ)
    if loaded:
        _log.info("loaded %s from %s", ", ".join(loaded), path)

    return run(args)


if __name__ == "__main__":
    sys.exit(main())
