"""Upload the DuckLake publish artifact to R2 and purge Cloudflare cache.

Reads the locally-built artifact produced by scripts/publish_ducklake.py:

    bc/bc_publish.ducklake     DuckDB DuckLake metadata catalog
    bc/bc_publish_data/        parquet data files

The catalog stores the public HTTPS path of `bc_publish_data/`. Consumers
attach to the catalog URL and read parquet files from that path.

R2 prefix: s3://timeball/baseball/v<DATA_VERSION>/
  <prefix>/baseball.ducklake          (renamed catalog, the attach target)
  <prefix>/bc_publish_data/*.parquet  (data files, immutable)

Required env vars:
  boto3: R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
  cache purge: CLOUDFLARE_API_TOKEN, CLOUDFLARE_ZONE_ID

`--wrangler` uses the active Cloudflare OAuth login instead of R2 credentials.
For a fresh version prefix, `--skip-purge` also avoids cache-purge credentials.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import importlib
import json
import logging
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from collections.abc import Callable
from typing import Protocol, cast

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = PROJECT_ROOT / "bc" / "bc_publish.ducklake"
DATA_PATH = PROJECT_ROOT / "bc" / "bc_publish_data"
DATA_VERSION_FILE = PROJECT_ROOT / "bc" / "data_version.txt"
CATALOG_METADATA_PATH = PROJECT_ROOT / "bc" / "catalog.json"

R2_BUCKET = "timeball"
PUBLIC_HOST = "data.baseball.computer"
CATALOG_OBJECT_NAME = "baseball.ducklake"
CATALOG_METADATA_OBJECT_NAME = "catalog.json"
DATA_DIR_NAME = DATA_PATH.name

DATA_CACHE_CONTROL = "public, max-age=31536000, immutable"
CATALOG_CACHE_CONTROL = "public, max-age=0, must-revalidate"
CATALOG_METADATA_CACHE_CONTROL = "public, max-age=0, must-revalidate"
DATA_UPLOAD_WORKERS = 4
MIN_DATA_UPLOAD_WORKERS = 1
MAX_DATA_UPLOAD_WORKERS = 16
UPLOAD_ATTEMPTS = 3
WRANGLER_VERSION = "4.128.0"
WRANGLER_MAX_UPLOAD_BYTES = 300 * 1024 * 1024

_log = logging.getLogger("upload_ducklake")


def env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"missing required env var: {name}")
    return val


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
    boto3 = cast(Boto3Module, importlib.import_module("boto3"))
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
        "WHERE table_catalog = 'bc_publish' AND table_type = 'BASE TABLE' "
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


def upload_artifact(
    prefix: str,
    uploader: ObjectUploader,
    workers: int = DATA_UPLOAD_WORKERS,
) -> tuple[str, str, int]:
    if not CATALOG_PATH.exists():
        raise SystemExit(
            f"catalog not found at {CATALOG_PATH} — run publish_ducklake.py first"
        )
    if not DATA_PATH.is_dir():
        raise SystemExit(f"data dir not found at {DATA_PATH}")

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
    metadata_key = f"{prefix}/{CATALOG_METADATA_OBJECT_NAME}"
    catalog_key = f"{prefix}/{CATALOG_OBJECT_NAME}"
    preflight_uploads(
        uploader,
        data_uploads + [(metadata_path, metadata_key), (CATALOG_PATH, catalog_key)],
    )
    total_data_bytes = upload_data_files(uploader, data_uploads, workers)

    metadata_bytes = upload_file(
        uploader,
        metadata_path,
        metadata_key,
        CATALOG_METADATA_CACHE_CONTROL,
        "application/json",
    )
    catalog_bytes = upload_file(
        uploader,
        CATALOG_PATH,
        catalog_key,
        CATALOG_CACHE_CONTROL,
        "application/octet-stream",
    )

    catalog_url = f"https://{PUBLIC_HOST}/{catalog_key}"
    metadata_url = f"https://{PUBLIC_HOST}/{metadata_key}"
    _log.info(
        "upload summary: data=%.1f MB across %d files, catalog=%.1f MB",
        total_data_bytes / 1e6,
        len(data_files),
        (catalog_bytes + metadata_bytes) / 1e6,
    )
    return catalog_url, metadata_url, total_data_bytes + catalog_bytes + metadata_bytes


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


def cloudflare_purge(urls: list[str]) -> None:
    zone_id = env("CLOUDFLARE_ZONE_ID")
    token = env("CLOUDFLARE_API_TOKEN")
    req = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache",
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps({"files": urls}).encode("utf-8"),
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        raise SystemExit(
            f"Cloudflare purge failed: status={e.code} body={e.read().decode('utf-8', 'replace')}"
        ) from e
    if not body.get("success"):
        raise SystemExit(f"Cloudflare purge failed: body={body}")
    _log.info("Cloudflare purged %s", ", ".join(urls))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        help="Upload but skip the Cloudflare cache-purge step (useful in dry-run).",
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
    parser.add_argument("-v", "--verbose", action="count", default=0)
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    data_version = read_data_version()
    prefix = f"baseball/v{data_version}"
    _log.info("uploading DuckLake artifact under s3://%s/%s/", R2_BUCKET, prefix)

    uploader: ObjectUploader = WranglerUploader() if args.wrangler else r2_client()
    if args.resume:
        uploader = ResumeUploader(uploader, prefix)
    catalog_url, metadata_url, _ = upload_artifact(prefix, uploader, args.workers)

    if args.skip_purge:
        _log.info("--skip-purge set; not calling Cloudflare API")
    else:
        cloudflare_purge([catalog_url, metadata_url])

    _log.info("attach URL: ducklake:%s", catalog_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
