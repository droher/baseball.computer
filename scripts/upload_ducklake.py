"""Upload the DuckLake publish artifact to R2 and purge Cloudflare cache.

Reads the locally-built artifact produced by scripts/publish_ducklake.py:

    bc/bc_publish.ducklake     SQLite catalog
    bc/bc_publish_data/        parquet data files (relative paths recorded
                               inside the catalog)

DuckLake stores file paths relative to the catalog by default, so the data
directory is uploaded under the same `bc_publish_data/` prefix that the
catalog references. Consumers attach to the catalog URL and parquet reads
resolve against the catalog's parent URL.

R2 prefix: s3://timeball/baseball/v<DATA_VERSION>/
  <prefix>/baseball.ducklake          (renamed catalog, the attach target)
  <prefix>/bc_publish_data/*.parquet  (data files, immutable)

Required env vars:
  R2_ACCOUNT_ID, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
  CLOUDFLARE_API_TOKEN, CLOUDFLARE_ZONE_ID
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

import boto3

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CATALOG_PATH = PROJECT_ROOT / "bc" / "bc_publish.ducklake"
DATA_PATH = PROJECT_ROOT / "bc" / "bc_publish_data"
DATA_VERSION_FILE = PROJECT_ROOT / "bc" / "data_version.txt"

R2_BUCKET = "timeball"
PUBLIC_HOST = "data.baseball.computer"
CATALOG_OBJECT_NAME = "baseball.ducklake"
DATA_DIR_NAME = DATA_PATH.name

DATA_CACHE_CONTROL = "public, max-age=31536000, immutable"
CATALOG_CACHE_CONTROL = "public, max-age=31536000"

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


def r2_client():
    account_id = env("R2_ACCOUNT_ID")
    return boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=env("R2_ACCESS_KEY_ID"),
        aws_secret_access_key=env("R2_SECRET_ACCESS_KEY"),
    )


def upload_file(client, local: Path, key: str, cache_control: str) -> int:
    extra = {"CacheControl": cache_control, "ContentType": "application/octet-stream"}
    client.upload_file(str(local), R2_BUCKET, key, ExtraArgs=extra)
    size = local.stat().st_size
    _log.info("uploaded %s -> s3://%s/%s (%.1f MB)", local.name, R2_BUCKET, key, size / 1e6)
    return size


def upload_artifact(prefix: str) -> tuple[str, int]:
    if not CATALOG_PATH.exists():
        raise SystemExit(f"catalog not found at {CATALOG_PATH} — run publish_ducklake.py first")
    if not DATA_PATH.is_dir():
        raise SystemExit(f"data dir not found at {DATA_PATH}")

    client = r2_client()

    data_files = sorted(p for p in DATA_PATH.rglob("*") if p.is_file())
    _log.info("uploading %d data files under %s/%s/", len(data_files), prefix, DATA_DIR_NAME)
    total_data_bytes = 0
    for f in data_files:
        rel = f.relative_to(DATA_PATH)
        key = f"{prefix}/{DATA_DIR_NAME}/{rel.as_posix()}"
        total_data_bytes += upload_file(client, f, key, DATA_CACHE_CONTROL)

    catalog_key = f"{prefix}/{CATALOG_OBJECT_NAME}"
    catalog_bytes = upload_file(client, CATALOG_PATH, catalog_key, CATALOG_CACHE_CONTROL)

    catalog_url = f"https://{PUBLIC_HOST}/{catalog_key}"
    _log.info(
        "upload summary: data=%.1f MB across %d files, catalog=%.1f MB",
        total_data_bytes / 1e6,
        len(data_files),
        catalog_bytes / 1e6,
    )
    return catalog_url, total_data_bytes + catalog_bytes


def cloudflare_purge(catalog_url: str) -> None:
    zone_id = env("CLOUDFLARE_ZONE_ID")
    token = env("CLOUDFLARE_API_TOKEN")
    req = urllib.request.Request(
        f"https://api.cloudflare.com/client/v4/zones/{zone_id}/purge_cache",
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        data=json.dumps({"files": [catalog_url]}).encode("utf-8"),
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
    _log.info("Cloudflare purged %s", catalog_url)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--skip-purge",
        action="store_true",
        help="Upload but skip the Cloudflare cache-purge step (useful in dry-run).",
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

    catalog_url, _ = upload_artifact(prefix)

    if args.skip_purge:
        _log.info("--skip-purge set; not calling Cloudflare API")
    else:
        cloudflare_purge(catalog_url)

    _log.info("attach URL: ducklake:%s", catalog_url)
    return 0


if __name__ == "__main__":
    sys.exit(main())
