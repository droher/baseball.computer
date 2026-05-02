"""30-line proof: attach a local DuckLake catalog from python-duckdb 1.5.x and
write/read a tiny table. Validates that the fundamental ATTACH path works
before betting Phase 4's publish layer on it.

We do *not* test the dbt-duckdb adapter shape here — `type: ducklake` and
`is_ducklake: true` are documented, but exercising them needs a full dbt
project. That belongs in Phase 4. The point of this spike is to confirm the
DuckDB-native path is healthy.
"""
from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path

import duckdb

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("ducklake_proof")


def main() -> None:
    log.info("duckdb version: %s", duckdb.__version__)
    workdir = Path(tempfile.mkdtemp(prefix="ducklake_proof_"))
    catalog = workdir / "catalog.ducklake"
    storage = workdir / "storage"
    storage.mkdir()

    try:
        con = duckdb.connect(":memory:")
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(
            f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{storage}/')"
        )
        con.execute("USE lake")
        con.execute("CREATE TABLE demo (id INT, season INT, player VARCHAR)")
        con.execute(
            "INSERT INTO demo VALUES (1, 1986, 'marsm001'), (2, 1986, 'santr001')"
        )
        rows = con.execute("SELECT COUNT(*) FROM demo").fetchone()
        log.info("rows in demo: %s", rows[0])
        log.info("storage contents: %s", sorted(p.name for p in storage.rglob("*")))

        # Multi-write sanity (DuckLake snapshots a new state per commit).
        con.execute("INSERT INTO demo VALUES (3, 1987, 'brenb001')")
        log.info(
            "post-second-write rowcount: %s",
            con.execute("SELECT COUNT(*) FROM demo").fetchone()[0],
        )
        con.close()

        # Re-attach in a fresh connection — round-trip the catalog.
        con2 = duckdb.connect(":memory:")
        con2.execute("INSTALL ducklake; LOAD ducklake;")
        con2.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{storage}/')")
        n = con2.execute("SELECT COUNT(*) FROM lake.demo").fetchone()[0]
        log.info("re-attach rowcount: %s", n)
        assert n == 3
        con2.close()
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
