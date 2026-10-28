"""EL step: NOAA Fisheries FOSS commercial landings -> raw schema in DuckDB.

Design (in the spirit of a managed connector such as Fivetran, done in plain Python):
* Partitioned by landing year. Each year is one "sync unit".
* Incremental: a watermark (max year successfully loaded) decides which new
  years to fetch, and a look-back window re-checks the most recent years because
  NOAA revises recent landings.
* Idempotent: every partition is fingerprinted (SHA-256 of its canonical JSON).
  If the fingerprint equals the last successful load, the partition is skipped;
  otherwise it is replaced atomically (DELETE + INSERT inside one transaction).
* Every partition attempt is written to raw._load_log (run id, year, rows,
  sha256, action, UTC timestamp, source URL).
* The raw API response is landed unchanged under data/raw/ (out of git).
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import logging
import time
import uuid
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import quote

import duckdb
import pandas as pd
import requests

API_URL = "https://apps-st.fisheries.noaa.gov/ods/foss/landings/"
PAGE_LIMIT = 10000
RAW_TABLE = "raw.foss_landings"
LOG_TABLE = "raw._load_log"
FIELDS = [
    "tsn", "ts_afs_name", "ts_scientific_name", "region_name", "state_name",
    "year", "pounds", "dollars", "source", "collection",
]

log = logging.getLogger("extract_load")

Fetcher = Callable[[int], list[dict]]


def year_url(year: int, offset: int = 0) -> str:
    q = quote(json.dumps({"year": year, "collection": "Commercial"}, separators=(",", ":")))
    return f"{API_URL}?q={q}&limit={PAGE_LIMIT}&offset={offset}"


def fetch_year(year: int, session: requests.Session | None = None, retries: int = 3) -> list[dict]:
    """Fetch every commercial landings row for one year, following ORDS pagination."""
    s = session or requests.Session()
    rows: list[dict] = []
    offset = 0
    while True:
        url = year_url(year, offset)
        for attempt in range(1, retries + 1):
            try:
                r = s.get(url, timeout=120)
                r.raise_for_status()
                payload = r.json()
                break
            except (requests.RequestException, ValueError):
                if attempt == retries:
                    raise
                time.sleep(2 ** attempt)
        items = payload.get("items", [])
        rows.extend(items)
        if not payload.get("hasMore"):
            return rows
        offset += len(items)


def source_row_id(item: dict) -> str | None:
    """ORDS exposes the Oracle ROWID in the self link; keep it as lineage."""
    for link in item.get("links") or []:
        if link.get("rel") == "self":
            return link["href"].rstrip("/").rsplit("/", 1)[-1]
    return None


def normalise(items: Iterable[dict]) -> list[dict]:
    out = []
    for it in items:
        rec = {k: it.get(k) for k in FIELDS}
        rec["source_row_id"] = source_row_id(it)
        out.append(rec)
    out.sort(key=lambda r: json.dumps(r, sort_keys=True, default=str))
    return out


def fingerprint(rows: list[dict]) -> str:
    blob = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


def ensure_schema(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("create schema if not exists raw")
    con.execute(f"""
        create table if not exists {RAW_TABLE} (
            tsn varchar, ts_afs_name varchar, ts_scientific_name varchar,
            region_name varchar, state_name varchar, year integer,
            pounds double, dollars double, source varchar, collection varchar,
            source_row_id varchar,
            _partition_sha256 varchar, _batch_id varchar, _loaded_at timestamp
        )""")
    con.execute(f"""
        create table if not exists {LOG_TABLE} (
            run_id varchar, year integer, action varchar, rows_loaded integer,
            partition_sha256 varchar, source_url varchar,
            started_at timestamp, finished_at timestamp, message varchar
        )""")


def last_hash(con, year: int) -> str | None:
    r = con.execute(
        f"""select partition_sha256 from {LOG_TABLE}
            where year = ? and action in ('inserted','replaced','unchanged')
            order by finished_at desc limit 1""", [year]).fetchone()
    return r[0] if r else None


def watermark(con) -> int | None:
    r = con.execute(
        f"select max(year) from {LOG_TABLE} where action in ('inserted','replaced','unchanged')"
    ).fetchone()
    return r[0] if r and r[0] is not None else None


def years_to_sync(wm: int | None, start_year: int, end_year: int, lookback: int) -> list[int]:
    if wm is None:
        return list(range(start_year, end_year + 1))
    first = max(start_year, wm - lookback + 1)
    return list(range(first, end_year + 1))


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)


def load_partition(con, year: int, rows: list[dict], run_id: str, started: dt.datetime) -> str:
    sha = fingerprint(rows)
    prev = last_hash(con, year)
    loaded_at = utcnow()
    if prev == sha:
        action, n = "unchanged", 0
    elif not rows:
        action, n = "empty", 0
    else:
        action = "replaced" if prev else "inserted"
        n = len(rows)
        con.execute("begin transaction")
        try:
            con.execute(f"delete from {RAW_TABLE} where year = ?", [year])
            df = pd.DataFrame(rows, columns=FIELDS + ["source_row_id"])
            df["_partition_sha256"], df["_batch_id"], df["_loaded_at"] = sha, run_id, loaded_at
            con.register("_incoming", df)
            con.execute(f"insert into {RAW_TABLE} select * from _incoming")
            con.unregister("_incoming")
            con.execute("commit")
        except Exception:
            con.execute("rollback")
            raise
    con.execute(
        f"insert into {LOG_TABLE} values (?,?,?,?,?,?,?,?,?)",
        [run_id, year, action, n, sha, year_url(year), started, utcnow(), None],
    )
    return action


def run(db_path: str | Path, start_year: int, end_year: int, lookback: int = 2,
        fetcher: Fetcher = fetch_year, landing_dir: Path | None = None) -> dict:
    con = duckdb.connect(str(db_path))
    ensure_schema(con)
    run_id = uuid.uuid4().hex[:12]
    wm = watermark(con)
    years = years_to_sync(wm, start_year, end_year, lookback)
    summary: dict[str, list[int]] = {}
    for y in years:
        started = utcnow()
        try:
            items = fetcher(y)
            if landing_dir:
                landing_dir.mkdir(parents=True, exist_ok=True)
                (landing_dir / f"foss_commercial_{y}.json").write_text(json.dumps(items))
            action = load_partition(con, y, normalise(items), run_id, started)
        except Exception as exc:  # log failed job, keep going
            con.execute(f"insert into {LOG_TABLE} values (?,?,?,?,?,?,?,?,?)",
                        [run_id, y, "failed", 0, None, year_url(y), started, utcnow(), str(exc)[:500]])
            action = "failed"
        summary.setdefault(action, []).append(y)
        log.info("year=%s action=%s", y, action)
    total = con.execute(f"select count(*) from {RAW_TABLE}").fetchone()[0]
    con.close()
    return {"run_id": run_id, "watermark_before": wm, "years_checked": years,
            "actions": summary, "raw_rows_total": total}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--db", default="warehouse.duckdb")
    p.add_argument("--start-year", type=int, default=2000)
    p.add_argument("--end-year", type=int, default=2024,
                   help="last published year (2025 not yet released in FOSS as of Oct 2026)")
    p.add_argument("--lookback", type=int, default=2)
    p.add_argument("--landing-dir", default="data/raw")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    res = run(a.db, a.start_year, a.end_year, a.lookback, landing_dir=Path(a.landing_dir))
    print(json.dumps(res, indent=2))
    if "failed" in res["actions"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
