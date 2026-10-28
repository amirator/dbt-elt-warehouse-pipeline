"""Unit tests for the EL step. The NOAA API is replaced by an in-memory fake fetcher,
built from the real record shape returned by the FOSS endpoint."""
import duckdb
import pytest

from ingestion import extract_load as el


def rec(year, state="MAINE", name="LOBSTER, AMERICAN", lbs=100.0, usd=500.0, rowid="AAA"):
    return {"tsn": "97314", "ts_afs_name": name, "ts_scientific_name": "Homarus americanus",
            "region_name": "New England", "state_name": state, "year": year,
            "pounds": lbs, "dollars": usd, "tot_count": None, "source": "ACCSP",
            "collection": "Commercial",
            "links": [{"rel": "self", "href": f"https://apps-st.fisheries.noaa.gov/ods/foss/landings/{rowid}{year}"}]}


class FakeAPI:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def __call__(self, year):
        self.calls.append(year)
        return list(self.data.get(year, []))


@pytest.fixture
def db(tmp_path):
    return tmp_path / "t.duckdb"


def q(db, sql):
    con = duckdb.connect(str(db))
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_initial_load_inserts_all_years(db):
    api = FakeAPI({2022: [rec(2022)], 2023: [rec(2023), rec(2023, state="MASSACHUSETTS", rowid="BBB")]})
    res = el.run(db, 2022, 2023, fetcher=api)
    assert res["actions"] == {"inserted": [2022, 2023]}
    assert q(db, "select count(*) from raw.foss_landings") == [(3,)]
    assert q(db, "select count(*) from raw._load_log") == [(2,)]


def test_rerun_is_idempotent_and_skips_unchanged(db):
    api = FakeAPI({2022: [rec(2022)], 2023: [rec(2023)]})
    el.run(db, 2022, 2023, fetcher=api)
    res = el.run(db, 2022, 2023, fetcher=api)
    assert res["actions"] == {"unchanged": [2022, 2023]}
    assert q(db, "select count(*) from raw.foss_landings") == [(2,)]


def test_incremental_watermark_with_lookback(db):
    api = FakeAPI({y: [rec(y)] for y in range(2015, 2025)})
    el.run(db, 2015, 2022, fetcher=api)
    api.calls.clear()
    el.run(db, 2015, 2024, lookback=2, fetcher=api)
    # watermark 2022, lookback 2 -> re-check 2021-2022, fetch new 2023-2024 only
    assert api.calls == [2021, 2022, 2023, 2024]


def test_revised_partition_is_replaced_not_duplicated(db):
    data = {2023: [rec(2023, lbs=100.0)]}
    api = FakeAPI(data)
    el.run(db, 2023, 2023, fetcher=api)
    data[2023] = [rec(2023, lbs=120.0)]
    res = el.run(db, 2023, 2023, fetcher=api)
    assert res["actions"] == {"replaced": [2023]}
    assert q(db, "select count(*), sum(pounds) from raw.foss_landings") == [(1, 120.0)]


def test_hash_is_order_independent():
    a = [rec(2023, rowid="A"), rec(2023, rowid="B")]
    assert el.fingerprint(el.normalise(a)) == el.fingerprint(el.normalise(list(reversed(a))))


def test_failed_fetch_is_logged_and_other_years_continue(db):
    def flaky(year):
        if year == 2023:
            raise ConnectionError("boom")
        return [rec(year)]
    res = el.run(db, 2022, 2024, fetcher=flaky)
    assert res["actions"]["failed"] == [2023]
    assert res["actions"]["inserted"] == [2022, 2024]
    assert q(db, "select action, message from raw._load_log where year = 2023") == [("failed", "boom")]
    # a failed year is retried on the next run (watermark ignores failures for that year's hash)
    res2 = el.run(db, 2022, 2024, lookback=3, fetcher=FakeAPI({y: [rec(y)] for y in (2022, 2023, 2024)}))
    assert res2["actions"]["inserted"] == [2023]


def test_source_row_id_from_self_link():
    assert el.source_row_id(rec(2024, rowid="AAFsl")) == "AAFsl2024"


def test_year_url_filters_commercial():
    url = el.year_url(2024)
    assert "collection" in url and "Commercial" in url and "limit=10000" in url


def test_years_to_sync():
    assert el.years_to_sync(None, 2000, 2003, 2) == [2000, 2001, 2002, 2003]
    assert el.years_to_sync(2024, 2000, 2024, 2) == [2023, 2024]
