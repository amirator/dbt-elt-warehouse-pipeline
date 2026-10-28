# PROJECT_BLURB: dbt-elt-warehouse-pipeline

## (a) CV Projects entry
**US Seafood Landings ELT Pipeline: Python + dbt + DuckDB** (personal project, github.com/amirator/dbt-elt-warehouse-pipeline)
*Tools:* Python, REST API ingestion, DuckDB, dbt-core/dbt-duckdb (incremental models, snapshots/SCD2, macros, sources/freshness, exposures, docs), SQL (Snowflake-portable), pytest, GitHub Actions
- Built an incremental, idempotent Python loader for the NOAA Fisheries API. It uses a year watermark with a 2-year look-back and a SHA-256 hash per partition, logs every load, and lands 79,307 records (2000-2024) into a raw schema. A re-run loads 0 duplicate rows, and a clean rebuild runs end to end in about 35 seconds with one command.
- Modelled the data in dbt through staging, intermediate and marts layers (70,415-row incremental fact, SCD2 snapshot, 11 models). 46 data tests pass with zero failures, including raw-to-mart reconciliation, and 10 pytest tests also pass. The tests surfaced real source issues (3,420 duplicate sub-record keys, 15 negative adjustments), and the marts report $4.91bn of US landings value in 2024.

## (b) One-line pitch
An honest, end-to-end ELT pipeline: an incremental Python connector into a raw schema, then dbt staging/marts with 46 passing tests, a snapshot and docs on NOAA seafood landings data. Built on DuckDB and written to port to Snowflake.

## (c) Playbook learnings (dbt / ELT)
- NOAA FOSS landings API (`apps-st.fisheries.noaa.gov/ods/foss/landings/?q={"year":Y,"collection":"Commercial"}&limit=10000`) is public ORDS JSON. It returns about 3k commercial rows per year, the latest year is 2024 as of Oct 2026, and you paginate with `hasMore`/`offset`. The ROWID in `links[rel=self]` is a unique row id.
- FOSS quirks: duplicate sub-records per year/state/species/source (aggregate them and prove the totals with a reconciliation test), 15 small negative ACCSP adjustments (pin them, don't drop them), `**` suffix = grouped record, names with trailing spaces, 15 rows with no region, and Alaska published at a coarse grain. It is wild capture only, with no aquaculture.
- Incremental EL pattern: a per-partition SHA-256 of sorted, canonical JSON, plus a watermark and look-back. Skip when the hash is unchanged; otherwise DELETE + INSERT in one transaction. Log every attempt, including failures, then continue and exit non-zero.
- dbt 1.12: generic test arguments go under `arguments:`, and `accepted_values` needs `arguments: {values: [...]}`. YAML snapshots use `relation:` plus `config:` (`strategy: check`, `hard_deletes`). Source freshness needs `loaded_at_field` per table when the timestamp column names differ.
- Avoid package installs in CI by re-implementing `generate_surrogate_key` with `dbt.hash(dbt.concat(...))`, and unique-combination as a 5-line generic test. Both are portable to Snowflake.
- Incremental fact on revisable data: `delete+insert` on a surrogate key with `where year >= max(year) - lookback + 1` (works on DuckDB and Snowflake).
- Lineage PNG without graphviz: load target/manifest.json, build an networkx DiGraph, then use `topological_generations` and `multipartite_layout`.
- run.sh: run the loader unit tests first, render the README from kpis.json last, then run the README drift test. Keep KPIs deterministic (drop run counters) so a clean rebuild gives byte-identical kpis.json.
