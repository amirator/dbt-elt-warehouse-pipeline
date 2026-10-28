#!/usr/bin/env bash
# One command: EL -> dbt build (seed, run, snapshot, test) -> freshness -> docs -> report.
#   ./run.sh            full pipeline (creates .venv on first run)
#   ./run.sh --offline  skip ingestion, reuse warehouse.duckdb
set -euo pipefail
cd "$(dirname "$0")"
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt
fi
PY=.venv/bin/python; DBT=.venv/bin/dbt
export DBT_PROFILES_DIR="$PWD" DBT_DUCKDB_PATH="${DBT_DUCKDB_PATH:-$PWD/warehouse.duckdb}"

echo "== 1/6 pytest (ingestion unit tests)"; .venv/bin/pytest -q tests_py/test_extract_load.py
if [ "${1:-}" != "--offline" ]; then
  echo "== 2/6 extract + load NOAA FOSS -> raw schema"; $PY -m ingestion.extract_load --db "$DBT_DUCKDB_PATH"
fi
echo "== 3/6 dbt source freshness"; $DBT source freshness
echo "== 4/6 dbt build";            $DBT build
echo "== 5/6 dbt docs generate";    $DBT docs generate
echo "== 6/6 report + lineage";     $PY scripts/make_report.py "$DBT_DUCKDB_PATH" > /dev/null
$PY scripts/render_readme.py && .venv/bin/pytest -q tests_py/test_readme_in_sync.py
echo "Done. See reports/report.html and target/index.html"
