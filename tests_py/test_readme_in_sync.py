"""Fails if README.md numbers drift from reports/kpis.json (playbook rule)."""
import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_readme_matches_kpis():
    spec = importlib.util.spec_from_file_location("rr", ROOT / "scripts" / "render_readme.py")
    rr = importlib.util.module_from_spec(spec); spec.loader.exec_module(rr)
    assert (ROOT / "README.md").read_text() == rr.render(), "README drifted: run scripts/render_readme.py"
