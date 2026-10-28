"""Render README.md from README.template.md + reports/kpis.json so narrative numbers never drift."""
import json
from pathlib import Path
from string import Template

ROOT = Path(__file__).resolve().parents[1]


def render() -> str:
    k = json.loads((ROOT / "reports" / "kpis.json").read_text())
    k = {key: (f"{v:,}" if isinstance(v, int) and not key.endswith("year") and key != "peak_year" else v) for key, v in k.items()}
    return Template((ROOT / "README.template.md").read_text()).substitute(k)


if __name__ == "__main__":
    (ROOT / "README.md").write_text(render())
    print("README.md rendered")
