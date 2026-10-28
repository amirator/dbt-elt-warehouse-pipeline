"""Build kpis.json, PNG charts, the lineage graph and a small HTML report from the dbt marts."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import networkx as nx

ROOT = Path(__file__).resolve().parents[1]
FIG = ROOT / "reports" / "figures"
SRC = "Source: NOAA Fisheries, FOSS commercial landings (apps-st.fisheries.noaa.gov/foss). Nominal USD, live-weight lb."


def q(con, sql):
    return con.execute(sql).fetchall()


def kpis(con) -> dict:
    yr = q(con, "select max(landing_year), min(landing_year) from marts.fct_landings")[0]
    last, first = yr
    tot = dict((y, (u, l)) for y, u, l in q(con, "select landing_year, sum(exvessel_usd), sum(landed_lbs) from marts.mart_landings_by_region_year group by 1"))
    peak_y = max(tot, key=lambda y: tot[y][0])
    grp = q(con, f"select species_group, exvessel_usd from marts.mart_species_group_trends where landing_year={last} and species_group not in ('Other','Confidential') order by exvessel_usd desc")
    lob = q(con, f"""select sum(f.exvessel_usd) from marts.fct_landings f join marts.dim_species d using (species_key)
                     where d.species_name='LOBSTER, AMERICAN' and f.landing_year={last}""")[0][0]
    me_lob = q(con, f"""select sum(f.exvessel_usd), sum(f.landed_lbs) from marts.fct_landings f join marts.dim_species d using (species_key)
                       where d.species_name='LOBSTER, AMERICAN' and f.state_name='MAINE' and f.landing_year={last}""")[0]
    ne = q(con, f"select sum(exvessel_usd) from marts.mart_landings_by_region_year where region_name='New England' and landing_year={last}")[0][0]
    scal = q(con, f"""select sum(f.exvessel_usd) from marts.fct_landings f join marts.dim_species d using (species_key)
                      where d.species_name='SCALLOP, SEA' and f.landing_year={last}""")[0][0]
    lob_px = q(con, f"""select f.landing_year, sum(f.exvessel_usd)/sum(f.landed_lbs) from marts.fct_landings f join marts.dim_species d using (species_key)
                       where d.species_name='LOBSTER, AMERICAN' and f.landing_year in ({first},{last}) group by 1 order by 1""")
    k = {
        "first_year": first, "last_year": last, "years": last - first + 1,
        "raw_rows": q(con, "select count(*) from raw.foss_landings")[0][0],
        "fact_rows": q(con, "select count(*) from marts.fct_landings")[0][0],
        "species": q(con, "select count(*) from marts.dim_species")[0][0],
        "states": q(con, "select count(*) from marts.dim_state")[0][0],
        "multi_record_keys": q(con, "select count(*) from marts.fct_landings where source_record_count > 1")[0][0],
        "negative_rows": q(con, "select count(*) from staging.stg_noaa__landings where landed_lbs < 0 or exvessel_usd < 0")[0][0],
        "missing_region_rows": q(con, "select count(*) from raw.foss_landings where region_name is null")[0][0],
        "null_dollar_rows": q(con, "select count(*) from raw.foss_landings where dollars is null")[0][0],
        "confidential_max_share_pct": round(100 * q(con, "select max(share_of_year_usd) from marts.mart_species_group_trends where species_group='Confidential'")[0][0], 1),
        "last_value_busd": round(tot[last][0] / 1e9, 2),
        "last_volume_blbs": round(tot[last][1] / 1e9, 2),
        "first_value_busd": round(tot[first][0] / 1e9, 2),
        "peak_year": peak_y, "peak_value_busd": round(tot[peak_y][0] / 1e9, 2),
        "change_peak_to_last_pct": round(100 * (tot[last][0] / tot[peak_y][0] - 1), 1),
        "top_group_last": grp[0][0], "top_group_last_musd": round(grp[0][1] / 1e6),
        "lobster_last_musd": round(lob / 1e6), "lobster_share_pct": round(100 * lob / tot[last][0], 1),
        "maine_lobster_last_musd": round(me_lob[0] / 1e6), "maine_lobster_last_mlbs": round(me_lob[1] / 1e6, 1),
        "sea_scallop_last_musd": round(scal / 1e6),
        "new_england_last_busd": round(ne / 1e9, 2), "new_england_share_pct": round(100 * ne / tot[last][0], 1),
        "lobster_usd_per_lb_first": round(lob_px[0][1], 2), "lobster_usd_per_lb_last": round(lob_px[1][1], 2),
    }
    return k


def chart_national(con, k):
    rows = q(con, "select landing_year, sum(exvessel_usd)/1e9, sum(landed_lbs)/1e9 from marts.mart_landings_by_region_year group by 1 order by 1")
    y, v, l = zip(*rows)
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(y, v, marker="o", color="#0b5394", label="Ex-vessel value (US$ bn)")
    ax.set_ylabel("US$ billion (nominal)"); ax.set_ylim(0, max(v) * 1.2)
    ax2 = ax.twinx(); ax2.bar(y, l, alpha=0.25, color="#6aa84f", label="Landed volume (bn lb)")
    ax2.set_ylabel("billion lb (live weight)"); ax2.set_ylim(0, max(l) * 1.6)
    ax.set_title(f"US commercial landings {k['first_year']}-{k['last_year']}: value peaked at ${k['peak_value_busd']}bn in {k['peak_year']}", loc="left")
    h1, l1 = ax.get_legend_handles_labels(); h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")
    ax.set_zorder(ax2.get_zorder() + 1); ax.patch.set_visible(False)
    fig.text(0.01, 0.01, SRC, fontsize=7, color="grey"); fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(FIG / "01_national_value_volume.png", dpi=130); plt.close(fig)


def chart_groups(con, k):
    rows = q(con, "select landing_year, species_group, exvessel_usd/1e6 from marts.mart_species_group_trends where is_watch_group order by 1")
    fig, ax = plt.subplots(figsize=(10, 5.5))
    for g in sorted({r[1] for r in rows}):
        pts = [(r[0], r[2]) for r in rows if r[1] == g]
        ax.plot(*zip(*pts), marker=".", label=g)
    ax.set_ylabel("US$ million (nominal)")
    ax.set_title("Ex-vessel value of watch species groups, all US states (salmon is mostly Alaska wild catch)", loc="left")
    ax.legend(ncol=4, fontsize=8, loc="upper left"); ax.grid(alpha=0.3)
    fig.text(0.01, 0.01, SRC, fontsize=7, color="grey"); fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(FIG / "02_species_group_value.png", dpi=130); plt.close(fig)


def chart_new_england(con, k):
    rows = q(con, f"""select species_group, state_name, sum(exvessel_usd)/1e6 from marts.mart_new_england_watch
                      where landing_year={k['last_year']} group by 1,2""")
    groups = [g for g, _ in sorted({(r[0], 0) for r in rows})]
    groups.sort(key=lambda g: -sum(r[2] or 0 for r in rows if r[0] == g))
    states = sorted({r[1] for r in rows}, key=lambda s: -sum(r[2] or 0 for r in rows if r[1] == s))
    fig, ax = plt.subplots(figsize=(10, 5))
    left = [0.0] * len(groups)
    for s in states:
        vals = [sum(r[2] or 0 for r in rows if r[0] == g and r[1] == s) for g in groups]
        ax.barh(groups, vals, left=left, label=s.title())
        left = [a + b for a, b in zip(left, vals)]
    ax.invert_yaxis(); ax.set_xlabel("US$ million (nominal)")
    for i, t in enumerate(left):
        ax.text(t, i, f" ${t:,.0f}m", va="center", fontsize=8)
    ax.set_xlim(0, max(left) * 1.15)
    ax.set_title(f"New England {k['last_year']}: watch-group value by state (lobster ${k['maine_lobster_last_musd']}m in Maine alone)", loc="left")
    ax.legend(fontsize=8, loc="lower right")
    fig.text(0.01, 0.01, SRC, fontsize=7, color="grey"); fig.tight_layout(rect=(0, 0.03, 1, 1))
    fig.savefig(FIG / "03_new_england_watch.png", dpi=130); plt.close(fig)


def chart_lineage():
    m = json.loads((ROOT / "target" / "manifest.json").read_text())
    g = nx.DiGraph()
    nodes = {**m["nodes"], **m["sources"], **m.get("exposures", {})}
    keep = {"model", "seed", "snapshot", "source", "exposure"}
    for uid, n in nodes.items():
        if n["resource_type"] in keep and n.get("package_name") == "seafood_elt":
            label = (f"{n['source_name']}.{n['name']}" if n["resource_type"] == "source" else n["name"])
            g.add_node(uid, label=label, kind=n["resource_type"])
    for uid, n in nodes.items():
        if uid in g:
            for p in n.get("depends_on", {}).get("nodes", []):
                if p in g:
                    g.add_edge(p, uid)
    for layer, nodes_ in enumerate(nx.topological_generations(g)):
        for u in nodes_:
            g.nodes[u]["layer"] = layer
    pos = nx.multipartite_layout(g, subset_key="layer", align="vertical")
    colors = {"source": "#93c47d", "seed": "#ffd966", "model": "#6fa8dc", "snapshot": "#c27ba0", "exposure": "#f6b26b"}
    fig, ax = plt.subplots(figsize=(15, 8))
    nx.draw_networkx_edges(g, pos, ax=ax, arrows=True, arrowsize=12, edge_color="#999", node_size=2500)
    nx.draw_networkx_nodes(g, pos, ax=ax, node_color=[colors[g.nodes[u]["kind"]] for u in g], node_size=900)
    nx.draw_networkx_labels(g, {u: (x, y - 0.045) for u, (x, y) in pos.items()},
                            labels={u: g.nodes[u]["label"] for u in g}, font_size=8, ax=ax)
    for kind, c in colors.items():
        ax.scatter([], [], c=c, s=120, label=kind)
    ax.legend(loc="upper left", fontsize=9); ax.set_title("dbt lineage (rendered from target/manifest.json)", loc="left")
    ax.axis("off"); fig.tight_layout()
    fig.savefig(FIG / "00_dbt_lineage.png", dpi=130); plt.close(fig)


def html(k):
    rows = "".join(f"<tr><td>{a}</td><td>{b}</td></tr>" for a, b in [
        (f"US commercial landings value, {k['last_year']}", f"${k['last_value_busd']}bn"),
        (f"Landed volume, {k['last_year']}", f"{k['last_volume_blbs']}bn lb"),
        ("Peak value year", f"{k['peak_year']} (${k['peak_value_busd']}bn)"),
        (f"Change {k['peak_year']} to {k['last_year']}", f"{k['change_peak_to_last_pct']}%"),
        (f"American lobster value, {k['last_year']}", f"${k['lobster_last_musd']}m ({k['lobster_share_pct']}% of US)"),
        (f"New England share of US value, {k['last_year']}", f"{k['new_england_share_pct']}%"),
    ])
    imgs = "".join(f'<img src="figures/{p}" style="max-width:100%;margin:12px 0">' for p in
                   ["01_national_value_volume.png", "02_species_group_value.png", "03_new_england_watch.png", "00_dbt_lineage.png"])
    (ROOT / "reports" / "report.html").write_text(f"""<!doctype html><html><head><meta charset="utf-8">
<title>US seafood landings - dbt ELT pipeline report</title>
<style>body{{font-family:sans-serif;max-width:1000px;margin:auto;padding:20px}}td{{padding:4px 12px;border-bottom:1px solid #ddd}}</style>
</head><body><h1>US commercial seafood landings, {k['first_year']}-{k['last_year']}</h1>
<p>Built from the dbt marts of this project. {SRC}</p><table>{rows}</table>{imgs}
<p style="color:grey;font-size:12px">Author: Amir Khan. Personal portfolio project.</p></body></html>""")


def main():
    FIG.mkdir(parents=True, exist_ok=True)
    db = sys.argv[1] if len(sys.argv) > 1 else str(ROOT / "warehouse.duckdb")
    con = duckdb.connect(db, read_only=True)
    k = kpis(con)
    (ROOT / "reports" / "kpis.json").write_text(json.dumps(k, indent=2) + "\n")
    chart_national(con, k); chart_groups(con, k); chart_new_england(con, k)
    con.close()
    chart_lineage(); html(k)
    print(json.dumps(k, indent=2))


if __name__ == "__main__":
    main()
