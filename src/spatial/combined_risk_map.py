"""Faz 6 demo: iki katmanı (yoğunluk / high-risk oranı) tek sayfada,
buton ile geçişli interaktif harita olarak üretir.

`risk_map.py`'daki iki ayrı statik Folium haritasının aksine, burada tek bir
Leaflet haritasında iki buton ile katman değiştirilebilir (aynı GeoJSON
katmanı, stil ve lejant o an seçili metriğe göre yeniden çizilir). Veri
kaynağı `risk_map.py` ile birebir aynıdır (county_risk.csv + counties_wkt).

Kullanım:
    .venv/Scripts/python.exe src/spatial/combined_risk_map.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from src.spatial.risk_map import BLUE_RAMP, MIN_N_FOR_RATE, load_counties, quantile_bins

RISK_CSV = "outputs/metrics/county_risk.csv"
OUT = Path("outputs/maps/combined_risk_map.html")


def main() -> None:
    risk = pd.read_csv(RISK_CSV, dtype={"geoid": str})
    gdf = load_counties().merge(risk, on="geoid", how="left")
    gdf["n_accidents"] = gdf["n_accidents"].fillna(0).astype(int)
    gdf.loc[gdf["n_accidents"] < MIN_N_FOR_RATE, "high_risk_rate"] = None

    dens_vals = gdf.loc[gdf["n_accidents"] > 0, "n_accidents"]
    rate_vals = gdf.loc[gdf["high_risk_rate"].notna(), "high_risk_rate"]
    bins_density = quantile_bins(dens_vals)
    bins_risk = quantile_bins(rate_vals)

    props = gdf[[
        "geoid", "county_name", "poly_state", "n_accidents",
        "high_risk_rate", "avg_severity",
    ]].copy()
    props["n_accidents"] = props["n_accidents"].where(props["n_accidents"] > 0)
    geo = gdf.set_geometry("geometry")[["geometry"]].join(props)
    geojson = json.loads(geo.to_json())

    n_counties = len(gdf)
    n_rated = int(gdf["high_risk_rate"].notna().sum())

    html = TEMPLATE.format(
        geojson=json.dumps(geojson, separators=(",", ":")),
        bins_density=json.dumps(bins_density),
        bins_risk=json.dumps(bins_risk),
        colors=json.dumps(BLUE_RAMP),
        n_counties=n_counties,
        n_rated=n_rated,
        min_n=MIN_N_FOR_RATE,
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"Yazıldı: {OUT} ({n_counties} ilçe, {n_rated} ilçede oran gösteriliyor)")


TEMPLATE = """<!DOCTYPE html>
<html lang="tr">
<head>
<meta charset="utf-8">
<title>İlçe Bazlı Kaza Riski — Yoğunluk / High-Risk Oranı</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<style>
  :root {{
    --ink: #0b0b0b; --ink-2: #52514e; --muted: #898781;
    --surface: #fcfcfb; --grid: #e1e0d9; --blue: #2a78d6;
  }}
  html, body {{ margin: 0; height: 100%; font-family: system-ui, -apple-system, "Segoe UI", sans-serif; color: var(--ink); }}
  #map {{ position: absolute; inset: 0; }}
  .panel {{
    position: absolute; z-index: 1000; background: var(--surface);
    border-radius: 10px; box-shadow: 0 2px 10px rgba(11,11,11,0.18);
    border: 1px solid var(--grid);
  }}
  #title-panel {{ top: 12px; left: 12px; padding: 10px 14px; max-width: 320px; }}
  #title-panel h1 {{ font-size: 14px; margin: 0 0 2px; }}
  #title-panel p {{ font-size: 11.5px; color: var(--ink-2); margin: 0; }}
  #toggle-panel {{ top: 88px; left: 12px; padding: 4px; display: flex; gap: 4px; }}
  .toggle-btn {{
    border: none; background: transparent; color: var(--ink-2);
    font-size: 12.5px; font-weight: 600; padding: 7px 12px; border-radius: 7px;
    cursor: pointer; transition: background .12s, color .12s;
  }}
  .toggle-btn:hover {{ background: #eef2f7; }}
  .toggle-btn.active {{ background: var(--blue); color: #fff; }}
  #legend-panel {{ bottom: 20px; left: 12px; padding: 10px 14px; font-size: 11.5px; min-width: 220px; }}
  #legend-title {{ font-weight: 600; margin-bottom: 6px; color: var(--ink); }}
  #legend-bar {{ display: flex; height: 10px; border-radius: 3px; overflow: hidden; margin-bottom: 4px; }}
  #legend-bar div {{ flex: 1; }}
  #legend-labels {{ display: flex; justify-content: space-between; color: var(--muted); font-variant-numeric: tabular-nums; }}
  #legend-note {{ color: var(--muted); margin-top: 6px; }}
  .leaflet-popup-content {{ font-size: 12.5px; }}
  .leaflet-popup-content b {{ color: var(--ink); }}
  .pop-row {{ display: flex; justify-content: space-between; gap: 14px; padding: 1px 0; }}
  .pop-row span:first-child {{ color: var(--ink-2); }}
</style>
</head>
<body>
<div id="map"></div>

<div class="panel" id="title-panel">
  <h1>İlçe bazlı kaza riski — ABD (2016–2023)</h1>
  <p>{n_counties} ilçe · Sedona nokta-poligon join, 7,7M kaza noktası</p>
</div>

<div class="panel" id="toggle-panel">
  <button class="toggle-btn active" data-mode="density">Kaza Yoğunluğu</button>
  <button class="toggle-btn" data-mode="risk">Yüksek-Risk Oranı</button>
</div>

<div class="panel" id="legend-panel">
  <div id="legend-title"></div>
  <div id="legend-bar"></div>
  <div id="legend-labels"></div>
  <div id="legend-note"></div>
</div>

<script>
const GEOJSON = {geojson};
const BINS = {{ density: {bins_density}, risk: {bins_risk} }};
const COLORS = {colors};
const NO_DATA = "#e1e0d9";
const MIN_N = {min_n};
const N_RATED = {n_rated};

const FIELD = {{ density: "n_accidents", risk: "high_risk_rate" }};
const TITLE = {{
  density: "İlçe bazlı kaza sayısı (quantile binler)",
  risk: `High-risk (Severity≥3) oranı — n≥${{MIN_N}} olan ilçeler`
}};
const FMT = {{
  density: v => v.toLocaleString("en-US"),
  risk: v => (v * 100).toFixed(1) + "%"
}};

let mode = "density";

function colorFor(value, bins) {{
  if (value === null || value === undefined) return NO_DATA;
  for (let i = 1; i < bins.length; i++) {{
    if (value <= bins[i] || i === bins.length - 1) return COLORS[Math.min(i - 1, COLORS.length - 1)];
  }}
  return COLORS[COLORS.length - 1];
}}

function styleFor(feature) {{
  const v = feature.properties[FIELD[mode]];
  return {{
    fillColor: colorFor(v, BINS[mode]),
    fillOpacity: 0.85,
    color: "#fcfcfb",
    weight: 0.4,
  }};
}}

const map = L.map("map", {{ zoomControl: true }}).setView([39.5, -98.35], 5);
L.tileLayer("https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png", {{
  attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
  subdomains: "abcd", maxZoom: 19,
}}).addTo(map);
map.zoomControl.setPosition("topright");

function popupHtml(p) {{
  const rate = p.high_risk_rate === null || p.high_risk_rate === undefined
    ? "yetersiz veri" : (p.high_risk_rate * 100).toFixed(1) + "%";
  const n = p.n_accidents === null || p.n_accidents === undefined ? 0 : p.n_accidents;
  const sev = p.avg_severity === null || p.avg_severity === undefined ? "—" : p.avg_severity.toFixed(2);
  return `
    <div class="pop-row"><span>İlçe</span><b>${{p.county_name}}</b></div>
    <div class="pop-row"><span>Eyalet</span><b>${{p.poly_state ?? "—"}}</b></div>
    <div class="pop-row"><span>Kaza sayısı</span><b>${{n.toLocaleString("en-US")}}</b></div>
    <div class="pop-row"><span>High-risk oranı</span><b>${{rate}}</b></div>
    <div class="pop-row"><span>Ort. şiddet</span><b>${{sev}}</b></div>
  `;
}}

const layer = L.geoJSON(GEOJSON, {{
  style: styleFor,
  onEachFeature: (feature, lyr) => {{
    lyr.bindPopup(popupHtml(feature.properties));
    lyr.on("mouseover", () => lyr.setStyle({{ weight: 1.6, color: "#0b0b0b" }}));
    lyr.on("mouseout", () => lyr.setStyle({{ weight: 0.4, color: "#fcfcfb" }}));
  }},
}}).addTo(map);

function renderLegend() {{
  const bins = BINS[mode];
  document.getElementById("legend-title").textContent = TITLE[mode];
  const bar = document.getElementById("legend-bar");
  bar.innerHTML = "";
  const nSeg = Math.max(bins.length - 1, 1);
  for (let i = 0; i < nSeg; i++) {{
    const seg = document.createElement("div");
    seg.style.background = COLORS[Math.min(i, COLORS.length - 1)];
    bar.appendChild(seg);
  }}
  const labels = document.getElementById("legend-labels");
  labels.innerHTML = `<span>${{FMT[mode](bins[0])}}</span><span>${{FMT[mode](bins[bins.length - 1])}}</span>`;
  document.getElementById("legend-note").textContent = mode === "risk"
    ? `Gri = veri yetersiz (n<${{MIN_N}}); oran gösterilen ilçe: ${{N_RATED}}`
    : "Gri = kayıt yok";
}}

function setMode(newMode) {{
  mode = newMode;
  document.querySelectorAll(".toggle-btn").forEach(b =>
    b.classList.toggle("active", b.dataset.mode === mode));
  layer.setStyle(styleFor);
  renderLegend();
}}

document.querySelectorAll(".toggle-btn").forEach(b =>
  b.addEventListener("click", () => setMode(b.dataset.mode)));

renderLegend();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
