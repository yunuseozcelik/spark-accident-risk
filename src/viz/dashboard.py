"""Faz 6 demo: etkileşimli ilçe risk dashboard'u üretir.

`combined_risk_map.py`'ın genişletilmiş hali. Tek HTML sayfasında:
  - 3 metrik arasında geçişli choropleth (kaza sayısı / high-risk oranı /
    ort. şiddet) — Sedona join çıktısı `county_risk.csv` üzerinden
  - ilçeye tıklandığında o ilçenin saatlik deseni, hava durumu kırılımı ve
    yıllık eğilimi (`county_profiles.json`), ülke geneliyle karşılaştırmalı
  - eyalet filtresi ve seçili metriğe göre canlı güncellenen ilk 10 listesi
  - ülke geneli özet kartları

Girdi:
    outputs/metrics/county_risk.csv       (src/spatial/county_risk.py)
    outputs/metrics/county_profiles.json  (src/spatial/county_profiles.py)
    data/boundaries/counties_wkt.parquet  (scripts/convert_boundaries.py)

Kullanım:
    .venv/Scripts/python.exe src/viz/dashboard.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd

from src.spatial.risk_map import BLUE_RAMP, MIN_N_FOR_RATE, load_counties, quantile_bins

RISK_CSV = "outputs/metrics/county_risk.csv"
PROFILES_JSON = "outputs/metrics/county_profiles.json"
TEMPLATE = Path(__file__).with_name("dashboard_template.html")
VENDOR = Path(__file__).with_name("vendor")
OUT = Path("outputs/maps/dashboard.html")


def state_bounds(gdf) -> dict:
    """Eyalet kodu -> [[güney, batı], [kuzey, doğu]] (Leaflet fitBounds formatı)."""
    out = {}
    for state, grp in gdf[gdf["poly_state"].notna()].groupby("poly_state"):
        minx, miny, maxx, maxy = grp.total_bounds
        out[state] = [[float(miny), float(minx)], [float(maxy), float(maxx)]]
    return out


def main() -> None:
    risk = pd.read_csv(RISK_CSV, dtype={"geoid": str})
    gdf = load_counties().merge(risk, on="geoid", how="left")
    gdf["n_accidents"] = gdf["n_accidents"].fillna(0).astype(int)
    # Az kayıtlı ilçede oran/ortalama gürültülü olur: haritada "yetersiz veri"
    small = gdf["n_accidents"] < MIN_N_FOR_RATE
    gdf.loc[small, ["high_risk_rate", "avg_severity"]] = None

    bins = {
        "n_accidents": quantile_bins(gdf.loc[gdf["n_accidents"] > 0, "n_accidents"]),
        "high_risk_rate": quantile_bins(gdf.loc[gdf["high_risk_rate"].notna(), "high_risk_rate"]),
        "avg_severity": quantile_bins(gdf.loc[gdf["avg_severity"].notna(), "avg_severity"]),
    }

    props = gdf[["geoid", "county_name", "poly_state", "n_accidents",
                 "high_risk_rate", "avg_severity"]].copy()
    props["n_accidents"] = props["n_accidents"].where(props["n_accidents"] > 0)
    geo = gdf.set_geometry("geometry")[["geometry"]].join(props)
    geojson = json.loads(geo.to_json())

    profiles = json.loads(Path(PROFILES_JSON).read_text(encoding="utf-8"))

    total_n = int(gdf["n_accidents"].sum())
    nat_hi = sum(profiles["national"]["hour"]["hi"])
    nat_n = sum(profiles["national"]["hour"]["n"])
    summary = {
        "total_accidents": total_n,
        "n_counties": int((gdf["n_accidents"] > 0).sum()),
        "national_high_risk_rate": nat_hi / nat_n if nat_n else 0.0,
        "n_rated": int(gdf["high_risk_rate"].notna().sum()),
        "min_n": MIN_N_FOR_RATE,
    }

    states = sorted(gdf.loc[gdf["poly_state"].notna(), "poly_state"].unique().tolist())

    html = TEMPLATE.read_text(encoding="utf-8")
    for token, value in [
        # Leaflet CDN yerine gömülü: demo sırasında internet gerekmesin
        ("__LEAFLET_CSS__", VENDOR.joinpath("leaflet.css").read_text(encoding="utf-8")),
        ("__LEAFLET_JS__", VENDOR.joinpath("leaflet.js").read_text(encoding="utf-8")),
        ("__GEOJSON__", json.dumps(geojson, separators=(",", ":"))),
        ("__PROFILES__", json.dumps(profiles, separators=(",", ":"))),
        ("__BINS__", json.dumps(bins)),
        ("__COLORS__", json.dumps(BLUE_RAMP)),
        ("__SUMMARY__", json.dumps(summary)),
        ("__STATES__", json.dumps(states)),
        ("__STATE_BOUNDS__", json.dumps(state_bounds(gdf), separators=(",", ":"))),
    ]:
        html = html.replace(token, value)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html, encoding="utf-8")
    print(f"Yazıldı: {OUT} — {OUT.stat().st_size / 1e6:.1f} MB, "
          f"{len(geojson['features'])} ilçe, {len(profiles['counties'])} profil")


if __name__ == "__main__":
    main()
