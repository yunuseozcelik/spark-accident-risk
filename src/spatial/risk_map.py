"""Faz 4: Folium ilçe risk haritaları.

county_risk.csv + ilçe poligonlarından iki interaktif harita üretir:
- outputs/maps/county_density_map.html — ilçe bazlı kaza sayısı (quantile binli)
- outputs/maps/county_risk_map.html — high-risk oranı; n<50 ilçeler
  "yetersiz veri" olarak gri (küçük örneklemde oran gürültülü olur)

Spark gerekmez; geopandas + folium yeterli. Poligonlar HTML boyutu için
sadeleştirilir (~0.01 derece tolerans).

Kullanım:
    .venv/bin/python src/spatial/risk_map.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import branca.colormap as cm
import folium
import geopandas as gpd
import pandas as pd
from shapely import wkt

RISK_CSV = "outputs/metrics/county_risk.csv"
COUNTY_WKT = "data/boundaries/counties_wkt.parquet"
OUT_DIR = Path("outputs/maps")

MIN_N_FOR_RATE = 50  # bu sayının altındaki ilçelerde oran gösterilmez
SIMPLIFY_TOL = 0.01  # derece; HTML boyutunu düşürür, ilçe ölçeğinde görünmez

# Doğrulanmış tek renkli (mavi) sequential ramp — açık = düşük, koyu = yüksek
BLUE_RAMP = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
NO_DATA = "#e1e0d9"

TOOLTIP_FIELDS = ["county_name", "poly_state", "n_accidents", "high_risk_rate", "avg_severity"]
TOOLTIP_ALIASES = ["İlçe", "Eyalet", "Kaza sayısı", "High-risk oranı", "Ort. şiddet"]


def load_counties() -> gpd.GeoDataFrame:
    df = pd.read_parquet(COUNTY_WKT)
    df["geoid"] = df["STATEFP"] + df["COUNTYFP"]
    gdf = gpd.GeoDataFrame(
        df[["geoid"]], geometry=df["wkt"].map(wkt.loads), crs="EPSG:4326"
    )
    gdf["geometry"] = gdf.geometry.simplify(SIMPLIFY_TOL)
    return gdf


def make_map(gdf: gpd.GeoDataFrame, value_col: str, caption: str, bins) -> folium.Map:
    colormap = cm.StepColormap(
        BLUE_RAMP, index=bins, vmin=bins[0], vmax=bins[-1], caption=caption
    )
    m = folium.Map(location=[39.5, -98.35], zoom_start=5, tiles="cartodbpositron")

    def style(feat):
        v = feat["properties"][value_col]
        return {
            "fillColor": NO_DATA if v is None else colormap(v),
            "fillOpacity": 0.85,
            "color": "#fcfcfb",  # ilçe sınırı: yüzey rengiyle 1px ayırıcı
            "weight": 0.4,
        }

    folium.GeoJson(
        gdf.to_json(),
        style_function=style,
        tooltip=folium.GeoJsonTooltip(fields=TOOLTIP_FIELDS, aliases=TOOLTIP_ALIASES),
        highlight_function=lambda _: {"weight": 1.5, "color": "#0b0b0b"},
    ).add_to(m)
    colormap.add_to(m)
    return m


def quantile_bins(series: pd.Series, k: int = 7) -> list:
    qs = series.quantile([i / k for i in range(k + 1)]).tolist()
    # quantile'lar çakışırsa (çok sayıda küçük ilçe) tekilleştir
    bins = sorted(set(qs))
    return bins if len(bins) >= 3 else [series.min(), series.median(), series.max()]


def main() -> None:
    risk = pd.read_csv(RISK_CSV, dtype={"geoid": str})
    gdf = load_counties().merge(risk, on="geoid", how="left")
    gdf["n_accidents"] = gdf["n_accidents"].fillna(0).astype(int)
    # Az kayıtlı ilçede oran gürültülü: haritada "yetersiz veri" olarak boş bırak
    gdf.loc[gdf["n_accidents"] < MIN_N_FOR_RATE, "high_risk_rate"] = None

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    dens = gdf[gdf["n_accidents"] > 0]
    m1 = make_map(
        gdf.assign(n_accidents=gdf["n_accidents"].where(gdf["n_accidents"] > 0)),
        "n_accidents",
        "İlçe bazlı kaza sayısı (2016–2023, quantile binler)",
        quantile_bins(dens["n_accidents"]),
    )
    m1.save(OUT_DIR / "county_density_map.html")
    print(f"Yazıldı: {OUT_DIR / 'county_density_map.html'}")

    rated = gdf[gdf["high_risk_rate"].notna()]
    m2 = make_map(
        gdf,
        "high_risk_rate",
        f"High-risk (Severity>=3) oranı — n>={MIN_N_FOR_RATE} olan ilçeler",
        quantile_bins(rated["high_risk_rate"]),
    )
    m2.save(OUT_DIR / "county_risk_map.html")
    print(f"Yazıldı: {OUT_DIR / 'county_risk_map.html'} "
          f"(oran gösterilen ilçe: {len(rated)}, yetersiz veri: {(gdf['high_risk_rate'].isna()).sum()})")


if __name__ == "__main__":
    main()
