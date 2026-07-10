"""TIGER/Line shapefile'larını WKT sütunlu Parquet'e dönüştürür.

Sedona'nın shapefile okuyucusu geotools sürüm uyumsuzluğuna hassas olduğundan
(bkz. status report, know-how), sınırlar geopandas ile okunup WKT olarak
Parquet'e yazılır; Spark tarafında ST_GeomFromWKT ile geometriye çevrilir.

Kullanım:
    .venv/bin/python scripts/convert_boundaries.py
"""
import geopandas as gpd

JOBS = [
    ("data/boundaries/state", "data/boundaries/states_wkt.parquet",
     ["STATEFP", "STUSPS", "NAME"]),
    ("data/boundaries/county", "data/boundaries/counties_wkt.parquet",
     ["STATEFP", "COUNTYFP", "NAME"]),
]

for src, dst, cols in JOBS:
    gdf = gpd.read_file(src)
    # TIGER NAD83 (EPSG:4269) -> WGS84; kaza koordinatlarıyla aynı referans
    gdf = gdf.to_crs(epsg=4326)
    out = gdf[cols].copy()
    out["wkt"] = gdf.geometry.to_wkt()
    out.to_parquet(dst, index=False)
    print(f"{src} -> {dst}: {len(out)} poligon")
