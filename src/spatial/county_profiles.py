"""Faz 6 demo: ilçe bazlı zaman/hava/yıl profilleri (dashboard veri katmanı).

`county_risk.py` ilçe başına tek satır özet üretir (kaza sayısı, high-risk
oranı, ort. şiddet). Dashboard'da bir ilçeye tıklandığında o ilçenin *kendi*
saatlik deseni, hava durumu kırılımı ve yıllık eğilimi ülke geneliyle
karşılaştırmalı gösterilebilsin diye, aynı Sedona join üzerinden bu üç
kırılımı da toplulaştırıp tek JSON'a yazar.

Çıktı: outputs/metrics/county_profiles.json
    {
      "national": {"hour": {"n": [...], "hi": [...]}, "weather": {...}, "year": {...}},
      "counties": {"<geoid>": {"hour_n": [...], "hour_hi": [...], ...}}
    }
`hi` = high_risk (Severity>=3) kayıt sayısı; oran istemci tarafında n'e
bölünerek hesaplanır (JSON boyutunu küçük tutar).

Kullanım:
    .venv/Scripts/python.exe src/spatial/county_profiles.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

FEATURES = "data/processed/accidents_features.parquet"
COUNTY_WKT = "data/boundaries/counties_wkt.parquet"
OUT = Path("outputs/metrics/county_profiles.json")

HOURS = list(range(24))
YEARS = list(range(2016, 2024))
WEATHER = ["Clear", "Cloudy", "Rain", "Fog_Low_Vis", "Snow_Ice",
           "Thunderstorm", "Windy_Dust", "Unknown", "Other"]


def collect_profile(joined, dim_col, keys):
    """(geoid, dim) kırılımında toplam ve high-risk sayılarını toplar.

    Dönen sözlük: {geoid: {"n": [...], "hi": [...]}} — diziler `keys` sırasında.
    """
    rows = (
        joined.groupBy("geoid", dim_col)
        .agg(F.count("*").alias("n"), F.sum("high_risk").alias("hi"))
        .collect()
    )
    idx = {k: i for i, k in enumerate(keys)}
    out = {}
    for r in rows:
        key = r[dim_col]
        if key not in idx:
            continue
        d = out.setdefault(r["geoid"], {"n": [0] * len(keys), "hi": [0] * len(keys)})
        d["n"][idx[key]] = r["n"]
        d["hi"][idx[key]] = int(r["hi"] or 0)
    return out


def national_from(counties: dict, size: int) -> dict:
    n = [0] * size
    hi = [0] * size
    for d in counties.values():
        for i in range(size):
            n[i] += d["n"][i]
            hi[i] += d["hi"][i]
    return {"n": n, "hi": hi}


def main() -> None:
    spark = get_spark("county-profiles", with_sedona=True)

    counties = spark.read.parquet(COUNTY_WKT).select(
        F.expr("ST_GeomFromWKT(wkt)").alias("geometry"),
        F.concat("STATEFP", "COUNTYFP").alias("geoid"),
    )
    acc = (
        spark.read.parquet(FEATURES)
        .select("high_risk", "hour", "year", "weather_group", "Start_Lat", "Start_Lng")
        .withColumn("point", F.expr("ST_Point(Start_Lng, Start_Lat)"))
    )

    acc.createOrReplaceTempView("acc")
    counties.createOrReplaceTempView("counties")
    joined = spark.sql(
        """
        SELECT a.high_risk, a.hour, a.year, a.weather_group, c.geoid
        FROM acc a JOIN counties c ON ST_Contains(c.geometry, a.point)
        """
    ).cache()
    print(f"Eşleşen kayıt: {joined.count():,}")

    hour = collect_profile(joined, "hour", HOURS)
    print(f"  saatlik profil: {len(hour)} ilçe")
    weather = collect_profile(joined, "weather_group", WEATHER)
    print(f"  hava profili:   {len(weather)} ilçe")
    year = collect_profile(joined, "year", YEARS)
    print(f"  yıllık profil:  {len(year)} ilçe")

    geoids = sorted(set(hour) | set(weather) | set(year))
    counties_out = {}
    for g in geoids:
        h = hour.get(g, {"n": [0] * 24, "hi": [0] * 24})
        w = weather.get(g, {"n": [0] * len(WEATHER), "hi": [0] * len(WEATHER)})
        y = year.get(g, {"n": [0] * len(YEARS), "hi": [0] * len(YEARS)})
        counties_out[g] = {
            "hour_n": h["n"], "hour_hi": h["hi"],
            "weather_n": w["n"], "weather_hi": w["hi"],
            "year_n": y["n"], "year_hi": y["hi"],
        }

    payload = {
        "keys": {"hour": HOURS, "weather": WEATHER, "year": YEARS},
        "national": {
            "hour": national_from(hour, 24),
            "weather": national_from(weather, len(WEATHER)),
            "year": national_from(year, len(YEARS)),
        },
        "counties": counties_out,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    size_mb = OUT.stat().st_size / 1e6
    print(f"\nYazıldı: {OUT} — {len(counties_out)} ilçe, {size_mb:.1f} MB")
    spark.stop()


if __name__ == "__main__":
    main()
