"""Temizleme: ham Parquet katmanından analize hazır temiz katman üretir.

Adımlar (öneri raporu Faz 3):
- Zorunlu alanlar (Severity, zaman, koordinat) null ise satırı at
- ABD sınır kutusu dışındaki koordinatları at (AK/HI dahil)
- Kaza süresini hesapla; negatif veya 24 saati aşan aykırı süreleri at
- Weather_Condition'ı ~127 ham değerden 8 gruba indir (weather_group)
- Precipitation null (%28,5) -> 0 + precip_missing bayrağı

Çıktı: eyalet bazlı partition'lanmış temiz Parquet + outputs/metrics/cleaning_summary.json

Kullanım:
    .venv/bin/python src/preprocessing/clean.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET_IN = "data/processed/accidents.parquet"
PARQUET_OUT = "data/processed/accidents_clean.parquet"
SUMMARY_OUT = Path("outputs/metrics/cleaning_summary.json")

REQUIRED = ["Severity", "Start_Time", "End_Time", "Start_Lat", "Start_Lng"]

# ABD sınır kutusu (Alaska ve Hawaii dahil)
LAT_MIN, LAT_MAX = 17.5, 72.0
LNG_MIN, LNG_MAX = -180.0, -65.0

MAX_DURATION_MIN = 24 * 60

# Sıra önemli: önce daha spesifik olaylar (kar "Light Snow Shower" gibi
# rain kelimesi içerebilir), en sona genel açık/bulutlu
WEATHER_GROUPS = [
    ("Thunderstorm", r"thunder|t-storm"),
    ("Snow_Ice", r"snow|sleet|wintry|ice|hail|freezing"),
    ("Rain", r"rain|drizzle|shower|precip"),
    ("Fog_Low_Vis", r"fog|mist|haze|smoke"),
    ("Windy_Dust", r"windy|dust|sand|squall|tornado"),
    ("Clear", r"fair|clear"),
    ("Cloudy", r"cloud|overcast"),
]


def add_weather_group(df):
    cond = F.lower(F.col("Weather_Condition"))
    expr = F.when(F.col("Weather_Condition").isNull(), "Unknown")
    for name, pattern in WEATHER_GROUPS:
        expr = expr.when(cond.rlike(pattern), name)
    return df.withColumn("weather_group", expr.otherwise("Other"))


def main() -> None:
    spark = get_spark("clean")
    df = spark.read.parquet(PARQUET_IN)
    counts = {"input": df.count()}

    df = df.na.drop(subset=REQUIRED)
    counts["after_required_notnull"] = df.count()

    df = df.filter(
        F.col("Start_Lat").between(LAT_MIN, LAT_MAX)
        & F.col("Start_Lng").between(LNG_MIN, LNG_MAX)
    )
    counts["after_coord_bbox"] = df.count()

    df = df.withColumn(
        "duration_min",
        (F.col("End_Time").cast("long") - F.col("Start_Time").cast("long")) / 60.0,
    ).filter(F.col("duration_min").between(0, MAX_DURATION_MIN))
    counts["after_duration_filter"] = df.count()

    df = add_weather_group(df)
    df = df.withColumn(
        "precip_missing", F.col("Precipitation(in)").isNull().cast("int")
    ).withColumn(
        "Precipitation(in)", F.coalesce(F.col("Precipitation(in)"), F.lit(0.0))
    )

    (
        df.repartition("State")
        .write.mode("overwrite")
        .partitionBy("State")
        .parquet(PARQUET_OUT)
    )

    clean = spark.read.parquet(PARQUET_OUT)
    counts["output"] = clean.count()
    weather_dist = {
        r["weather_group"]: r["count"]
        for r in clean.groupBy("weather_group").count().orderBy(F.desc("count")).collect()
    }

    summary = {
        "row_counts": counts,
        "dropped_total": counts["input"] - counts["output"],
        "weather_group_distribution": weather_dist,
    }
    SUMMARY_OUT.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_OUT.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nYazıldı: {PARQUET_OUT}, {SUMMARY_OUT}")
    spark.stop()


if __name__ == "__main__":
    main()
