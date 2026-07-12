"""Öznitelik üretimi: temiz katmandan MLlib'e hazır öznitelik tablosu üretir.

Üretilen öznitelikler (öneri raporu Faz 3):
- Zaman: hour, day_of_week, month, year, is_weekend, is_rush_hour
- Süre: duration_min (temizlemeden gelir)
- Hava: sayısal hava sütunları + weather_group + precip_missing
- Yol/POI: True/False string bayrakları 0/1'e çevrilir
- Etiketler: Severity (1-4) + high_risk (Severity >= 3)

Modelde kullanılmayacak serbest metin/ID sütunları taşınmaz.

Kullanım:
    .venv/bin/python src/features/build_features.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET_IN = "data/processed/accidents_clean.parquet"
PARQUET_OUT = "data/processed/accidents_features.parquet"

WEATHER_NUM = [
    "Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
    "Wind_Speed(mph)", "Precipitation(in)",
]
ROAD_FLAGS = [
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
    "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
    "Traffic_Signal", "Turning_Loop",
]
# Sabah 07-09 ve akşam 16-18 (yerel saat Start_Time'da)
RUSH_HOURS = [7, 8, 9, 16, 17, 18]


def main() -> None:
    spark = get_spark("build-features")
    df = spark.read.parquet(PARQUET_IN)

    df = (
        df.withColumn("hour", F.hour("Start_Time"))
        .withColumn("day_of_week", F.dayofweek("Start_Time"))
        .withColumn("month", F.month("Start_Time"))
        .withColumn("year", F.year("Start_Time"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn("is_rush_hour", F.col("hour").isin(RUSH_HOURS).cast("int"))
        .withColumn("is_night", (F.col("Sunrise_Sunset") == "Night").cast("int"))
        .withColumn("high_risk", (F.col("Severity") >= 3).cast("int"))
    )
    for c in ROAD_FLAGS:
        df = df.withColumn(c, (F.lower(F.col(c)) == "true").cast("int"))

    features = df.select(
        "ID", "Severity", "high_risk",
        "hour", "day_of_week", "month", "year",
        "is_weekend", "is_rush_hour", "is_night",
        "duration_min", "Distance(mi)",
        *WEATHER_NUM, "weather_group", "precip_missing",
        *ROAD_FLAGS,
        "Start_Lat", "Start_Lng", "County", "State",
    )

    (
        features.repartition("State")
        .write.mode("overwrite")
        .partitionBy("State")
        .parquet(PARQUET_OUT)
    )

    out = spark.read.parquet(PARQUET_OUT)
    print(f"Yazıldı: {PARQUET_OUT} — {out.count():,} satır, {len(out.columns)} sütun")
    out.printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
