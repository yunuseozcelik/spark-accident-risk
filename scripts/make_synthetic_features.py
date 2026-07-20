"""Sentetik öznitelik parquet'i üretir (smoke test / CI için).

Gerçek US-Accidents verisi (~3 GB, gitignore) indirilmeden Faz 5 modelleme
pipeline'ının uçtan uca çalıştığını doğrulamak için `accidents_features.parquet`
ile aynı şemada küçük, rastgele ama gerçekçi bir tablo yazar. Severity dağılımı
gerçek veriye benzer şekilde dengesiz tutulur (çoğunluk Severity 2).

Kullanım:
    .venv/Scripts/python.exe scripts/make_synthetic_features.py --rows 20000
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.models.pipeline import ROAD_FLAGS, WEATHER_NUM
from src.spark_session import get_spark

OUT = "data/processed/synthetic_features.parquet"
WEATHER_GROUPS = ["Clear", "Cloudy", "Rain", "Snow_Ice", "Fog_Low_Vis",
                  "Thunderstorm", "Windy_Dust", "Other", "Unknown"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--out", default=OUT)
    args = parser.parse_args()

    spark = get_spark("make-synthetic")
    df = spark.range(args.rows).withColumnRenamed("id", "row_id")

    df = (
        df
        .withColumn("ID", F.concat(F.lit("SYN-"), F.col("row_id").cast("string")))
        # Severity 1-4, gerçekçi dengesiz dağılım (~%3/%80/%12/%5)
        .withColumn("_u", F.rand(1))
        .withColumn("Severity",
            F.when(F.col("_u") < 0.03, 1)
             .when(F.col("_u") < 0.83, 2)
             .when(F.col("_u") < 0.95, 3)
             .otherwise(4))
        .withColumn("high_risk", (F.col("Severity") >= 3).cast("int"))
        # Zaman öznitelikleri
        .withColumn("hour", (F.rand(2) * 24).cast("int"))
        .withColumn("day_of_week", (F.rand(3) * 7 + 1).cast("int"))
        .withColumn("month", (F.rand(4) * 12 + 1).cast("int"))
        .withColumn("year", (F.rand(5) * 8 + 2016).cast("int"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
        .withColumn("is_rush_hour", F.col("hour").isin(7, 8, 9, 16, 17, 18).cast("int"))
        .withColumn("is_night", (F.rand(6) < 0.35).cast("int"))
        .withColumn("duration_min", F.round(F.rand(7) * 240 + 5, 1))
        .withColumn("Distance(mi)", F.round(F.rand(8) * 3, 3))
        # Hava durumu: kategorik + sayısal (bir kısmı null -> Imputer denenir)
        .withColumn("_wg", (F.rand(9) * len(WEATHER_GROUPS)).cast("int"))
        .withColumn("weather_group",
            F.element_at(F.array(*[F.lit(w) for w in WEATHER_GROUPS]), F.col("_wg") + 1))
        .withColumn("Temperature(F)",
            F.when(F.rand(10) < 0.05, None).otherwise(F.round(F.rand(11) * 100 - 10, 1)))
        .withColumn("Humidity(%)", F.round(F.rand(12) * 100, 0))
        .withColumn("Pressure(in)", F.round(F.rand(13) * 3 + 28, 2))
        .withColumn("Visibility(mi)", F.round(F.rand(14) * 10, 1))
        .withColumn("Wind_Speed(mph)", F.round(F.rand(15) * 30, 1))
        .withColumn("Precipitation(in)", F.round(F.rand(16) * 0.5, 2))
        .withColumn("precip_missing", (F.rand(17) < 0.28).cast("int"))
        # Konum + eyalet
        .withColumn("Start_Lat", F.round(F.rand(18) * 25 + 25, 5))
        .withColumn("Start_Lng", F.round(F.rand(19) * -60 - 70, 5))
        .withColumn("County", F.concat(F.lit("County_"), (F.rand(20) * 50).cast("int").cast("string")))
        .withColumn("State", F.element_at(
            F.array(*[F.lit(s) for s in ["CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA"]]),
            (F.rand(21) * 8).cast("int") + 1))
    )
    # Yol bayrakları: seyrek 0/1
    for i, c in enumerate(ROAD_FLAGS):
        df = df.withColumn(c, (F.rand(30 + i) < 0.08).cast("int"))

    df = df.drop("row_id", "_u", "_wg")

    (df.write.mode("overwrite").partitionBy("State").parquet(args.out))
    out = spark.read.parquet(args.out)
    print(f"Yazıldı: {args.out} — {out.count():,} satır, {len(out.columns)} sütun")
    print("Severity dağılımı:")
    out.groupBy("Severity").count().orderBy("Severity").show()
    spark.stop()


if __name__ == "__main__":
    main()
