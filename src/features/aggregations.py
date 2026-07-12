"""Toplulaştırmalar: zaman/hava/yol bazlı kaza sayısı ve high-risk oranları.

Öznitelik tablosundan okur; rapor grafikleri ve EDA için özet CSV'ler üretir:
- saat bazlı, haftanın günü bazlı, ay/yıl bazlı
- hava grubu bazlı
- yol/POI bayrağı bazlı (bayrak var/yok karşılaştırması)

Kullanım:
    .venv/bin/python src/features/aggregations.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET = "data/processed/accidents_features.parquet"
OUT_DIR = Path("outputs/metrics")

ROAD_FLAGS = [
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
    "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
    "Traffic_Signal", "Turning_Loop",
]


def agg_by(df, col):
    return (
        df.groupBy(col)
        .agg(
            F.count("*").alias("n_accidents"),
            F.round(F.avg("high_risk"), 4).alias("high_risk_rate"),
            F.round(F.avg("duration_min"), 1).alias("avg_duration_min"),
        )
        .orderBy(col)
    )


def write_csv(sdf, name):
    path = OUT_DIR / f"agg_{name}.csv"
    sdf.toPandas().to_csv(path, index=False)
    print(f"Yazıldı: {path}")


def main() -> None:
    spark = get_spark("aggregations")
    df = spark.read.parquet(PARQUET).cache()

    write_csv(agg_by(df, "hour"), "by_hour")
    write_csv(agg_by(df, "day_of_week"), "by_day_of_week")
    write_csv(agg_by(df, "month"), "by_month")
    write_csv(agg_by(df, "year"), "by_year")
    write_csv(agg_by(df, "weather_group").orderBy(F.desc("n_accidents")), "by_weather")

    # Her yol bayrağı için bayraklı/bayraksız high-risk karşılaştırması
    rows = []
    overall = df.agg(F.avg("high_risk")).first()[0]
    for flag in ROAD_FLAGS:
        r = (
            df.groupBy(F.col(flag).alias("flag_value"))
            .agg(F.count("*").alias("n"), F.avg("high_risk").alias("hr"))
            .collect()
        )
        by_val = {row["flag_value"]: row for row in r}
        if 1 not in by_val:  # hiç işaretli kayıt yoksa atla (örn. Turning_Loop)
            continue
        rows.append({
            "flag": flag,
            "n_with_flag": by_val[1]["n"],
            "high_risk_with_flag": round(by_val[1]["hr"], 4),
            "high_risk_without_flag": round(by_val[0]["hr"], 4) if 0 in by_val else None,
        })
    import pandas as pd
    pd.DataFrame(rows).sort_values("n_with_flag", ascending=False).to_csv(
        OUT_DIR / "agg_by_road_flag.csv", index=False
    )
    print(f"Yazıldı: {OUT_DIR / 'agg_by_road_flag.csv'} (genel high-risk oranı: {overall:.4f})")
    spark.stop()


if __name__ == "__main__":
    main()
