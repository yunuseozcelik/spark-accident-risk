"""US-Accidents CSV -> Parquet dönüşümü.

Ham CSV'yi okur, tip dönüşümlerini yapar ve eyalet bazlı partition'lanmış
Parquet olarak yazar. Sonraki tüm aşamalar Parquet'ten okur.

Kullanım:
    .venv/bin/python src/ingestion/csv_to_parquet.py [csv_yolu]
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

RAW_CSV = "data/raw/US_Accidents_March23.csv"
PARQUET_OUT = "data/processed/accidents.parquet"

TIMESTAMP_COLS = ["Start_Time", "End_Time", "Weather_Timestamp"]
DOUBLE_COLS = [
    "Start_Lat", "Start_Lng", "End_Lat", "End_Lng", "Distance(mi)",
    "Temperature(F)", "Wind_Chill(F)", "Humidity(%)", "Pressure(in)",
    "Visibility(mi)", "Wind_Speed(mph)", "Precipitation(in)",
]


def main(csv_path: str = RAW_CSV) -> None:
    spark = get_spark("csv-to-parquet")
    df = spark.read.csv(csv_path, header=True, inferSchema=False)

    df = df.withColumn("Severity", F.col("Severity").cast("int"))
    for c in TIMESTAMP_COLS:
        # Kaynakta kesirli saniyeli ve saniyesiz karışık format var
        df = df.withColumn(c, F.to_timestamp(F.col(c)))
    for c in DOUBLE_COLS:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))

    (
        df.repartition("State")
        .write.mode("overwrite")
        .partitionBy("State")
        .parquet(PARQUET_OUT)
    )

    n = spark.read.parquet(PARQUET_OUT).count()
    print(f"Parquet yazıldı: {PARQUET_OUT} — {n:,} satır")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else RAW_CSV)
