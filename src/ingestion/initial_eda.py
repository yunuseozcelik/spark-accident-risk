"""İlk EDA: status report için temel veri seti istatistikleri.

Parquet katmanından okur; çıktıları outputs/metrics/ altına yazar.

Kullanım:
    .venv/bin/python src/ingestion/initial_eda.py
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET = "data/processed/accidents.parquet"
OUT_DIR = Path("outputs/metrics")

KEY_COLS = [
    "Severity", "Start_Time", "Start_Lat", "Start_Lng", "City", "County",
    "State", "Temperature(F)", "Humidity(%)", "Visibility(mi)",
    "Wind_Speed(mph)", "Precipitation(in)", "Weather_Condition",
    "Junction", "Traffic_Signal", "Crossing",
]


def main() -> None:
    spark = get_spark("initial-eda")
    df = spark.read.parquet(PARQUET)
    df.cache()

    n_rows = df.count()
    date_range = df.select(
        F.min("Start_Time").alias("min"), F.max("Start_Time").alias("max")
    ).first()

    severity = {
        str(r["Severity"]): r["count"]
        for r in df.groupBy("Severity").count().orderBy("Severity").collect()
    }
    top_states = {
        r["State"]: r["count"]
        for r in df.groupBy("State").count().orderBy(F.desc("count")).limit(10).collect()
    }
    null_rates = {
        c: df.filter(F.col(c).isNull()).count() / n_rows
        for c in KEY_COLS if c in df.columns
    }

    summary = {
        "n_rows": n_rows,
        "n_cols": len(df.columns),
        "date_min": str(date_range["min"]),
        "date_max": str(date_range["max"]),
        "severity_distribution": severity,
        "top10_states": top_states,
        "null_rates_key_cols": {k: round(v, 4) for k, v in null_rates.items()},
    }

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / "initial_eda.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False))
    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nYazıldı: {out}")
    spark.stop()


if __name__ == "__main__":
    main()
