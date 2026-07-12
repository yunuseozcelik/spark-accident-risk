"""Faz 4: tam veri üzerinde ilçe bazlı kaza yoğunluğu ve risk metrikleri.

Tüm kazaları (7,7M nokta) ABD ilçe poligonlarıyla (3.235) Sedona ST_Contains
ile eşler ve üretir:
- outputs/metrics/county_risk.csv: ilçe başına kaza sayısı, high-risk oranı,
  ortalama şiddet (harita katmanının veri kaynağı)
- outputs/metrics/state_mismatch.csv: etiketli State ile poligondan gelen
  eyaletin uyuşmadığı kayıt sayıları (mekânsal doğrulama; SC demosundaki
  veri kalitesi bulgusunun tam veri hali)

Kullanım:
    .venv/bin/python src/spatial/county_risk.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

FEATURES = "data/processed/accidents_features.parquet"
COUNTY_WKT = "data/boundaries/counties_wkt.parquet"
STATE_WKT = "data/boundaries/states_wkt.parquet"
OUT_DIR = Path("outputs/metrics")


def main() -> None:
    spark = get_spark("county-risk", with_sedona=True)

    counties = (
        spark.read.parquet(COUNTY_WKT)
        .select(
            F.expr("ST_GeomFromWKT(wkt)").alias("geometry"),
            F.concat("STATEFP", "COUNTYFP").alias("geoid"),
            F.col("STATEFP").alias("state_fp"),
            F.col("NAME").alias("county_name"),
        )
    )
    state_lookup = (
        spark.read.parquet(STATE_WKT)
        .select(F.col("STATEFP").alias("state_fp"), F.col("STUSPS").alias("poly_state"))
    )

    acc = (
        spark.read.parquet(FEATURES)
        .select("ID", "Severity", "high_risk", "Start_Lat", "Start_Lng", "State")
        .withColumn("point", F.expr("ST_Point(Start_Lng, Start_Lat)"))
    )
    n_total = acc.count()

    acc.createOrReplaceTempView("acc")
    counties.createOrReplaceTempView("counties")

    joined = spark.sql(
        """
        SELECT a.ID, a.Severity, a.high_risk, a.State,
               c.geoid, c.state_fp, c.county_name
        FROM acc a JOIN counties c
          ON ST_Contains(c.geometry, a.point)
        """
    ).join(state_lookup, "state_fp").cache()

    n_joined = joined.count()
    print(f"Eşleşen kayıt: {n_joined:,} / {n_total:,} "
          f"(poligon dışı: {n_total - n_joined:,})")

    risk = (
        joined.groupBy("geoid", "poly_state", "county_name")
        .agg(
            F.count("*").alias("n_accidents"),
            F.round(F.avg("high_risk"), 4).alias("high_risk_rate"),
            F.round(F.avg("Severity"), 3).alias("avg_severity"),
        )
        .orderBy(F.desc("n_accidents"))
    )
    risk_pd = risk.toPandas()
    risk_pd.to_csv(OUT_DIR / "county_risk.csv", index=False)
    print(f"Yazıldı: {OUT_DIR / 'county_risk.csv'} ({len(risk_pd)} ilçe)")

    mismatch = (
        joined.filter(F.col("State") != F.col("poly_state"))
        .groupBy(F.col("State").alias("labeled_state"), F.col("poly_state"))
        .count()
        .orderBy(F.desc("count"))
    )
    mismatch_pd = mismatch.toPandas()
    mismatch_pd.to_csv(OUT_DIR / "state_mismatch.csv", index=False)
    print(f"Eyalet uyuşmazlığı: {int(mismatch_pd['count'].sum()):,} kayıt, "
          f"{len(mismatch_pd)} eyalet çifti — {OUT_DIR / 'state_mismatch.csv'}")
    spark.stop()


if __name__ == "__main__":
    main()
