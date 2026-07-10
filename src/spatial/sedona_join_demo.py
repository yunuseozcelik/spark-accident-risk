"""Sedona demo: tek eyaletin kazalarını ilçe poligonlarıyla eşler.

Nokta-poligon join'in çalıştığını doğrular ve ilçe bazlı kaza sayısı +
yüksek şiddet (Severity >= 3) oranı üretir. Status report'taki demo run için.

Kullanım:
    .venv/bin/python src/spatial/sedona_join_demo.py [STATE_KODU]  # varsayılan: SC
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET = "data/processed/accidents.parquet"
COUNTY_WKT = "data/boundaries/counties_wkt.parquet"
STATE_WKT = "data/boundaries/states_wkt.parquet"
OUT = "outputs/metrics/county_risk_demo.csv"


def main(state: str = "SC") -> None:
    spark = get_spark("sedona-join-demo", with_sedona=True)

    counties = (
        spark.read.parquet(COUNTY_WKT)
        .select(
            F.expr("ST_GeomFromWKT(wkt)").alias("geometry"),
            F.col("STATEFP").alias("state_fp"),
            F.col("NAME").alias("county_name"),
        )
    )
    state_fp = (
        spark.read.parquet(STATE_WKT)
        .filter(F.col("STUSPS") == state)
        .first()["STATEFP"]
    )

    acc = (
        spark.read.parquet(PARQUET)
        .filter(F.col("State") == state)
        .filter(F.col("Start_Lat").isNotNull() & F.col("Start_Lng").isNotNull())
        .select("ID", "Severity", "Start_Lat", "Start_Lng")
        .withColumn("point", F.expr("ST_Point(Start_Lng, Start_Lat)"))
    )

    acc.createOrReplaceTempView("acc")
    counties.createOrReplaceTempView("counties")

    joined = spark.sql(
        """
        SELECT c.state_fp, c.county_name,
               COUNT(*)                                   AS n_accidents,
               AVG(CASE WHEN a.Severity >= 3 THEN 1.0 ELSE 0.0 END) AS high_risk_rate
        FROM acc a JOIN counties c
          ON ST_Contains(c.geometry, a.point)
        GROUP BY c.state_fp, c.county_name
        ORDER BY n_accidents DESC
        """
    )

    rows = joined.collect()
    in_state = [r for r in rows if r["state_fp"] == state_fp]
    out_state = [r for r in rows if r["state_fp"] != state_fp]
    n_out = sum(r["n_accidents"] for r in out_state)
    print(f"{state}: {len(in_state)} eyalet içi ilçe eşleşti")
    if out_state:
        print(
            f"Veri kalitesi notu: State={state} etiketli {n_out:,} kayıt başka "
            f"eyaletin ilçesine düştü ({len(out_state)} ilçe) — temizlemede ele alınacak"
        )
    for r in in_state[:10]:
        print(f"  {r['county_name']:<20} {r['n_accidents']:>7,}  high-risk: {r['high_risk_rate']:.2%}")
    rows = in_state

    Path("outputs/metrics").mkdir(parents=True, exist_ok=True)
    import csv
    with open(OUT, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["county", "n_accidents", "high_risk_rate"])
        w.writerows([(r["county_name"], r["n_accidents"], round(r["high_risk_rate"], 4)) for r in rows])
    print(f"Yazıldı: {OUT}")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "SC")
