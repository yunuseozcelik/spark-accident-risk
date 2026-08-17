"""Demo günü: Spark Web UI'ı canlı göstermek için gerçek veri üzerinde iş çalıştırır.

`smoke_test.py` saniyeler içinde bittiği için Web UI (localhost:4040) kapanır ve
izleyiciye gösterilemez. Bu script öznitelik tablosunun tamamı üzerinde birkaç
gerçek Spark işi (count + iki shuffle'lı toplulaştırma) çalıştırır, sonuçları
yazar ve ardından SparkSession'ı **açık tutar** — böylece DAG, stage/task
dağılımı ve executor sekmeleri tarayıcıdan incelenebilir.

Kullanım (proje kökünden):
    powershell -File scripts/run_demo_ui.ps1
veya:
    .venv/Scripts/python.exe scripts/demo_spark_ui.py

    --input  : okunacak parquet (varsayılan: öznitelik tablosu)
    --hold N : UI'ı N saniye açık tut (varsayılan: Enter'a basılana kadar)
"""
import argparse
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import functions as F

from src.spark_session import get_spark

PARQUET_IN = "data/processed/accidents_features.parquet"


def banner(text: str) -> None:
    print("\n" + "=" * 62)
    print(f"  {text}")
    print("=" * 62)


def wait(prompt: str) -> None:
    """Enter bekler; stdin yoksa (script olarak cagrildiysa) beklemeden gecer."""
    try:
        input(prompt)
    except EOFError:
        print(prompt + "(stdin yok, devam)")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=PARQUET_IN)
    parser.add_argument("--hold", type=int, default=0,
                        help="UI'i N saniye acik tut; 0 ise Enter beklenir")
    args = parser.parse_args()

    spark = get_spark("DEMO - Accident Risk Pipeline")
    sc = spark.sparkContext

    banner("SPARK WEB UI")
    print(f"  Tarayicida ac  ->  {sc.uiWebUrl}")
    print(f"  Master         :  {sc.master}")
    print(f"  Spark surumu   :  {spark.version}")
    print("\n  (UI acilana kadar bekle, sonra Enter ile isleri baslat)")
    wait("  >> Baslatmak icin Enter: ")

    # --- Is 1: dagitik okuma + sayim ------------------------------------
    banner("IS 1/3 - Parquet okuma ve satir sayimi")
    t0 = time.time()
    df = spark.read.parquet(args.input).cache()
    n = df.count()
    print(f"  Satir sayisi : {n:,}")
    print(f"  Sutun sayisi : {len(df.columns)}")
    print(f"  Sure         : {time.time() - t0:.1f} sn")

    # --- Is 2: hava durumuna gore high-risk orani (shuffle) --------------
    banner("IS 2/3 - Hava durumu grubuna gore yuksek-risk orani (shuffle)")
    t0 = time.time()
    weather = (
        df.groupBy("weather_group")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("high_risk"), 4).alias("high_risk_rate"),
            F.round(F.avg("Severity"), 3).alias("avg_severity"),
        )
        .orderBy(F.desc("n"))
    )
    weather.show(truncate=False)
    print(f"  Sure: {time.time() - t0:.1f} sn")

    # --- Is 3: saat bazli desen (shuffle + siralama) ---------------------
    banner("IS 3/3 - Saat bazli yuksek-risk orani (shuffle + sort)")
    t0 = time.time()
    hourly = (
        df.groupBy("hour")
        .agg(
            F.count("*").alias("n"),
            F.round(F.avg("high_risk"), 4).alias("high_risk_rate"),
        )
        .orderBy("hour")
    )
    hourly.show(24, truncate=False)
    print(f"  Sure: {time.time() - t0:.1f} sn")

    banner("BITTI - Web UI hala acik")
    print(f"  {sc.uiWebUrl}")
    print("  Bakilacak sekmeler:")
    print("    Jobs       -> 3 is, her biri stage'lere bolunmus")
    print("    Stages     -> her stage'in task sayisi (paralellik kaniti)")
    print("    Storage    -> cache'lenen DataFrame ve bellekteki boyutu")
    print("    Executors  -> aktif cekirdek sayisi ve islenen veri")
    print("    SQL/DataFrame -> sorgunun fiziksel plani (DAG)")

    if args.hold > 0:
        print(f"\n  {args.hold} saniye acik kalacak...")
        time.sleep(args.hold)
    else:
        wait("\n  >> Kapatmak icin Enter: ")

    spark.stop()
    print("  Spark kapatildi.")


if __name__ == "__main__":
    main()
