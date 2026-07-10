"""Proje genelinde ortak SparkSession kurulumu.

Yerel makine kısıtlarına göre ayarlanmıştır (Jetson, 7.4 GB RAM, 6 çekirdek):
- driver memory 3g: sistemin geri kalanına yer bırakır
- shuffle partitions 24: küçük küme için 200 varsayılanı gereksiz yüksek
"""
from pyspark.sql import SparkSession


def get_spark(app_name: str = "spark-accident-risk", with_sedona: bool = False) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if with_sedona:
        from sedona.spark import SedonaContext
        builder = (
            builder
            .config(
                "spark.jars.packages",
                "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.9.0,"
                "org.datasyslab:geotools-wrapper:1.7.1-28.5",
            )
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        )
        spark = SedonaContext.create(builder.getOrCreate())
        return spark
    return builder.getOrCreate()
