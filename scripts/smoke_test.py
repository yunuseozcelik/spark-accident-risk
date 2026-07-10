"""PySpark kurulum doğrulaması: session açar, küçük bir DataFrame işler."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.spark_session import get_spark

spark = get_spark("smoke-test")
df = spark.createDataFrame(
    [(1, "TX", 3), (2, "CA", 2), (3, "TX", 4), (4, "FL", 1)],
    ["id", "state", "severity"],
)
result = df.groupBy("state").avg("severity").orderBy("state").collect()
print("Spark version:", spark.version)
for row in result:
    print(row["state"], "->", row["avg(severity)"])
spark.stop()
print("SMOKE TEST OK")
