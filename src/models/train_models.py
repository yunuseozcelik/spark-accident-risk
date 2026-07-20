"""Faz 5 — MLlib model eğitimi ve karşılaştırması.

İki görev:
  1. Çok sınıflı Severity (1-4):      Multinomial LR + RandomForest
  2. İkili high_risk (Severity >= 3): LR + RandomForest + GBTClassifier

Her model için sınıf dengesizliği ağırlıklandırma (weightCol) uygulanır ve
uygun metrikler hesaplanır. Sonuçlar `outputs/metrics/` altına yazılır:
  - model_comparison.json / .csv   (tüm modellerin metrik tablosu)
  - feature_importances.json      (ağaç modelleri için öznitelik önemleri)
  - confusion_*.json               (karışıklık matrisleri)

Kullanım:
    .venv/Scripts/python.exe src/models/train_models.py
    .venv/Scripts/python.exe src/models/train_models.py --input data/processed/synthetic_features.parquet
"""
import argparse
import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.ml import Pipeline
from pyspark.ml.classification import (
    GBTClassifier,
    LogisticRegression,
    RandomForestClassifier,
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
)
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.models.pipeline import build_feature_stages
from src.spark_session import get_spark

PARQUET_IN = "data/processed/accidents_features.parquet"
METRICS_DIR = Path("outputs/metrics")
SEED = 42


# --------------------------------------------------------------------------
# Sınıf ağırlıkları (dengesizlik önlemi)
# --------------------------------------------------------------------------
def add_class_weight(df: DataFrame, label_col: str) -> DataFrame:
    """Dengeli sınıf ağırlığı kolonu ekler: w(c) = N / (K * n_c).

    Böylece azınlık sınıfları (ör. Severity 1/4 veya high_risk=1) eğitimde
    orantılı olarak daha fazla ağırlık alır.
    """
    counts = {r[label_col]: r["n"] for r in df.groupBy(label_col).agg(F.count("*").alias("n")).collect()}
    total = sum(counts.values())
    k = len(counts)
    weights = {c: total / (k * n) for c, n in counts.items()}

    expr = F.lit(1.0)
    for c, w in weights.items():
        expr = F.when(F.col(label_col) == c, F.lit(float(w))).otherwise(expr)
    return df.withColumn("classWeight", expr), weights


# --------------------------------------------------------------------------
# Öznitelik adları (feature importance yorumu için)
# --------------------------------------------------------------------------
def extract_feature_names(transformed: DataFrame) -> list:
    """`features` vektör kolonunun metadata'sından öznitelik adlarını çıkarır."""
    meta = transformed.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
    names = {}
    for attrs in meta.values():
        for a in attrs:
            names[a["idx"]] = a["name"]
    return [names[i] for i in sorted(names)]


def importances_to_dict(model, feature_names: list, top_k: int = 20) -> dict:
    """featureImportances vektörünü ad->önem eşlemesine çevirir (en önemli top_k)."""
    imp = model.featureImportances.toArray()
    pairs = sorted(
        ((feature_names[i], float(imp[i])) for i in range(len(imp))),
        key=lambda x: x[1],
        reverse=True,
    )
    return dict(pairs[:top_k])


# --------------------------------------------------------------------------
# Değerlendirme
# --------------------------------------------------------------------------
def eval_multiclass(pred: DataFrame, label_col: str) -> dict:
    ev = lambda m: MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName=m
    ).evaluate(pred)
    return {
        "accuracy": ev("accuracy"),
        "weighted_f1": ev("f1"),
        "weighted_precision": ev("weightedPrecision"),
        "weighted_recall": ev("weightedRecall"),
    }


def eval_binary(pred: DataFrame, label_col: str) -> dict:
    mc = lambda m: MulticlassClassificationEvaluator(
        labelCol=label_col, predictionCol="prediction", metricName=m
    ).evaluate(pred)
    roc = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    ).evaluate(pred)
    pr = BinaryClassificationEvaluator(
        labelCol=label_col, rawPredictionCol="rawPrediction", metricName="areaUnderPR"
    ).evaluate(pred)
    # Yüksek-risk (pozitif sınıf = 1) recall: TP / (TP + FN)
    tp = pred.filter((F.col(label_col) == 1) & (F.col("prediction") == 1)).count()
    fn = pred.filter((F.col(label_col) == 1) & (F.col("prediction") == 0)).count()
    high_risk_recall = tp / (tp + fn) if (tp + fn) else 0.0
    return {
        "accuracy": mc("accuracy"),
        "weighted_f1": mc("f1"),
        "roc_auc": roc,
        "pr_auc": pr,
        "high_risk_recall": high_risk_recall,
    }


def confusion_matrix(pred: DataFrame, label_col: str) -> dict:
    rows = pred.groupBy(label_col, "prediction").count().collect()
    cm = {}
    for r in rows:
        cm.setdefault(str(int(r[label_col])), {})[str(int(r["prediction"]))] = r["count"]
    return cm


# --------------------------------------------------------------------------
# Ana akış
# --------------------------------------------------------------------------
def run_task(train, test, label_col, models, evaluator, feature_names_holder):
    """Bir görev için verilen modelleri eğitir ve metrikleri toplar."""
    results = {}
    importances = {}
    confusions = {}
    for name, clf in models:
        model = clf.fit(train)
        pred = model.transform(test)
        results[name] = evaluator(pred, label_col)
        confusions[name] = confusion_matrix(pred, label_col)
        # Ağaç modellerinde öznitelik önemi
        if hasattr(model, "featureImportances"):
            importances[name] = importances_to_dict(model, feature_names_holder[0])
        print(f"  [{label_col}] {name}: {json.dumps(results[name], ensure_ascii=False)}")
    return results, importances, confusions


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=PARQUET_IN, help="Öznitelik parquet yolu")
    parser.add_argument("--sample", type=float, default=None, help="Örnekleme oranı (0-1)")
    args = parser.parse_args()

    spark = get_spark("faz5-train")
    df = spark.read.parquet(args.input)
    if args.sample:
        df = df.sample(fraction=args.sample, seed=SEED)

    # Severity 1-4 -> LR/RF etiketleri 0-tabanlı olmalı
    df = df.withColumn("severity_label", (F.col("Severity") - 1).cast("double"))
    df = df.withColumn("high_risk", F.col("high_risk").cast("double"))

    # Öznitelik pipeline'ını bir kez fit et, iki görevde de kullan
    feat_pipeline = Pipeline(stages=build_feature_stages()).fit(df)
    feat_df = feat_pipeline.transform(df).cache()
    feature_names = extract_feature_names(feat_df)
    feature_names_holder = [feature_names]
    print(f"Toplam öznitelik boyutu: {len(feature_names)}")

    n_total = feat_df.count()
    train, test = feat_df.randomSplit([0.8, 0.2], seed=SEED)
    train = train.cache()
    print(f"Satır: {n_total:,} | eğitim: {train.count():,} | test: {test.count():,}")

    # --- Görev 1: Çok sınıflı Severity ---
    train_m, w_m = add_class_weight(train, "severity_label")
    print(f"\n=== Görev 1: Çok sınıflı Severity === (sınıf ağırlıkları: "
          f"{ {int(k): round(v,3) for k,v in w_m.items()} })")
    multiclass_models = [
        ("logreg", LogisticRegression(
            featuresCol="features", labelCol="severity_label",
            weightCol="classWeight", maxIter=50, family="multinomial")),
        ("random_forest", RandomForestClassifier(
            featuresCol="features", labelCol="severity_label",
            weightCol="classWeight", numTrees=60, maxDepth=10, seed=SEED)),
    ]
    res_m, imp_m, cm_m = run_task(
        train_m, test, "severity_label", multiclass_models,
        eval_multiclass, feature_names_holder)

    # --- Görev 2: İkili high_risk ---
    train_b, w_b = add_class_weight(train, "high_risk")
    print(f"\n=== Görev 2: İkili high_risk === (sınıf ağırlıkları: "
          f"{ {int(k): round(v,3) for k,v in w_b.items()} })")
    binary_models = [
        ("logreg", LogisticRegression(
            featuresCol="features", labelCol="high_risk",
            weightCol="classWeight", maxIter=50)),
        ("random_forest", RandomForestClassifier(
            featuresCol="features", labelCol="high_risk",
            weightCol="classWeight", numTrees=60, maxDepth=10, seed=SEED)),
        ("gbt", GBTClassifier(
            featuresCol="features", labelCol="high_risk",
            weightCol="classWeight", maxIter=40, maxDepth=6, seed=SEED)),
    ]
    res_b, imp_b, cm_b = run_task(
        train_b, test, "high_risk", binary_models,
        eval_binary, feature_names_holder)

    # --- Çıktıları yaz ---
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    comparison = {
        "n_rows": n_total,
        "n_features": len(feature_names),
        "multiclass_severity": res_m,
        "binary_high_risk": res_b,
        "class_weights": {
            "severity_label": {int(k): v for k, v in w_m.items()},
            "high_risk": {int(k): v for k, v in w_b.items()},
        },
    }
    (METRICS_DIR / "model_comparison.json").write_text(
        json.dumps(comparison, indent=2, ensure_ascii=False))

    # Düz CSV tablosu (görev, model, metrik sütunları)
    with (METRICS_DIR / "model_comparison.csv").open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["task", "model", "metric", "value"])
        for task, res in [("severity", res_m), ("high_risk", res_b)]:
            for model, metrics in res.items():
                for metric, value in metrics.items():
                    w.writerow([task, model, metric, round(value, 5)])

    (METRICS_DIR / "feature_importances.json").write_text(
        json.dumps({"severity": imp_m, "high_risk": imp_b}, indent=2, ensure_ascii=False))
    (METRICS_DIR / "confusion_matrices.json").write_text(
        json.dumps({"severity": cm_m, "high_risk": cm_b}, indent=2, ensure_ascii=False))

    print(f"\nYazıldı: {METRICS_DIR}/model_comparison.json|csv, "
          f"feature_importances.json, confusion_matrices.json")
    spark.stop()


if __name__ == "__main__":
    main()
