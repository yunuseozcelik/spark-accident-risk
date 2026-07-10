# Traffic Accident Severity Analysis and Spatial Risk Mapping with Apache Spark and Apache Sedona

**Yunus Emre Özçelik** (221401001) · **Ali Kağan Güven** (221401002)
BİL 401 — Big Data and Distributed Data Processing, TOBB ETÜ

## Abstract

Traffic accidents impose a significant burden on transportation networks, and
understanding the factors that drive their severity is key to targeted prevention.
In this project, we build an end-to-end distributed data pipeline on the
US-Accidents dataset (~7.7M records, 49 US states, 2016–2023) using Apache Spark.
The pipeline covers distributed ingestion and conversion of raw CSV data into
partitioned Parquet, data cleaning, feature engineering over temporal, weather,
and road/POI attributes, and spatial enrichment: accident coordinates are joined
with US Census TIGER/Line state and county boundary polygons using Apache Sedona
to produce regional risk maps. On top of the engineered features, we train Spark
MLlib classifiers — multinomial logistic regression and random forest for the
4-level severity task, and logistic regression, random forest, and gradient-boosted
trees for a binary high-risk (Severity ≥ 3) task — and compare them with class-aware
metrics (weighted F1, ROC-AUC, PR-AUC, high-risk recall). Beyond predictive
performance, we report feature importance analyses and county-level accident
density and high-severity-rate maps. *(Sonuçlar geldikçe: one sentence with key
quantitative findings.)* Note that the Severity field measures impact on traffic
flow rather than injury outcomes, and we use the term accordingly.

## 1. Introduction

*(Taslak — hafta 7 için zorunlu değil ama kısa bir giriş iyi durur.)*
Problem, motivasyon, araştırma sorusu:

> To what extent can accident severity be explained and predicted from time of
> day, weather conditions, road/POI features, and location? Which state/county
> regions carry high accident density and high severity risk?

## 2. Related Work

*(Ali Kağan — okumalar sonrası doldurulacak. Plan:)*

- **Moosavi et al. (2019), "A Countrywide Traffic Accident Dataset"
  (arXiv:1906.05409)** — veri setini tanıtan ana makale; toplama yöntemi ve
  temel istatistikler. Bizim farkımız: Sedona ile mekânsal join + bölgesel risk
  haritalama + dağıtık uçtan uca pipeline.
- **Moosavi et al. (2019), "Accident Risk Prediction based on Heterogeneous
  Sparse Data" (CIKM/SIGSPATIAL)** — aynı ekibin tahmin çalışması.
- *US-Accidents ile severity tahmini yapan 1-2 çalışma daha (Google Scholar:
  "US-Accidents severity prediction").*
- *Apache Sedona / mekânsal büyük veri işleme üzerine 1 makale (Yu et al.,
  GeoSpark/Sedona).*

Her çalışma için: ne yapmışlar, ne bulmuşlar, bizden farkı ne (1-2 cümle).

## 3. Proposed Implementation

### 3.1 Dataset

US-Accidents (Feb 2016 – Mar 2023): ~7.7M records, ~46 columns, ~3 GB CSV.
Target: `Severity` (ordinal 1–4, traffic-impact scale). Key feature groups:
temporal (`Start_Time`, duration), weather (`Temperature`, `Humidity`,
`Visibility`, `Wind_Speed`, `Precipitation`, `Weather_Condition`), road/POI
flags (`Junction`, `Traffic_Signal`, `Crossing`, `Stop`, `Roundabout`), and
location (`Start_Lat`, `Start_Lng`, `City`, `County`, `State`). Boundary data:
US Census TIGER/Line state and county polygons.

### 3.2 Pipeline Architecture

| Stage | Operation | Output |
|---|---|---|
| 1. Ingestion | CSV read, schema/type casting, CSV → Parquet partitioned by state | Fast, reproducible read layer |
| 2. Cleaning | Null analysis, outlier coordinate/duration filters, categorical normalization | Analysis-ready base table |
| 3. Feature engineering | Hour, day, month, weekend, rush hour, accident duration, weather groups | Enriched features for MLlib |
| 4. Spatial processing | Sedona point-in-polygon join (`ST_Contains`); county/state aggregation | Regional density & risk metrics |
| 5. Modeling | StringIndexer → OneHotEncoder → VectorAssembler; LR / RF / GBT | Predictive performance, feature importance |
| 6. Visualization | Aggregates exported to Pandas/Folium | Charts, confusion matrix, risk maps |

### 3.3 Modeling Plan

- **Multiclass:** Severity 1–4 with multinomial logistic regression and random
  forest.
- **Binary high-risk:** Severity ≥ 3 relabeled as high-risk; LR, RF, and
  GBTClassifier compared (GBT in Spark MLlib is binary-only, hence the split).
- **Class imbalance:** class weighting, sampling, and threshold analysis as
  needed.
- **Metrics:** accuracy, weighted F1, macro/weighted precision-recall, confusion
  matrix (multiclass); ROC-AUC, PR-AUC, F1, high-risk recall (binary).

### 3.4 Environment

Local Apache Spark 3.5 (PySpark) in local/standalone mode on personal machines
(≤8 GB RAM); Apache Sedona 1.9 for spatial SQL; Python 3.10, OpenJDK 17.
Data is partitioned by state so that analyses can be restricted to selected
states/years under local resource constraints without changing the pipeline.

## 4. Results

*(Final rapor için — model karşılaştırma tablosu, feature importance, haritalar.)*

## 5. Conclusion

*(Final rapor için.)*

## References

1. Moosavi, S., Samavatian, M. H., Parthasarathy, S., & Ramnath, R. (2019).
   A Countrywide Traffic Accident Dataset. arXiv:1906.05409.
2. US-Accidents Dataset. kaggle.com/datasets/sobhanmoosavi/us-accidents
3. U.S. Census Bureau TIGER/Line Shapefiles.
4. Apache Spark Documentation. spark.apache.org/docs/latest/
5. Apache Sedona Documentation. sedona.apache.org/latest/
