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
density and high-severity-rate maps. As a preliminary demonstration, the full
dataset has been converted into a state-partitioned Parquet layer (2.9 GB CSV →
645 MB) and a Sedona point-in-polygon join of 383K South Carolina accidents
against 3,235 county polygons produced county-level accident density and
high-severity-rate metrics on a resource-constrained local cluster.
Note that the Severity field measures impact on traffic
flow rather than injury outcomes, and we use the term accordingly.

## 1. Introduction

Traffic accidents are a persistent source of congestion, economic loss, and harm
on road networks. Large-scale accident records combined with contextual signals —
time of day, weather, and road infrastructure — make it possible to study which
conditions are associated with more disruptive accidents and where such accidents
concentrate geographically. However, nationwide accident data at the scale of
millions of records exceeds what single-machine, in-memory tools handle
comfortably, motivating a distributed processing approach.

This project investigates two questions on the US-Accidents dataset:

> To what extent can accident severity be explained and predicted from time of
> day, weather conditions, road/POI features, and location? Which state/county
> regions carry high accident density and high severity risk?

We address them with an end-to-end pipeline built on Apache Spark for distributed
data processing and MLlib modeling, and Apache Sedona for spatial joins between
accident coordinates and administrative boundary polygons. The emphasis of the
project is not raw predictive accuracy but a reproducible distributed pipeline
that couples severity modeling with interpretable regional risk mapping.

## 2. Related Work

**Moosavi et al. [1]** introduce US-Accidents, a countrywide traffic accident
dataset collected continuously from real-time traffic incident feeds (MapQuest
and Bing APIs) and augmented with weather, period-of-day, and points-of-interest
attributes. The paper describes the collection and integration process and
presents descriptive statistics on temporal and environmental accident patterns.
We use the March 2023 release of this dataset (~7.7M records); where their focus
is dataset construction, ours is a distributed analysis and modeling pipeline
built on top of it.

**Moosavi et al. [2]** follow up with an accident *risk prediction* task: using
the same heterogeneous sparse signals (time, weather, POI), they propose a deep
neural model (DAP) that predicts the risk of accident occurrence for a geographic
region within a time window. Our task differs in target and method: we predict
the *severity* of an already-occurred accident with classical, interpretable
MLlib classifiers, and we complement prediction with county-level spatial risk
aggregation rather than grid-based occurrence forecasting.

**Yu et al. [3]** present GeoSpark — the system later donated to the Apache
Software Foundation as Apache Sedona — which extends Spark with spatial RDDs,
spatial partitioning, and spatial query operators such as range, kNN, and join
queries over geometric objects. This is the infrastructure work our spatial
stage relies on: we use Sedona's `ST_GeomFromWKT` / `ST_Contains` SQL interface
to join ~7.7M accident points with US Census county polygons in a distributed
fashion.

*(Ali Kağan — buraya US-Accidents üzerinde severity tahmini yapan 1-2 çalışma
daha eklenecek; okuma listesindeki adaylardan atıfları doğrulayarak. Her biri
için: ne yapmışlar, ne bulmuşlar, bizden farkı ne — 2-3 cümle.)*

In contrast to prior severity-prediction work on US-Accidents, which typically
runs on single-machine Python/scikit-learn stacks over samples of the data, our
contribution is an end-to-end distributed pipeline — ingestion, cleaning,
feature engineering, spatial enrichment, and modeling all in Spark/Sedona over
the full dataset — with regional risk maps as a first-class output alongside
model metrics.

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
2. Moosavi, S., Samavatian, M. H., Parthasarathy, S., Teodorescu, R., &
   Ramnath, R. (2019). Accident Risk Prediction based on Heterogeneous Sparse
   Data: New Dataset and Insights. In *Proc. 27th ACM SIGSPATIAL*.
3. Yu, J., Wu, J., & Sarwat, M. (2015). GeoSpark: A Cluster Computing Framework
   for Processing Large-Scale Spatial Data. In *Proc. 23rd ACM SIGSPATIAL*.
4. US-Accidents Dataset. kaggle.com/datasets/sobhanmoosavi/us-accidents
5. U.S. Census Bureau TIGER/Line Shapefiles.
6. Apache Spark Documentation. spark.apache.org/docs/latest/
7. Apache Sedona Documentation. sedona.apache.org/latest/
