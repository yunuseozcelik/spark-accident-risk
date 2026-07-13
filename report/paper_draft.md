# Traffic Accident Severity Analysis and Spatial Risk Mapping with Apache Spark and Apache Sedona

**Yunus Emre Özçelik** (221401001) · **Ali Kağan Güven** (221401002)
BİL 401 — Big Data and Distributed Data Processing, TOBB ETÜ

## Abstract

Traffic accidents impose a significant burden on transportation networks, and understanding the temporal, meteorological, road-related, and spatial factors associated with their severity is important for traffic management and risk analysis. This project develops an end-to-end distributed data-processing pipeline for the US-Accidents dataset using Apache Spark and Apache Sedona. The implemented pipeline covers distributed CSV ingestion, conversion to state-partitioned Parquet, data cleaning, temporal and weather-based feature engineering, Spark aggregations, spatial point-in-polygon joins with US Census TIGER/Line county boundaries, and county-level risk mapping.

The dataset contains 7,728,394 accident records and 46 attributes. The raw 2.9 GB CSV file was converted into an approximately 645 MB state-partitioned Parquet layer. After removing 34,981 records with invalid accident durations, 7,693,413 records remained in the cleaned dataset. Preliminary spatial analysis has produced county-level accident-density and high-risk-rate metrics together with interactive Folium maps.

The next stage of the project will train and compare Spark MLlib classifiers for multiclass severity prediction and binary high-risk classification. Since the Severity field represents the effect of an accident on traffic flow rather than injury or fatality outcomes, all interpretations in this study use severity in the traffic-impact sense.

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

Moosavi et al. [1] introduced US-Accidents, a countrywide traffic-accident dataset collected from real-time traffic incident feeds and enriched with temporal, meteorological, and points-of-interest attributes. Their work primarily focused on dataset construction, integration, and descriptive analysis. In contrast, the present project develops a reproducible distributed processing, spatial analysis, and machine-learning pipeline on the March 2023 release of the dataset.

Moosavi et al. [2] subsequently proposed the Deep Accident Prediction model for predicting the probability of an accident occurring in a geographic region and time window from heterogeneous and sparse contextual data. Their target is future accident-occurrence risk, whereas our modeling task estimates the traffic-impact severity of an accident that has already occurred. Our work also produces county-level spatial risk indicators rather than grid-based occurrence forecasts.

Yu et al. [3] introduced GeoSpark, which later became Apache Sedona, as a cluster-computing framework that extends Apache Spark with spatial data structures, partitioning methods, indexing, and geometric query operations. The spatial stage of our project relies on this distributed spatial-processing approach. Accident coordinates are represented as geometric points and joined with US Census county polygons using Sedona spatial SQL functions.

Boyagoda and Nawarathna [4] investigated accident-severity prediction using approximately 1.5 million records and 45 attributes from an earlier release of the US-Accidents dataset. They applied supervised classification methods including decision trees, k-nearest neighbors, and random forests. Their study demonstrates that the dataset's severity label can be modeled using conventional machine-learning algorithms. However, their work primarily focuses on classification over an earlier dataset release, while our project processes the larger March 2023 release through a distributed Spark pipeline and integrates predictive modeling with spatial enrichment and county-level risk mapping.

Ennaji et al. [5] also examined machine-learning-based severity classification using the US-Accidents dataset. Their study provides further evidence that environmental, temporal, and road-related variables can be used in accident-severity prediction. Our project differs by integrating preprocessing, feature engineering, spatial processing, model training, and evaluation within one reproducible Spark and Sedona architecture. It also explicitly addresses class imbalance through class-aware evaluation metrics and a separate binary high-risk classification task.

Overall, previous studies using US-Accidents have primarily emphasized dataset construction, accident-occurrence prediction, or severity-classification performance. The main contribution of this project is the integration of large-scale ingestion, Parquet-based storage, distributed cleaning and feature engineering, Apache Sedona spatial joins, Spark MLlib modeling, and interactive regional risk maps within a single end-to-end pipeline.

## 3. Proposed Implementation

### 3.1 Dataset

US-Accidents (2016–2023; observed timestamps: 14 January 2016–1 April 2023): ~7.7M records, 46 columns, ~3 GB CSV.
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

## References

1. Moosavi, S., Samavatian, M. H., Parthasarathy, S., & Ramnath, R. (2019). A Countrywide Traffic Accident Dataset. arXiv:1906.05409.

2. Moosavi, S., Samavatian, M. H., Parthasarathy, S., Teodorescu, R., & Ramnath, R. (2019). Accident Risk Prediction based on Heterogeneous Sparse Data: New Dataset and Insights. In *Proceedings of the 27th ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems*. https://doi.org/10.1145/3347146.3359078

3. Yu, J., Wu, J., & Sarwat, M. (2015). GeoSpark: A Cluster Computing Framework for Processing Large-Scale Spatial Data. In *Proceedings of the 23rd ACM SIGSPATIAL International Conference on Advances in Geographic Information Systems*. https://doi.org/10.1145/2820783.2820860

4. Boyagoda, L. S., & Nawarathna, L. S. (2022). Analysis and Prediction of Severity of United States Countrywide Car Accidents Based on Machine Learning Techniques. In *2022 7th International Conference on Information Technology Research (ICITR)*, 1–5. https://doi.org/10.1109/ICITR57877.2022.9993371

5. Ennaji, F. Z., Knouzi, M., El Kabtane, H., & Elalaoui Elabdallaoui, H. (2023). Accident Severity Prediction using Machine Learning: A Case Study on the US Accidents Dataset. In *2023 17th International Conference on Signal-Image Technology & Internet-Based Systems (SITIS)*, 242–246. https://doi.org/10.1109/SITIS61268.2023.00044

6. US-Accidents Dataset. Kaggle: sobhanmoosavi/us-accidents.

7. U.S. Census Bureau. TIGER/Line Shapefiles.

8. Apache Spark Documentation.

9. Apache Sedona Documentation.