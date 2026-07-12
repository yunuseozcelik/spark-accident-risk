# BİL 401 — Hafta 7 Status Report

**Proje:** Apache Spark ve Apache Sedona ile Trafik Kazası Şiddeti Analizi ve Mekânsal Risk Haritalama
**Ekip:** Yunus Emre Özçelik (221401001), Ali Kağan Güven (221401002)
**Repo:** https://github.com/yunuseozcelik/spark-accident-risk
**Tarih:** 13.07.2026

## 1. Veri Toplama Durumu — TAMAMLANDI

- **US-Accidents (2016–2023):** Kaggle API ile indirildi; `US_Accidents_March23.csv`,
  2.9 GB, ~7,7M kayıt, 46 sütun.
- **US Census TIGER/Line 2023:** Eyalet (15 MB) ve ilçe (126 MB) sınır
  shapefile'ları indirildi.
- Ham CSV, Spark ile okunup **eyalet bazlı partition'lanmış Parquet** katmanına
  dönüştürüldü (`Severity` int, zaman sütunları timestamp, sayısal sütunlar double);
  2,9 GB CSV → **645 MB Parquet**.
- İlk EDA (tam veri, `outputs/metrics/initial_eda.json`):
  - **7.728.394 kayıt**, 46 sütun; Ocak 2016 – Nisan 2023.
  - **Severity dağılımı belirgin dengesiz:** 1: %0,9 · 2: %79,7 · 3: %16,8 · 4: %2,6 —
    öneri raporundaki binary high-risk (Severity ≥ 3) görevi ve class weighting
    planını doğruluyor.
  - Null oranları: konum/zaman/Severity %0; hava sütunları ~%2; `Wind_Speed` %7,4;
    `Precipitation` **%28,5** (temizleme aşamasında özel strateji gerekecek).
  - En yoğun eyaletler: CA (1,74M), FL (880K), TX (583K), SC (383K), NY (348K).

## 2. Platform ve Sistem Durumu

| Bileşen | Sürüm | Durum |
|---|---|---|
| Apache Spark (PySpark) | 3.5.8 | ✅ Kuruldu, smoke test geçti |
| Apache Sedona | 1.9.0 | ✅ Kuruldu; SC × ilçe nokta-poligon join demosu çalıştı (bkz. §4) |
| OpenJDK | 17.0.19 | ✅ |
| Python | 3.10 | ✅ venv + requirements.txt |
| Donanım | 6 çekirdek ARM64, 7.4 GB RAM | Yerel local[4] mod |

Spark, yerel kısıtlara göre `driver.memory=3g`, `shuffle.partitions=24` ile
yapılandırıldı; veri eyalet bazlı partition'landığı için analizler seçili
eyaletlerle sınırlandırılabiliyor.

## 3. Kurulum Sorunları ve Edinilen Know-How

- **Sedona jar sürüm uyumu:** PyPI'daki `apache-sedona` 1.9.0 ile Maven'daki
  `sedona-spark-shaded-3.5_2.12:1.9.0` + `geotools-wrapper:1.7.1-28.5`
  eşleştirilmesi gerekti; sürüm uyumsuzluğu class-not-found hatalarına yol açıyor.
- **Sedona shapefile okuyucusu** geotools sürüm uyumsuzluğu nedeniyle
  (`ClassNotFoundException: org.geotools.api...`) güvenilir çalışmadı. Öneri
  raporundaki plan-B uygulandı: sınırlar geopandas ile WKT sütunlu Parquet'e
  dönüştürülüp (`scripts/convert_boundaries.py`, NAD83→WGS84) Spark tarafında
  `ST_GeomFromWKT` ile join'lendi — geotools bağımlılığı tamamen kalktı.
- **Bellek kısıtı:** 7.4 GB RAM'de 2.9 GB CSV işlerken Parquet'e erken dönüşüm ve
  eyalet bazlı partitioning kritik; shuffle partition sayısı düşürüldü.
- **keplergl** ARM64 ortamında sorunlu olduğundan harita görselleştirmesi için
  Folium'a karar verildi.
- **Karışık zaman formatı:** Ham CSV'de zaman sütunları kesirli saniyeli ve
  saniyesiz karışık formatta geliyor; `inferSchema` yerine açık şema + `to_timestamp`
  ile tip dönüşümü yapıldı — hem 2,9 GB CSV üzerinde ikinci bir tam okuma geçişi
  önlendi hem format kaynaklı null'lar oluşmadı.

## 4. Demo Çalıştırmalar

- ✅ PySpark smoke test: SparkSession + groupBy aggregation (`scripts/smoke_test.py`).
- ✅ CSV → Parquet dönüşümü: tam veri seti (7,7M satır), 2,9 GB → 645 MB,
  eyalet bazlı partition (`src/ingestion/csv_to_parquet.py`).
- ✅ İlk EDA: satır sayısı, Severity dağılımı, null oranları, en yoğun 10 eyalet
  → `outputs/metrics/initial_eda.json` (`src/ingestion/initial_eda.py`).
- ✅ Sedona nokta-poligon join demosu: SC kazaları (382K) × ABD ilçe poligonları
  (3.235), `ST_Contains` ile ilçe bazlı kaza sayısı + yüksek şiddet oranı
  (`src/spatial/sedona_join_demo.py` → `outputs/metrics/county_risk_demo.csv`).
  46 SC ilçesinin tamamı eşleşti; en yoğun ilçe Greenville (57K kaza), en yüksek
  high-risk oranı Charleston (%38,2). **Veri kalitesi bulgusu:** SC etiketli 82
  kayıt koordinat olarak komşu eyaletlere düşüyor — temizleme aşamasında
  mekânsal doğrulama adımı eklenecek.

## 5. Paper Taslağı

Ekte: abstract, related work ve proposed implementation bölümlerini içeren taslak
(`report/paper_draft.md`).

## 6. Sonraki Adımlar

Temizleme + öznitelik üretimi (Faz 3), Sedona bölgesel risk metrikleri (Faz 4),
MLlib model eğitimi ve karşılaştırması (Faz 5) — bkz. `docs/ROADMAP.md`.
