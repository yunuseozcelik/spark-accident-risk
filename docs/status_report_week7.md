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
  dönüştürüldü (`Severity` int, zaman sütunları timestamp, sayısal sütunlar double).
  *(EDA çıktılarından doldur: toplam satır, tarih aralığı, Severity dağılımı.)*

## 2. Platform ve Sistem Durumu

| Bileşen | Sürüm | Durum |
|---|---|---|
| Apache Spark (PySpark) | 3.5.8 | ✅ Kuruldu, smoke test geçti |
| Apache Sedona | 1.9.0 | ✅ Kuruldu *(join denemesi: durumu güncelle)* |
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
- **Bellek kısıtı:** 7.4 GB RAM'de 2.9 GB CSV işlerken Parquet'e erken dönüşüm ve
  eyalet bazlı partitioning kritik; shuffle partition sayısı düşürüldü.
- **keplergl** ARM64 ortamında sorunlu olduğundan harita görselleştirmesi için
  Folium'a karar verildi.
- *(Parquet dönüşüm süresi, EDA sırasında karşılaşılanlar: güncelle.)*

## 4. Demo Çalıştırmalar

- ✅ PySpark smoke test: SparkSession + groupBy aggregation.
- ✅ CSV → Parquet dönüşümü (tam veri seti). *(süre/boyut ekle)*
- ✅ İlk EDA: satır sayısı, Severity dağılımı, null oranları, en yoğun 10 eyalet
  → `outputs/metrics/initial_eda.json`. *(önemli sayıları buraya yaz)*
- *(Sedona nokta-poligon join denemesi — cuma günü ekle)*

## 5. Paper Taslağı

Ekte: abstract, related work ve proposed implementation bölümlerini içeren taslak
(`report/paper_draft.md`).

## 6. Sonraki Adımlar

Temizleme + öznitelik üretimi (Faz 3), Sedona bölgesel risk metrikleri (Faz 4),
MLlib model eğitimi ve karşılaştırması (Faz 5) — bkz. `docs/ROADMAP.md`.
