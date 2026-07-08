# Trafik Kazası Şiddeti Analizi ve Mekânsal Risk Haritalama

**BİL 401 — Big Data and Distributed Data Processing | Analytics (Applied) Track**

Apache Spark ve Apache Sedona ile US-Accidents veri seti üzerinde trafik kazalarının
şiddetini etkileyen zamansal, meteorolojik, yol ve konumsal faktörlerin uçtan uca
dağıtık bir veri hattıyla analizi ve bölgesel risk haritalarının üretilmesi.

## Ekip

| Ad Soyad | Öğrenci No |
|---|---|
| Yunus Emre Özçelik | 221401001 |
| Ali Kağan Güven | 221401002 |

## Veri Setleri

- **US-Accidents (2016–2023)** — ~7,7M kayıt, ~46 sütun, ~3 GB CSV
  [Kaggle](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents) · [Dokümantasyon](https://smoosavi.org/datasets/us_accidents)
- **US Census TIGER/Line** — eyalet/ilçe sınır poligonları (Sedona mekânsal join için)
  [census.gov](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html)

> Veri dosyaları repoya **commit edilmez** (`data/` gitignore'da). İndirme adımları için
> `data/README.md` dosyasına bakın.

## Klasör Düzeni

```
├── data/
│   ├── raw/          # İndirilen ham CSV (gitignore)
│   ├── processed/    # Parquet + temizlenmiş ara katmanlar (gitignore)
│   └── boundaries/   # TIGER/Line shapefile / GeoJSON (gitignore)
├── notebooks/        # EDA ve deneme notebook'ları
├── src/
│   ├── ingestion/    # CSV okuma, şema, CSV -> Parquet
│   ├── preprocessing/# Temizleme, null/aykırı değer işlemleri
│   ├── features/     # Zaman/hava/yol öznitelik üretimi
│   ├── spatial/      # Sedona nokta-poligon join, bölgesel toplulaştırma
│   ├── models/       # MLlib pipeline'ları (LR / RF / GBT)
│   └── viz/          # Grafik ve harita üretimi (Folium/Kepler.gl)
├── outputs/
│   ├── figures/      # Grafikler, confusion matrix
│   ├── maps/         # HTML risk haritaları
│   └── metrics/      # Model karşılaştırma tabloları (csv/json)
├── report/           # Akademik rapor (paper) kaynakları
├── scripts/          # Kurulum ve çalıştırma yardımcı script'leri
└── docs/
    ├── ROADMAP.md    # Yol haritası ve kontrol noktaları
    └── proposal/     # Proje öneri raporu ve ders proje tanımı
```

## Teknoloji Yığını

- **Apache Spark (PySpark)** — Spark SQL, DataFrame API
- **Apache Sedona** — ST_Contains/ST_Within, nokta-poligon join
- **Spark MLlib** — LogisticRegression, RandomForest, GBTClassifier
- **Görselleştirme** — Matplotlib/Seaborn, Folium, Kepler.gl
- **Ortam** — Python 3.x, Java 11/17, yerel Spark (local/standalone)

## Kurulum (özet)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
# Java 11 veya 17 kurulu olmalı (spark önkoşulu)
```

Ayrıntılı yol haritası ve teslim takvimi için: [`docs/ROADMAP.md`](docs/ROADMAP.md)
