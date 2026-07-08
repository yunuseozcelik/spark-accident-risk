# Yol Haritası — BİL 401 Proje

Proje: **Apache Spark + Apache Sedona ile Trafik Kazası Şiddeti Analizi ve Mekânsal Risk Haritalama**

## Ders Kontrol Noktaları (uzak.etu.edu.tr)

| Kontrol noktası | Tarih | Teslim edilecekler | Durum |
|---|---|---|---|
| Hafta 4 | 16.06.2026 | Proje tanımı paragrafı, veri kaynağı, veri açıklaması, platform/araç listesi | ✅ Öneri raporu teslim edildi |
| Hafta 7 | TBD (~Temmuz ortası) | Status report: veri toplama durumu, platform kurulumu, demo run'lar + **paper taslağı** (abstract, related work, proposed implementation) | 🔜 Yaklaşıyor |
| Final | TBD | Tüm kod + veri kaynağı + paper; **20 dk demo günü sunumu** | ⏳ |

> ⚠️ Bugün itibarıyla (08.07.2026) Hafta 7 teslimi yakın. Öncelik: ortam kurulumu + veri
> indirme + en az bir demo run + paper taslak bölümleri.

---

## Faz 1 — Ortam ve Veri (ACİL, Hafta 7 öncesi)

- [ ] Python venv + `requirements.txt` kurulumu; Java 11/17 doğrulaması (`java -version`)
- [ ] PySpark "hello world" — local modda SparkSession açılıp basit bir DataFrame işlemi
- [ ] Apache Sedona kurulumu ve `ST_Contains` ile mini smoke test
- [ ] Kaggle'dan US-Accidents CSV indirme (`scripts/download_data.sh` — kaggle CLI)
- [ ] TIGER/Line eyalet + ilçe shapefile indirme (gerekirse GeoJSON'a dönüştürme)
- [ ] CSV → Parquet dönüşümü (`src/ingestion/`) — şema kontrolü, tip dönüşümleri
- [ ] İlk EDA notebook'u: satır sayısı, null oranları, Severity dağılımı, tarih aralığı

## Faz 2 — Hafta 7 Teslimi

- [ ] Status report yazımı: veri durumu, kurulum notları/sorunları, demo run çıktıları
- [ ] Paper taslağı (`report/`): **Abstract**, **Related Work** (Moosavi vd. arXiv:1906.05409
      + 2-3 ek çalışma), **Proposed Implementation** (pipeline mimarisi)
- [ ] uzak.etu.edu.tr üzerinden yükleme

## Faz 3 — Veri Hattı ve Öznitelikler

- [ ] Temizleme (`src/preprocessing/`): null analizi, aykırı koordinat/süre filtreleri,
      kategorik normalizasyon (Weather_Condition gruplama)
- [ ] Öznitelik üretimi (`src/features/`): saat, gün, ay, hafta sonu, rush hour,
      kaza süresi, hava durumu grupları
- [ ] Zaman/hava/yol bazlı Spark toplulaştırmaları → `outputs/metrics/`
- [ ] Parquet partitioning (örn. State/yıl bazlı) + caching stratejisi

## Faz 4 — Mekânsal Analiz (Sedona)

- [ ] Kaza noktaları × eyalet/ilçe poligonları nokta-poligon join
- [ ] İlçe bazlı kaza yoğunluğu ve yüksek şiddet oranı metrikleri
- [ ] Folium / Kepler.gl risk haritası → `outputs/maps/`

## Faz 5 — Modelleme (MLlib)

- [ ] Pipeline: StringIndexer → OneHotEncoder → VectorAssembler
- [ ] Çok sınıflı görev (Severity 1-4): Multinomial LR + RandomForest
- [ ] İkili yüksek-risk görevi (Severity ≥ 3): LR + RF + GBTClassifier
- [ ] Sınıf dengesizliği: class weighting / örnekleme / eşik analizi
- [ ] Metrikler: accuracy, weighted F1, ROC-AUC, PR-AUC, yüksek-risk recall,
      confusion matrix; feature importance yorumu
- [ ] Model karşılaştırma tablosu → `outputs/metrics/`

## Faz 6 — Rapor ve Demo

- [ ] Paper'ın tamamlanması: methodology, results, discussion, conclusion
- [ ] Tüm grafiklerin/haritaların son hali (`outputs/`)
- [ ] Demo senaryosu: seçili eyalet/yıl üzerinde pipeline'ı baştan sona çalıştırma
      (ingestion → Sedona join → model tahmini → risk haritası), 20 dk'ya prova
- [ ] Final paket: kod + veri kaynağı bağlantıları + paper → uzak.etu.edu.tr

---

## Riskler ve Önlemler (öneri raporundan)

| Risk | Önlem |
|---|---|
| 3 GB CSV yerelde ağır çalışır | Önce Parquet dönüşümü; gerekirse eyalet/yıl örnekleme + partitioning |
| Sedona kurulum / shapefile sorunu | Sınırları GeoJSON/Parquet'e dönüştürüp Sedona join |
| Severity sınıf dengesizliği | Class weighting, binary high-risk etiketleme, sınıf bazlı metrikler |
| Model performansı sınırlı kalır | Katkı accuracy'ye değil; dağıtık pipeline + mekânsal analiz + yorumlanabilirliğe dayanır |

## İş Bölümü (öneriden)

| Dönem | Ana iş |
|---|---|
| 1. Hafta | Ortam kurulumu, veri indirme, CSV→Parquet, ilk EDA (ortak) |
| 2. Hafta | Temizleme + öznitelik + toplulaştırmalar (veri hattı ağırlıklı) |
| 3. Hafta | Sedona join, risk metrikleri, harita (mekânsal ağırlıklı) |
| 4. Hafta | MLlib eğitim, metrikler, rapor + sunum (modelleme/rapor ağırlıklı) |
