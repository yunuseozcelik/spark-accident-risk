# Veri Klasörü

Buradaki dosyalar **gitignore'dadır**; her ekip üyesi veriyi kendisi indirir.

## 1. US-Accidents (ana veri seti)

Kaggle hesabı + API token gerekir (`~/.kaggle/kaggle.json`):

```bash
kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw/
unzip data/raw/us-accidents.zip -d data/raw/
```

Beklenen dosya: `data/raw/US_Accidents_March23.csv` (~3 GB, ~7,7M satır, 46 sütun)

## 2. TIGER/Line sınır verileri

- Eyaletler: https://www2.census.gov/geo/tiger/TIGER2023/STATE/tl_2023_us_state.zip
- İlçeler:   https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip

```bash
curl -L -o data/boundaries/tl_2023_us_state.zip  https://www2.census.gov/geo/tiger/TIGER2023/STATE/tl_2023_us_state.zip
curl -L -o data/boundaries/tl_2023_us_county.zip https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip
unzip -o data/boundaries/tl_2023_us_state.zip  -d data/boundaries/state/
unzip -o data/boundaries/tl_2023_us_county.zip -d data/boundaries/county/
```

## 3. Katmanlar

| Klasör | İçerik |
|---|---|
| `raw/` | İndirilen ham CSV/zip |
| `processed/` | CSV→Parquet dönüşümü ve temizlenmiş ara tablolar |
| `boundaries/` | Eyalet/ilçe shapefile veya GeoJSON |
