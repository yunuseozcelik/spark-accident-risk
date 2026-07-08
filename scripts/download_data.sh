#!/usr/bin/env bash
# US-Accidents + TIGER/Line sınır verilerini indirir.
# Önkoşul: kaggle CLI ve ~/.kaggle/kaggle.json (bkz. data/README.md)
set -euo pipefail
cd "$(dirname "$0")/.."

echo "[1/2] US-Accidents (Kaggle)..."
kaggle datasets download -d sobhanmoosavi/us-accidents -p data/raw/
unzip -o data/raw/us-accidents.zip -d data/raw/

echo "[2/2] TIGER/Line eyalet + ilçe sınırları..."
curl -L -o data/boundaries/tl_2023_us_state.zip  https://www2.census.gov/geo/tiger/TIGER2023/STATE/tl_2023_us_state.zip
curl -L -o data/boundaries/tl_2023_us_county.zip https://www2.census.gov/geo/tiger/TIGER2023/COUNTY/tl_2023_us_county.zip
unzip -o data/boundaries/tl_2023_us_state.zip  -d data/boundaries/state/
unzip -o data/boundaries/tl_2023_us_county.zip -d data/boundaries/county/

echo "Tamamlandı. data/raw/ ve data/boundaries/ hazır."
