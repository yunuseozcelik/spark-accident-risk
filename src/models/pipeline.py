"""Faz 5 — MLlib öznitelik pipeline'ı.

`accidents_features.parquet` şemasından (bkz. `src/features/build_features.py`)
model girdilerini hazırlayan ortak Spark ML aşamalarını üretir:

    Imputer (sayısal hava)  ->  StringIndexer + OneHotEncoder (kategorik)
                            ->  VectorAssembler  ->  "features" vektörü

Bu modül yalnızca aşamaları/kolon listelerini tanımlar; eğitim ve
değerlendirme `src/models/train_models.py` içindedir.
"""
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    Imputer,
    OneHotEncoder,
    StringIndexer,
    VectorAssembler,
)

# --- Kolon grupları (build_features.py çıktısıyla birebir) ---

# Sayısal hava sütunları: temizlemede impute edilmediler -> Imputer (median)
WEATHER_NUM = [
    "Temperature(F)", "Humidity(%)", "Pressure(in)", "Visibility(mi)",
    "Wind_Speed(mph)", "Precipitation(in)",
]

# Yol/POI ikili bayrakları (0/1)
ROAD_FLAGS = [
    "Amenity", "Bump", "Crossing", "Give_Way", "Junction", "No_Exit",
    "Railway", "Roundabout", "Station", "Stop", "Traffic_Calming",
    "Traffic_Signal", "Turning_Loop",
]

# Zaman ve diğer sayısal öznitelikler (year modele alınmaz — dağıtım kayması riski)
TIME_NUM = [
    "hour", "day_of_week", "month",
    "is_weekend", "is_rush_hour", "is_night",
    "duration_min", "Distance(mi)",
]

# One-hot kodlanacak kategorik öznitelik(ler)
CATEGORICAL = ["weather_group"]

# Imputer uygulanacak sayısal sütunlar (median ile doldurulur)
IMPUTE_COLS = WEATHER_NUM

# Assembler'a girecek tüm sayısal sütunlar (impute sonrası) + precip bayrağı
NUMERIC_FEATURES = TIME_NUM + WEATHER_NUM + ["precip_missing"] + ROAD_FLAGS


def build_feature_stages() -> list:
    """Etiketten bağımsız öznitelik dönüşüm aşamalarını döndürür.

    VectorAssembler çıktısı ``features`` kolonudur; her iki görev
    (çok sınıflı Severity ve ikili high_risk) bu aynı öznitelikleri kullanır.
    """
    imputer = Imputer(
        inputCols=IMPUTE_COLS,
        outputCols=IMPUTE_COLS,  # yerinde doldur
        strategy="median",
    )

    indexers = [
        StringIndexer(
            inputCol=c,
            outputCol=f"{c}_idx",
            handleInvalid="keep",  # görülmemiş kategori -> ekstra indeks
        )
        for c in CATEGORICAL
    ]
    encoders = [
        OneHotEncoder(
            inputCol=f"{c}_idx",
            outputCol=f"{c}_ohe",
            handleInvalid="keep",
        )
        for c in CATEGORICAL
    ]

    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + [f"{c}_ohe" for c in CATEGORICAL],
        outputCol="features",
        handleInvalid="keep",
    )

    return [imputer, *indexers, *encoders, assembler]


def build_feature_pipeline() -> Pipeline:
    """Öznitelik aşamalarını tek bir Pipeline nesnesi olarak döndürür."""
    return Pipeline(stages=build_feature_stages())
