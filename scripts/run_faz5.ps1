# Faz 5 modelleme pipeline'ını çalıştırır (JAVA_HOME'u JDK 17'ye sabitler).
#
# Varsayılan: sentetik veri üretip modelleri onun üzerinde eğitir (smoke test).
# Gerçek veri için: -InputPath data/processed/accidents_features.parquet
#
#   powershell -ExecutionPolicy Bypass -File scripts/run_faz5.ps1
#   powershell -File scripts/run_faz5.ps1 -InputPath data/processed/accidents_features.parquet

param(
  [string]$InputPath = "data/processed/synthetic_features.parquet",
  [int]$Rows = 20000
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Spark 3.5 için JDK 17
$jdk17 = "C:\Users\$env:USERNAME\Java\jdk-17"
if (Test-Path $jdk17) {
  $env:JAVA_HOME = $jdk17
  $env:PATH = "$jdk17\bin;$env:PATH"
  Write-Host "JAVA_HOME = $jdk17" -ForegroundColor Cyan
} else {
  Write-Host "UYARI: Özel JDK 17 yolu bulunamadı, sistem Java'sı kullanılıyor." -ForegroundColor Yellow
}

$py = ".\.venv\Scripts\python.exe"

if ($InputPath -like "*synthetic*") {
  Write-Host "== Sentetik veri üretiliyor ($Rows satır) ==" -ForegroundColor Cyan
  & $py scripts/make_synthetic_features.py --rows $Rows
}

Write-Host "== Modeller eğitiliyor ==" -ForegroundColor Cyan
& $py src/models/train_models.py --input $InputPath