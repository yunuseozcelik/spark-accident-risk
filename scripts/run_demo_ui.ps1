# Demo günü: Spark Web UI'ı canlı göstermek için tek komut.
#
#   powershell -ExecutionPolicy Bypass -File scripts/run_demo_ui.ps1
#
# JAVA_HOME'u JDK 17'ye sabitler, gerçek öznitelik tablosu üzerinde üç Spark işi
# çalıştırır ve Web UI'ı (http://localhost:4040) açık tutar.

param(
  [string]$InputPath = "data/processed/accidents_features.parquet",
  [int]$Hold = 0
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

$jdk17 = "C:\Users\$env:USERNAME\Java\jdk-17"
if (Test-Path $jdk17) {
  $env:JAVA_HOME = $jdk17
  $env:PATH = "$jdk17\bin;$env:PATH"
} else {
  Write-Host "UYARI: JDK 17 bulunamadi, sistem Java'si kullanilacak." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "  SPARK WEB UI DEMO" -ForegroundColor Cyan
Write-Host "  Tarayicida acilacak adres: http://localhost:4040" -ForegroundColor Cyan
Write-Host ""

& ".\.venv\Scripts\python.exe" scripts/demo_spark_ui.py --input $InputPath --hold $Hold
