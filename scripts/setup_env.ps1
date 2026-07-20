# Windows ortam kurulumu — Faz 5 (ve genel) için tek komutla hazırlık.
#
# Kurar/yapar:
#   1. Python 3.11 sanal ortamı (.venv)
#   2. requirements.txt bağımlılıkları
#   3. Apache Sedona JAR'larını pyspark/jars içine (Windows'ta Maven çözümü
#      güvenilmez olduğundan, bkz. src/spark_session.py)
#   4. JDK 17 kontrolü (Spark 3.5, Java 8/11/17 ister — Java 21 desteklenmez)
#
# Çalıştırma (proje kökünden):
#   powershell -ExecutionPolicy Bypass -File scripts/setup_env.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

Write-Host "== 1. Sanal ortam ==" -ForegroundColor Cyan
if (-not (Test-Path ".venv")) { py -3.11 -m venv .venv }
$py = ".\.venv\Scripts\python.exe"
& $py -m pip install --upgrade pip setuptools wheel

Write-Host "== 2. Bağımlılıklar ==" -ForegroundColor Cyan
& $py -m pip install -r requirements.txt

Write-Host "== 3. Sedona JAR'ları ==" -ForegroundColor Cyan
$jarsDir = (& $py -c "import pyspark,os;print(os.path.join(os.path.dirname(pyspark.__file__),'jars'))").Trim()
$jars = @{
  "sedona-spark-shaded-3.5_2.12-1.9.0.jar" = "https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.9.0/sedona-spark-shaded-3.5_2.12-1.9.0.jar"
  "geotools-wrapper-1.7.1-28.5.jar"        = "https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.7.1-28.5/geotools-wrapper-1.7.1-28.5.jar"
}
foreach ($name in $jars.Keys) {
  $dest = Join-Path $jarsDir $name
  if (Test-Path $dest) { Write-Host "  zaten var: $name" }
  else {
    Write-Host "  indiriliyor: $name"
    Invoke-WebRequest -Uri $jars[$name] -OutFile $dest -UseBasicParsing
  }
}

Write-Host "== 4. winutils (Windows Hadoop) ==" -ForegroundColor Cyan
$hadoopBin = "C:\Users\$env:USERNAME\hadoop\bin"
New-Item -ItemType Directory -Force $hadoopBin | Out-Null
$winBase = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin"
foreach ($f in @("winutils.exe", "hadoop.dll")) {
  $dest = Join-Path $hadoopBin $f
  if (Test-Path $dest) { Write-Host "  zaten var: $f" }
  else {
    Write-Host "  indiriliyor: $f"
    Invoke-WebRequest -Uri "$winBase/$f" -OutFile $dest -UseBasicParsing
  }
}
Write-Host "  HADOOP_HOME = C:\Users\$env:USERNAME\hadoop (spark_session.py otomatik bulur)"

Write-Host "== 5. JDK 17 kontrolü ==" -ForegroundColor Cyan
$jdk17 = "C:\Users\$env:USERNAME\Java\jdk-17"
if (Test-Path $jdk17) {
  Write-Host "  JDK 17 bulundu: $jdk17"
  Write-Host "  Spark için: `$env:JAVA_HOME='$jdk17'  (run_faz5.ps1 bunu otomatik yapar)"
} else {
  Write-Host "  UYARI: JDK 17 ($jdk17) yok. Spark 3.5 Java 21 ile hata verebilir." -ForegroundColor Yellow
  Write-Host "  İndir: https://adoptium.net/temurin/releases/?version=17"
}

Write-Host "`nKurulum tamam. Smoke test: powershell -File scripts/run_faz5.ps1" -ForegroundColor Green
