"""Proje genelinde ortak SparkSession kurulumu.

Yerel çalışma ortamları için:
- Driver memory: 3 GB
- Local worker: 4
- Shuffle partition: 24

Python driver ve worker süreçlerinde aktif sanal ortamın
Python yorumlayıcısı kullanılır.
"""

import os
import platform
import sys
from pathlib import Path

from pyspark.sql import SparkSession


SEDONA_PACKAGE = (
    "org.apache.sedona:"
    "sedona-spark-shaded-3.5_2.12:1.9.0"
)

GEOTOOLS_PACKAGE = (
    "org.datasyslab:"
    "geotools-wrapper:1.7.1-28.5"
)

SEDONA_JAR_NAME = (
    "sedona-spark-shaded-3.5_2.12-1.9.0.jar"
)

GEOTOOLS_JAR_NAME = (
    "geotools-wrapper-1.7.1-28.5.jar"
)


def _ensure_supported_java() -> None:
    """Spark 3.5 için desteklenen JDK'yı (8/11/17) JAVA_HOME'a ayarlar.

    Bu makinede varsayılan Java 21 olabilir; Spark 3.5 Java 21'i desteklemez
    ve modül erişim (InaccessibleObject) hataları verebilir. Yerelde kurulu
    bir JDK 17 varsa (setup_env.ps1 ile ~/Java/jdk-17 altına) onu kullanırız.
    JAVA_HOME zaten uygun bir JDK'ya işaret ediyorsa dokunmayız.
    """
    if os.environ.get("SPARK_ACCIDENT_SKIP_JAVA_CHECK"):
        return
    # Bilinen JDK 17 konumlarına öncelik ver (JAVA_HOME Java 21 olabilir),
    # bulunamazsa mevcut JAVA_HOME'a düş.
    candidates = [
        str(Path.home() / "Java" / "jdk-17"),
        r"C:\Program Files\Eclipse Adoptium\jdk-17",
        r"C:\Program Files\Java\jdk-17",
        os.environ.get("JAVA_HOME", ""),
    ]
    for cand in candidates:
        if cand and (Path(cand) / "bin" / ("java.exe" if os.name == "nt" else "java")).is_file():
            os.environ["JAVA_HOME"] = cand
            return
    # Uygun JDK bulunamadı: sessizce sistem java'sına bırak (uyarı ver).
    print(
        "[uyarı] JDK 17 bulunamadı; Spark 3.5 sistem Java'sıyla çalışacak. "
        "Sorun olursa: powershell -File scripts/setup_env.ps1"
    )


def _ensure_hadoop_home() -> None:
    """Windows'ta Spark için HADOOP_HOME + winutils.exe/hadoop.dll ayarlar.

    Spark, Windows'ta yerel dosya sistemi işlemleri için Hadoop'un
    winutils.exe ve hadoop.dll ikililerini gerektirir; yoksa
    'HADOOP_HOME and hadoop.home.dir are unset' hatası verir. Bunlar
    setup_env.ps1 ile ~/hadoop/bin altına indirilir.
    """
    if os.name != "nt":
        return
    existing = os.environ.get("HADOOP_HOME")
    candidates = [existing, str(Path.home() / "hadoop")]
    for cand in candidates:
        if cand and (Path(cand) / "bin" / "winutils.exe").is_file():
            os.environ["HADOOP_HOME"] = cand
            bin_dir = str(Path(cand) / "bin")
            if bin_dir not in os.environ.get("PATH", ""):
                os.environ["PATH"] = bin_dir + os.pathsep + os.environ.get("PATH", "")
            return
    print(
        "[uyarı] winutils.exe bulunamadı; Windows'ta Spark hata verebilir. "
        "Çözüm: powershell -File scripts/setup_env.ps1"
    )


def _check_windows_sedona_jars() -> None:
    """Windows ortamında gerekli Sedona JAR'larını kontrol eder."""

    import pyspark

    jars_directory = (
        Path(pyspark.__file__).resolve().parent / "jars"
    )

    required_jars = [
        jars_directory / SEDONA_JAR_NAME,
        jars_directory / GEOTOOLS_JAR_NAME,
    ]

    missing_jars = [
        jar.name
        for jar in required_jars
        if not jar.is_file()
    ]

    if missing_jars:
        missing_text = ", ".join(missing_jars)

        raise FileNotFoundError(
            "Windows için gerekli Sedona JAR dosyaları "
            f"bulunamadı: {missing_text}. "
            f"Beklenen klasör: {jars_directory}"
        )


def get_spark(
    app_name: str = "spark-accident-risk",
    with_sedona: bool = False,
) -> SparkSession:
    """Ortak SparkSession oluşturur.

    Args:
        app_name: Spark uygulamasının görünen adı.
        with_sedona: Apache Sedona desteğini etkinleştirir.

    Returns:
        Yapılandırılmış SparkSession.
    """

    _ensure_supported_java()
    _ensure_hadoop_home()

    python_executable = sys.executable
    is_windows = platform.system() == "Windows"

    os.environ["PYSPARK_PYTHON"] = python_executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_executable

    if is_windows:
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[4]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.pyspark.python",
            python_executable,
        )
        .config(
            "spark.pyspark.driver.python",
            python_executable,
        )
    )

    if is_windows:
        builder = (
            builder
            .config(
                "spark.driver.host",
                "127.0.0.1",
            )
            .config(
                "spark.driver.bindAddress",
                "127.0.0.1",
            )
        )

    if with_sedona:
        from sedona.spark import SedonaContext

        builder = builder.config(
            "spark.serializer",
            "org.apache.spark.serializer.KryoSerializer",
        )

        if is_windows:
            # Windows'ta JAR dosyaları doğrudan
            # PySpark'ın jars klasöründen yüklenir.
            _check_windows_sedona_jars()
        else:
            # Linux/Jetson ortamında Maven paket yöntemi korunur.
            builder = builder.config(
                "spark.jars.packages",
                f"{SEDONA_PACKAGE},{GEOTOOLS_PACKAGE}",
            )

        return SedonaContext.create(
            builder.getOrCreate()
        )

    return builder.getOrCreate()
