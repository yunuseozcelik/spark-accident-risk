# BİL 401 — Proje Ara Raporu

| Bilgi | Açıklama |
|---|---|
| **Proje** | Apache Spark ve Apache Sedona ile Trafik Kazası Şiddeti Analizi ve Mekânsal Risk Haritalama |
| **Ekip** | Yunus Emre Özçelik (221401001), Ali Kağan Güven (221401002) |
| **Ders** | BİL 401 — Büyük Veri ve Dağıtık Veri İşleme |
| **Tarih** | 13.07.2026 |
| **GitHub** | https://github.com/yunuseozcelik/spark-accident-risk |

## 1. Projenin Mevcut Durumu

Bu projede US-Accidents veri seti üzerinde trafik kazalarının trafik akışına etkisini ifade eden `Severity` seviyesinin zamansal, meteorolojik, yol ve konum özellikleriyle ilişkisi incelenmektedir. Projenin temel amacı yalnızca bir tahmin modeli geliştirmek değil; büyük ölçekli verinin Apache Spark ile alınması, dönüştürülmesi, temizlenmesi, zenginleştirilmesi, Apache Sedona ile mekânsal olarak birleştirilmesi ve bölgesel risk çıktılarının oluşturulmasını kapsayan uçtan uca bir veri hattı geliştirmektir.

Veri setindeki `Severity` alanı yaralanma veya ölüm ciddiyetini değil, kazanın trafik akışı üzerindeki etkisini ve oluşturduğu gecikme seviyesini göstermektedir. Bu nedenle proje kapsamında kullanılan “şiddet” ve “yüksek risk” ifadeleri trafik etkisi bağlamında yorumlanmaktadır.

## 2. Veri Toplama ve Hazırlama Durumu

US-Accidents March 2023 sürümü Kaggle üzerinden indirilmiştir. Ham veri yaklaşık 2,9 GB büyüklüğünde bir CSV dosyasıdır ve 7.728.394 kayıt ile 46 sütun içermektedir. Veri setinde gözlemlenen zaman damgaları 2016–2023 dönemini kapsamaktadır.

Ham CSV dosyası Apache Spark kullanılarak açık bir şema ile okunmuş, tarih ve sayısal alanların veri tipleri dönüştürülmüş ve veri `State` sütununa göre partition edilmiş Parquet formatında kaydedilmiştir. Bu işlem sonucunda yaklaşık 2,9 GB olan CSV verisi yaklaşık 645 MB büyüklüğünde bir Parquet katmanına dönüştürülmüştür.

İlk keşifsel veri analizi sonucunda Severity sınıflarının belirgin biçimde dengesiz olduğu görülmüştür:

- Severity 1: yaklaşık %0,9
- Severity 2: yaklaşık %79,7
- Severity 3: yaklaşık %16,8
- Severity 4: yaklaşık %2,6

Bu dağılım, modelleme aşamasında yalnızca accuracy metriğinin yeterli olmayacağını ve sınıf ağırlıkları, örnekleme, eşik analizi ve sınıf bazlı metriklerin kullanılmasının gerekli olduğunu göstermektedir.

Temizleme aşamasında zorunlu alanlar, koordinatlar ve kaza süreleri kontrol edilmiştir. Başlangıçtaki 7.728.394 kayıttan geçersiz kaza süresine sahip 34.981 kayıt çıkarılmış ve 7.693.413 kayıt temizlenmiş veri setine aktarılmıştır. `Weather_Condition` değerleri Clear, Cloudy, Rain, Fog/Low Visibility, Snow/Ice, Thunderstorm, Windy/Dust, Unknown ve Other grupları altında normalize edilmiştir.

## 3. Platform ve Sistem Kurulumu

Projede aşağıdaki teknolojiler kullanılmaktadır:

| Bileşen | Sürüm / Yapılandırma | Durum |
|---|---|---|
| Python | 3.10 | Kuruldu |
| OpenJDK | 17 | Kuruldu |
| Apache Spark / PySpark | 3.5.8 | Smoke test başarılı |
| Apache Sedona | 1.9.0 | Mekânsal SQL testi başarılı |
| Depolama | CSV ve partition edilmiş Parquet | Çalışıyor |
| Mekânsal veri | Census TIGER/Line eyalet ve ilçe sınırları | Hazır |
| Görselleştirme | Folium | İki risk haritası üretildi |
| Geliştirme ortamları | Linux/ARM64 ve Windows 11 | İki ortamda doğrulandı |

Spark yerel modda çalıştırılmakta; bellek ve işlemci kısıtlarına uygun olarak `local[4]`, 3 GB driver memory ve 24 shuffle partition kullanılmaktadır. Veri eyalet bazında partition edildiği için gerekli durumlarda seçili eyalet veya yıllar üzerinde çalışma yapılabilmektedir.

## 4. Gerçekleştirilen Veri Hattı ve Demo Çalışmaları

Şu ana kadar aşağıdaki aşamalar tamamlanmıştır:

1. Ham US-Accidents CSV verisinin Spark ile okunması.
2. Şema ve veri tipi dönüşümlerinin uygulanması.
3. CSV verisinin eyalet bazlı Parquet formatına dönüştürülmesi.
4. Null değer, koordinat ve kaza süresi kontrollerinin yapılması.
5. Saat, gün, ay, hafta sonu, yoğun saat ve kaza süresi özelliklerinin üretilmesi.
6. Hava koşullarının daha genel kategorilere ayrılması.
7. `Severity >= 3` kayıtları için binary `high_risk` etiketinin oluşturulması.
8. Saat, gün, ay, yıl, hava durumu ve yol özelliklerine göre Spark toplulaştırmalarının üretilmesi.
9. Kaza koordinatlarının Apache Sedona geometrik noktalarına dönüştürülmesi.
10. Kaza noktalarının Census ilçe poligonlarıyla `ST_Contains` kullanılarak birleştirilmesi.
11. İlçe bazında toplam kaza sayısı, ortalama Severity ve yüksek risk oranlarının hesaplanması.
12. İlçe bazlı kaza yoğunluğu ve yüksek risk oranı için iki etkileşimli Folium haritasının üretilmesi.

PySpark smoke testinde küçük bir DataFrame üzerinde `groupBy` ve ortalama hesaplama işlemleri başarıyla tamamlanmıştır. Sedona smoke testinde ise `ST_Point` ve `ST_AsText` fonksiyonları kullanılarak geometrik nokta üretimi doğrulanmıştır.

İlk Sedona demosunda South Carolina kazaları ABD ilçe poligonlarıyla birleştirilmiş ve eyaletteki 46 ilçenin tamamı eşleştirilmiştir. Daha sonra mekânsal analiz tam veri üzerinde çalıştırılmış, ilçe risk metrikleri ve eyalet etiketi ile koordinattan bulunan eyalet arasındaki uyuşmazlıklar hesaplanmıştır. Toplam 1.609 kayıtta eyalet uyuşmazlığı belirlenmiş; bunların önemli bölümünün eyalet sınırlarına yakın koordinatlardan kaynaklandığı değerlendirilmiştir.

## 5. Karşılaşılan Problemler ve Çözümler

### Karışık zaman biçimleri

Ham CSV dosyasında kesirli saniyeli ve saniyesiz farklı zaman formatları bulunmaktadır. `inferSchema` ile ikinci bir tam veri okuması yapmak yerine açık şema ve `to_timestamp` dönüşümleri kullanılmıştır.

### Sedona ve GeoTools uyumluluğu

Sedona’nın doğrudan shapefile okuma yöntemi GeoTools sürüm uyuşmazlığı nedeniyle güvenilir çalışmamıştır. Alternatif çözüm olarak sınır dosyaları GeoPandas ile WKT içeren Parquet formatına dönüştürülmüş ve Spark içinde `ST_GeomFromWKT` kullanılarak okunmuştur.

### Yerel bellek kısıtı

Yaklaşık 3 GB büyüklüğündeki CSV’nin sınırlı RAM üzerinde işlenmesi için erken Parquet dönüşümü, eyalet bazında partitioning ve düşük shuffle partition sayısı kullanılmıştır.

### Windows Python worker problemi

Windows ortamında Spark worker süreçleri aktif sanal ortamın Python yorumlayıcısını otomatik olarak bulamamıştır. `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` ve ilgili Spark yapılandırmaları aktif `sys.executable` yoluna ayarlanarak problem çözülmüştür.

### Windows Sedona JAR yükleme problemi

Windows ortamında Maven üzerinden indirilen Sedona JAR dosyaları `HADOOP_HOME / winutils.exe` hatasına yol açmıştır. Windows için gerekli JAR dosyaları PySpark’ın yerel `jars` klasöründen yüklenmiş, Linux/ARM64 ortamında ise Maven yöntemi korunmuştur. Böylece aynı SparkSession modülü farklı işletim sistemlerinde çalışabilir hâle getirilmiştir.

## 6. Güncel İlerleme ve Sonraki Adımlar

### Tamamlanan aşamalar

- Veri setlerinin indirilmesi
- Spark ve Sedona ortamlarının kurulması
- CSV → Parquet dönüşümü
- İlk keşifsel veri analizi
- Temizleme ve kategorik normalizasyon
- Öznitelik üretimi
- Zaman, hava ve yol bazlı toplulaştırmalar
- Sedona nokta-poligon birleştirmesi
- İlçe risk metrikleri
- Folium risk haritaları
- Paper taslağının Abstract, Related Work ve Proposed Implementation bölümleri

### Sonraki aşamalar

- Spark MLlib veri hazırlama pipeline’ının kurulması
- Çok sınıflı Logistic Regression ve Random Forest modellerinin eğitilmesi
- Binary high-risk Logistic Regression, Random Forest ve GBT modellerinin eğitilmesi
- Sınıf dengesizliği stratejilerinin uygulanması
- Accuracy, weighted F1, ROC-AUC, PR-AUC ve high-risk recall metriklerinin hesaplanması
- Confusion matrix ve feature importance çıktılarının oluşturulması
- Final paper’ın Results, Discussion ve Conclusion bölümlerinin tamamlanması
- 20 dakikalık uçtan uca demo senaryosunun hazırlanması

## 7. İş Bölümü

Yunus Emre Özçelik; veri indirme, CSV–Parquet dönüşümü, veri hattı, ilk EDA, temizleme, öznitelik üretimi ve mekânsal analiz çalışmalarında görev almıştır.

Ali Kağan Güven; literatür taraması, paper taslağının akademik bölümleri, Windows ortamının kurulması, Spark/Sedona platform uyumluluğu ve ara rapor kontrol çalışmalarında görev almıştır.

Mekânsal analiz doğrulaması, rapor incelemesi, sonraki modelleme aşaması ve final demo hazırlığı ekip tarafından ortak yürütülecektir.