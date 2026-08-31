Hybrid Big Data Pipeline

مشروع Hybrid Big Data Pipeline لمعالجة ملفات الطلبات الكبيرة التي تحتوي على بيانات ذات جودة مختلفة، باستخدام Python Batch و Apache Spark (PySpark) و MongoDB.

يعتمد المشروع على مفهوم ELT، ويشمل Data Quality و Quarantine و Upsert و Idempotency، بالإضافة إلى تسجيل Metrics ونتائج التشغيل.

============================================================
1. فكرة المشروع
============================================================

المشروع يستقبل ملف بيانات بصيغة CSV يحتوي على سجلات طلبات قد تحتوي على أخطاء أو بيانات غير مكتملة.

يقوم النظام تلقائيًا بفحص حجم الملف، ثم يختار محرك المعالجة المناسب:

- إذا كان حجم الملف أصغر من أو يساوي 200 MB يتم استخدام Python Batch.
- إذا كان حجم الملف أكبر من 200 MB يتم استخدام PySpark.

المعمارية:

CSV File
   |
   v
File Router
   |
   +----------------------+
   |                      |
   v                      v
Python Batch           PySpark
   |                      |
   +----------+-----------+
              |
              v
          orders_raw
              |
              v
      Cleaning + Validation
              |
        +-----+-----+
        |           |
        v           v
      Valid       Quarantine
        |
        v
orders_validated
        |
        v
     Metrics

يتم الاحتفاظ بالبيانات الأصلية في Raw Layer قبل إجراء أي تنظيف أو تعديل عليها، حتى يمكن تتبع البيانات ومراجعتها وإعادة معالجتها عند الحاجة.

============================================================
2. التقنيات المستخدمة
============================================================

- Python
- Apache Spark / PySpark
- MongoDB
- PyMongo
- MongoDB Spark Connector
- pytest
- python-dotenv
- python-dateutil

============================================================
3. متطلبات تشغيل المشروع
============================================================

قبل تشغيل المشروع يجب توفر:

- Python 3.10 أو أحدث.
- Java 17 أو أحدث لتشغيل PySpark.
- MongoDB يعمل على الجهاز أو باستخدام MongoDB URI مخصص.
- مساحة تخزين كافية للبيانات وملفات Spark المؤقتة.

الإعداد الافتراضي لـ MongoDB:

mongodb://localhost:27017

اسم قاعدة البيانات الافتراضي:

midterm_data_pipeline

============================================================
4. تثبيت المشروع
============================================================

Windows:

python -m venv .venv

.\.venv\Scripts\Activate.ps1

إذا منع PowerShell عملية التفعيل:

Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned

ثم:

.\.venv\Scripts\Activate.ps1

تثبيت المكتبات:

python -m pip install -r requirements.txt

Linux / macOS:

python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt

============================================================
5. إعداد Configuration
============================================================

الإعدادات الرئيسية موجودة في:

config/settings.py

ومن أهم الإعدادات:

SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 1000
MONGO_BATCH_SIZE = 500
SPARK_MASTER = local[*]
SPARK_SHUFFLE_PARTITIONS = 8
TARGET_CURRENCY = YER

يمكن أيضًا تحديد إعدادات MongoDB باستخدام Environment Variables:

MONGODB_URI
DB_NAME
COLLECTION_RAW
COLLECTION_VALIDATED
COLLECTION_QUARANTINE

============================================================
6. إنشاء Small Sample
============================================================

python -m src.create_small_sample --input data\orders_huge_mixed_quality.csv --output data\orders_sample_500.csv --rows 500 --seed 42

يمكن تغيير عدد السجلات باستخدام --rows، ويضمن --seed 42 إمكانية إعادة إنشاء نفس العينة.

============================================================
7. تشغيل المشروع
============================================================

تشغيل ملف صغير:

python -m src.main --input data\orders_sample.csv

تشغيل الملف الكبير:

python -m src.main --input data\orders_huge_mixed_quality.csv

يقوم File Router تلقائيًا بتحديد المحرك المناسب حسب حجم الملف.

============================================================
8. اختبار PySpark باستخدام ملف صغير
============================================================

python -m src.main --input data\orders_sample.csv --threshold-mb 1

============================================================
9. التحكم في Batch Size
============================================================

python -m src.main --input data\orders_sample.csv --batch-size 500

============================================================
10. مراحل معالجة البيانات
============================================================

المرحلة الأولى: File Discovery

يقوم النظام بـ:
1. قراءة مسار الملف.
2. حساب حجم الملف.
3. إنشاء id_run خاص بالتشغيل.
4. تحديد محرك المعالجة.

المرحلة الثانية: File Router

إذا كان:

File Size <= 200 MB

يتم اختيار:

Python Batch

أما إذا كان:

File Size > 200 MB

يتم اختيار:

PySpark

المرحلة الثالثة: Python Batch

عند اختيار Python Batch يتم استخدام csv.DictReader لقراءة الملف بطريقة Streaming، ولا يتم تحميل الملف كاملًا إلى الذاكرة.

المرحلة الرابعة: PySpark

عند اختيار PySpark يتم استخدام SparkSession و DataFrame API و Explicit Schema و MongoDB Spark Connector.

============================================================
11. Raw Layer
============================================================

قبل تنفيذ أي عملية تنظيف، يتم تخزين البيانات الأصلية في:

orders_raw

يتم الاحتفاظ بمعلومات مثل:

id_run
file_source
number_row_source
at_ingested
engine_used
record_raw

الهدف هو الحفاظ على نسخة من البيانات الأصلية قبل إجراء أي تعديل، وهذا يوفر Traceability و Auditability وإمكانية إعادة المعالجة.

============================================================
12. Data Quality
============================================================

يطبق المشروع مجموعة من قواعد التنظيف والتصحيح، منها:

- توحيد الأرقام العربية والفارسية.
- توحيد Currency.
- تنظيف الأرقام والفواصل.
- تحويل بعض القيم النصية المعروفة للأسعار.
- تنظيف أرقام الهواتف.
- تصحيح بعض أخطاء البريد الإلكتروني.
- توحيد التواريخ.
- إزالة المسافات والرموز غير الضرورية.
- توحيد بعض النصوص.
- إعادة حساب إجمالي الطلب عند الحاجة.

============================================================
13. Cleaning و Correction
============================================================

عند اكتشاف خطأ يمكن تصحيحه بطريقة مؤكدة، يتم تطبيق التصحيح.

ولا يتم التصحيح إذا كان يحتاج إلى تخمين.

يتم تسجيل عمليات التصحيح في:

corrections

ومن المعلومات المسجلة:

field
original_value
corrected_value
rule_code

وهذا يسمى Audit Trail.

============================================================
14. Classification
============================================================

يتم تصنيف كل سجل إلى:

valid
corrected
quarantine

Valid:
السجل صحيح ولا يحتاج إلى تعديل.

Corrected:
السجل يحتوي على خطأ يمكن تصحيحه بطريقة آمنة ومحددة.

Quarantine:
السجل يحتوي على مشكلة لا يمكن تصحيحها بشكل آمن دون التخمين.

============================================================
15. Quarantine
============================================================

السجلات التي لا يمكن تصحيحها يتم نقلها إلى:

orders_quarantine

ومن أمثلة الأسباب:

MISSING_ORDER_ID
MISSING_CUSTOMER_ID
INVALID_IMPOSSIBLE_DATE
CORRUPTED_ITEMS_JSON
EMPTY_ITEMS
UNKNOWN_PRICE
AMBIGUOUS_NEGATIVE_VALUE
DUPLICATE_ORDER_ID
MULTIPLE_CONFLICTING_ERRORS
EMAIL_UNFIXABLE

يتم الاحتفاظ بالبيانات الأصلية مع تفاصيل الخطأ بدل حذفها.

============================================================
16. MongoDB Collections
============================================================

orders_raw:
تحتوي على البيانات الخام قبل التنظيف.

orders_validated:
تحتوي على السجلات الصحيحة والسجلات التي تم تصحيحها بشكل آمن.

يستخدم المشروع order_id كـ Stable Business Key.

كما يتم إنشاء uniq_order_id كـ Unique Index لمنع تكرار نفس الطلب.

orders_quarantine:
تحتوي على السجلات التي لم يتمكن النظام من تصحيحها بأمان.

============================================================
17. Upsert
============================================================

يستخدم المشروع Upsert.

إذا كان السجل غير موجود:

Insert

وإذا كان السجل موجودًا:

Update

وبالتالي لا يتم إنشاء سجل جديد لنفس order_id في كل مرة يتم فيها تشغيل المشروع.

============================================================
18. Idempotency
============================================================

يدعم المشروع Idempotency، أي أن إعادة تشغيل نفس البيانات لا تؤدي إلى إنشاء سجلات مكررة.

يتم التمييز بين:

Inserted
Updated
Unchanged

وفي إعادة التشغيل الموثقة:

count_inserted = 0
count_updated = 0
count_unchanged = 11136

============================================================
19. Update Test
============================================================

تم اختبار تحديث سجل موجود:

count_inserted = 0
count_updated = 1
count_unchanged = 11135
target_matches = 1

============================================================
20. Consistency Check
============================================================

المعادلة المستخدمة:

read_rows = valid + corrected + quarantine

في التشغيل الكبير:

30,000,000
=
23,513,283
+
2,866,644
+
3,620,073

وكانت النتيجة:

raw_equals_valid_plus_corrected_plus_quarantine = true

EXIT_CODE = 0

============================================================
21. Metrics
============================================================

يقوم المشروع بتسجيل:

- اسم الملف.
- حجم الملف.
- المحرك المستخدم.
- id_run.
- عدد السجلات المقروءة.
- عدد سجلات Raw.
- عدد Valid.
- عدد Corrected.
- عدد Quarantine.
- زمن التنفيذ.
- Throughput.
- Batch Size.
- Spark Partitions.
- عدد Inserted.
- عدد Updated.
- عدد Unchanged.
- إحصائيات الأخطاء.
- نتيجة Consistency Check.

النتائج:

reports/results.json
reports/timing_run.json

============================================================
22. نتائج التشغيل الكبير
============================================================

file_size_mb = 12650.32
used_engine = pyspark
read_rows = 30000000
loaded_raw = 30000000
count_valid = 23513283
count_corrected = 2866644
count_quarantine = 3620073
seconds_elapsed = 27569.953
throughput_rows_per_sec = 1088.14

============================================================
23. مقارنة Python Batch و PySpark
============================================================

Metric              Python Batch       PySpark
------------------------------------------------
Records              12,008             12,008
Time (seconds)       9.7663             60.3969
Records/second       1,229.53           198.82
Partitions            N/A                2
Valid                 8,830              9,293
Corrected             2,306              1,193
Quarantine             872               1,522

قد يكون PySpark أبطأ في الملفات الصغيرة بسبب الوقت اللازم لتشغيل Spark وJVM، بينما الهدف من PySpark هو المعالجة الموزعة والتوسع مع البيانات الكبيرة.

============================================================
24. Tests
============================================================

لتشغيل الاختبارات:

python -m pytest -q

النتيجة الموثقة:

25 passed

============================================================
25. Project Structure
============================================================

midterm-data-pipeline-main/
|
├── README.md
├── requirements.txt
├── .gitignore
|
├── config/
│   ├── __init__.py
│   └── settings.py
|
├── data/
│   └── .gitkeep
|
├── src/
│   ├── __init__.py
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── quality_rules.py
│   ├── elt_pipeline.py
│   ├── incremental_loader.py
│   ├── mongo_setup.py
│   └── metrics.py
|
├── tests/
│   ├── test_cleaning_rules.py
│   └── test_classification.py
|
├── reports/
│   ├── results.json
│   ├── timing_run.json
│   ├── compare_python.json
│   ├── compare_spark.json
│   └── screenshots/
|
└── docs/
    └── architecture.md

============================================================
26. وظيفة الملفات الرئيسية
============================================================

main.py
نقطة التشغيل الرئيسية وتنظيم مراحل المشروع.

file_router.py
اختيار Python أو Spark حسب حجم الملف.

batch_loader.py
تنفيذ Python Batch.

spark_loader.py
تنفيذ PySpark.

quality_rules.py
قواعد Data Quality والتنظيف والتصنيف.

elt_pipeline.py
تنفيذ مراحل Raw و Validated و Quarantine.

incremental_loader.py
دعم المعالجة Incremental.

mongo_setup.py
إعداد MongoDB و Collections و Indexes.

metrics.py
تسجيل Metrics ونتائج التشغيل.

create_small_sample.py
إنشاء Small Sample قابلة لإعادة الإنتاج.

============================================================
27. Resource Cleanup
============================================================

يستخدم المشروع try/finally لضمان إغلاق الموارد بعد انتهاء التنفيذ.

يتم إيقاف Spark باستخدام:

spark_session.stop()

كما يتم إغلاق اتصال MongoDB بعد انتهاء المعالجة.

============================================================
28. Design Decisions
============================================================

لماذا Python Batch؟
لأن الملفات الصغيرة يمكن معالجتها بكفاءة باستخدام Streaming دون الحاجة إلى تشغيل Spark.

لماذا PySpark؟
لأن الملفات الكبيرة تحتاج إلى معالجة قابلة للتوسع والاستفادة من المعالجة الموزعة.

لماذا Raw قبل Cleaning؟
للحفاظ على البيانات الأصلية وإمكانية تتبعها ومراجعتها وإعادة معالجتها.

لماذا Quarantine؟
لمنع حذف البيانات التي تحتوي على أخطاء لا يمكن تصحيحها بشكل آمن.

لماذا Upsert؟
لإضافة السجلات الجديدة وتحديث السجلات الموجودة دون إنشاء نسخ مكررة.

لماذا Unique Index؟
لضمان عدم وجود أكثر من سجل بنفس order_id داخل orders_validated.

============================================================
29. ملاحظات مهمة
============================================================

- قيمة 200 MB هي قيمة قابلة للتعديل لاختيار محرك المعالجة.
- المشروع يقوم بتوحيد العملة المستهدفة إلى YER ولا يقوم بعملية Foreign Exchange Conversion.
- قد يكون PySpark أبطأ من Python Batch في الملفات الصغيرة بسبب وقت تشغيل Spark.
- يمكن أن تحتوي orders_raw على بيانات من أكثر من Run، لذلك يستخدم id_run عند الحاجة لتحديد تشغيل معين.
- لا يتم تطبيق Correction إلا عندما يكون التصحيح واضحًا وقابلًا للتحديد بدون تخمين.

============================================================
30. أدلة المشروع
============================================================

يتضمن المشروع نتائج وأدلة خاصة بـ:

- تشغيل Python Batch.
- تشغيل PySpark.
- MongoDB Collections.
- Data Quality.
- Quarantine.
- Metrics.
- Upsert.
- Idempotency.
- Update Test.
- مقارنة Python Batch و PySpark.
- Consistency Check.

أثناء العرض العملي يمكن توضيح:

1. قرار File Router.
2. حجم الملف.
3. المحرك الذي تم اختياره.
4. orders_raw.
5. السجلات الصحيحة والمصححة.
6. orders_quarantine.
7. MongoDB.
8. Metrics.
9. Idempotency.
10. Update Test.
11. Consistency Check.

============================================================
31. تشغيل سريع
============================================================

بعد تشغيل MongoDB وتفعيل البيئة الافتراضية:

.\.venv\Scripts\Activate.ps1

ثم:

python -m pip install -r requirements.txt

ثم تشغيل العينة:

python -m src.main --input data\orders_sample.csv

ولتوليد عينة جديدة:

python -m src.create_small_sample --input data\orders_huge_mixed_quality.csv --output data\orders_sample_500.csv --rows 500 --seed 42

ولتجربة PySpark:

python -m src.main --input data\orders_sample.csv --threshold-mb 1

وللتأكد من الاختبارات:

python -m pytest -q

============================================================
32. Project Summary
============================================================

هذا المشروع يقدم Hybrid Data Pipeline يجمع بين:

Python Batch
+
PySpark
+
MongoDB
+
ELT
+
Data Quality
+
Quarantine
+
Upsert
+
Idempotency
+
Metrics

والهدف هو بناء Pipeline قادرة على معالجة البيانات الصغيرة والكبيرة، والحفاظ على البيانات الخام، واكتشاف الأخطاء وتصحيح ما يمكن تصحيحه، وعزل البيانات غير القابلة للتصحيح، ومنع التكرار أثناء إعادة التشغيل.
