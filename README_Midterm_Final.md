# Hybrid ELT Big Data Pipeline — Midterm + Final Phase

**Student Name:** شيماء نعمان القحطاني

## وصف المشروع

هذا المشروع هو **Hybrid ELT Big Data Pipeline** لمعالجة بيانات الطلبات باستخدام Python Batch وApache Spark وMongoDB.

يتكون المشروع من مرحلتين:

1. **المشروع النصفي (Midterm Phase):** بناء خط بيانات هجين يستقبل ملفات الطلبات، يختار آلية المعالجة المناسبة حسب حجم الملف، يحفظ البيانات الخام، ينفذ التنظيف والتحقق من الجودة، يعزل السجلات غير الصالحة، ويدعم Upsert وIdempotency وتسجيل المقاييس.
2. **المرحلة النهائية (Final Phase):** استكمال المشروع النصفي بإضافة الاستعلامات والفهارس و`executionStats` وتقارير Aggregation وMaterialized Views والتحديث التزايدي والـScheduled Jobs وواجهة FastAPI موحدة.

> **مهم:** المرحلة النهائية تستمر داخل نفس مشروع النصفي ونفس قاعدة البيانات، ولا تنشئ Dataset جديدًا ولا تستبدل `orders_raw`.

---

# 1. الهدف من المشروع

الهدف هو بناء خط بيانات قادر على التعامل مع بيانات الطلبات بأحجام مختلفة باستخدام مسارين للمعالجة:

```text
Input CSV
   │
   ▼
File Discovery / File Size
   │
   ▼
Hybrid Router
   │
   ├───────────────┐
   │               │
   ▼               ▼
Python Batch    PySpark
   │               │
   └───────┬───────┘
           ▼
       orders_raw
           │
           ▼
    Cleaning / Validation
           │
      ┌────┴─────────┐
      │              │
      ▼              ▼
orders_validated   Quarantine
      │
      ▼
Final Phase
      │
      ├── Queries
      ├── Indexes
      ├── Aggregations
      ├── Materialized Views
      ├── Scheduled Jobs
      └── FastAPI
```

---

# 2. التقنيات المستخدمة

- Python
- Apache Spark / PySpark
- MongoDB
- PyMongo
- FastAPI
- Uvicorn
- Pandas / CSV streaming حسب مسار المعالجة
- JSON
- Threading للـScheduler البسيط
- MongoDB Aggregation Framework

---

# 3. قاعدة البيانات

يستخدم المشروع قاعدة البيانات الأصلية للمشروع:

```text
midterm_data_pipeline_bigdata
```

ومن أهم Collections:

```text
orders_raw
orders_validated
quarantine_orders
daily_sales_summary
top_products_summary
job_runs
```

ولا يتم إنشاء قاعدة بيانات بديلة للمرحلة النهائية.

---

# 4. إعداد البيئة

## إنشاء Virtual Environment

```bash
python -m venv .venv
```

### Windows

```bash
.venv\Scripts\activate
```

### Linux / macOS

```bash
source .venv/bin/activate
```

## تثبيت المتطلبات

```bash
pip install -r requirements.txt
```

---

# 5. إعداد MongoDB

انسخ:

```text
example.env
```

إلى:

```text
.env
```

ثم ضع إعدادات الاتصال المناسبة ببيئة التشغيل.

يجب عدم وضع بيانات اتصال حساسة داخل الكود أو GitHub.

---

# 6. المشروع النصفي — Midterm Phase

## 6.1 Hybrid Router

يقوم المشروع باختيار طريقة المعالجة حسب حجم ملف الإدخال.

المساران هما:

```text
Small File
   ↓
Python Batch
```

و:

```text
Large File
   ↓
PySpark
```

ويتم تحديد القرار من خلال حجم الملف والـthreshold الموجود في إعدادات المشروع.

---

# 7. Python Batch Processing

عند اختيار Python Batch يتم التعامل مع ملف CSV بطريقة streaming/batches بدل تحميل الملف كاملًا في الذاكرة.

المسار العام:

```text
CSV
 ↓
Read Batch
 ↓
Process Rows
 ↓
Cleaning / Validation
 ↓
MongoDB
```

يتم استخدام عمليات batch للإدخال إلى MongoDB وتسجيل المقاييس الخاصة بالتشغيل.

---

# 8. PySpark Processing

عند اختيار PySpark يتم استخدام:

```text
SparkSession
DataFrame API
```

مع مخطط بيانات مناسب للبيانات.

المسار:

```text
Large CSV
   ↓
PySpark
   ↓
DataFrame
   ↓
Cleaning / Validation
   ↓
MongoDB
```

ويتم تسجيل معلومات التشغيل والمقاييس الخاصة بالـSpark processing.

---

# 9. ELT Architecture

المشروع يستخدم أسلوب ELT بحيث يتم الاحتفاظ بالبيانات الخام أولًا قبل تطبيق مراحل التنظيف والتحقق.

المسار:

```text
Input
  ↓
Raw Load
  ↓
orders_raw
  ↓
Cleaning / Validation
  ↓
Validated / Quarantine
```

وهذا يسمح بالحفاظ على البيانات الخام وعدم فقدان السجلات الأصلية أثناء المعالجة.

---

# 10. orders_raw

تمثل:

```text
orders_raw
```

طبقة البيانات الخام.

يتم تحميل السجلات إليها قبل مرحلة التنظيف النهائية.

ولا يتم استبدال البيانات الخام عند إضافة وظائف المرحلة النهائية.

---

# 11. Data Quality & Cleaning

يحتوي المشروع على قواعد تلقائية لفحص جودة البيانات ومعالجة السجلات.

من أمثلة مشاكل الجودة التي يتعامل معها المشروع:

```text
ID_ORDER_MISSING
JSON_ITEMS_CORRUPTED
EMAIL_INVALID
PHONE_INVALID
NEGATIVE_VALUE
```

وتوجد قواعد تنظيف وتصحيح وفحص متعددة، مع عزل السجلات التي لا يمكن اعتمادها.

---

# 12. Corrected Records / Audit

عند إمكانية تصحيح السجل، يتم التعامل معه كسجل مصحح بدل إسقاطه مباشرة.

يتم الاحتفاظ بأثر التصحيح ضمن آلية الـaudit المستخدمة في المشروع، بحيث يمكن تتبع السجلات التي تم تعديلها أثناء التنظيف.

---

# 13. Quarantine

السجلات التي لا تجتاز قواعد الجودة يتم عزلها في:

```text
quarantine_orders
```

مع سبب واضح للحجر.

مثال على السبب:

```text
ID_ORDER_MISSING
EMAIL_INVALID
PHONE_INVALID
JSON_ITEMS_CORRUPTED
NEGATIVE_VALUE
```

وهذا يمنع إدخال البيانات غير الصالحة إلى مجموعة البيانات المعتمدة.

---

# 14. orders_validated

السجلات التي تجتاز عمليات التنظيف والتحقق يتم وضعها في:

```text
orders_validated
```

وهي المصدر الأساسي للتحليلات والـMaterialized Views في المرحلة النهائية.

---

# 15. Business Key

يستخدم المشروع:

```text
order_id
```

كمفتاح أعمال ثابت للسجل.

ويتم استخدامه لدعم:

- Upsert
- Idempotency
- تتبع السجلات المتأثرة في التحديث التزايدي

---

# 16. Upsert

يعتمد الإدخال على Upsert باستخدام مفتاح الأعمال بدل إنشاء نسخة جديدة من السجل في كل إعادة تشغيل.

الفكرة:

```text
order_id موجود
      ↓
Update

order_id غير موجود
      ↓
Insert
```

---

# 17. Idempotency

تم تصميم Pipeline بحيث إعادة تشغيل نفس البيانات لا تؤدي إلى إنشاء سجلات مكررة عند استخدام نفس `order_id`.

وبذلك يمكن إعادة تشغيل المعالجة بأمان دون مضاعفة السجلات المعتمدة.

---

# 18. Metrics

يتم تسجيل مقاييس التشغيل ونتائج المعالجة.

ومن المقاييس التي يمكن تتبعها:

```text
total records
validated records
corrected records
quarantine records
processing time
batch information
```

ويتم الاحتفاظ بنتائج التشغيل في ملفات وتقارير المشروع مثل:

```text
reports/results.json
```

---

# 19. Consistency

يتم التحقق من اتساق نتائج التشغيل من خلال العلاقة بين السجلات المعالجة وحالاتها المختلفة.

الفكرة:

```text
Total Records
=
Validated
+
Corrected / processed
+
Quarantine
```

وفق تعريف المقاييس المستخدم في تنفيذ المشروع.

---

# 20. التشغيل من CLI

تشغيل الـPipeline:

```bash
python -m src.main --input data/orders_sample.csv
```

ويستخدم هذا نفس الـRouter والـBatch/PySpark loaders الموجودة في المشروع.

---

# 21. المرحلة النهائية — Final Phase

تضيف المرحلة النهائية وظائف التحليل والأداء والتشغيل فوق المشروع النصفي.

وتشمل:

```text
Queries
Indexes
Explain
Aggregations
Materialized Views
Incremental Refresh
Scheduled Jobs
FastAPI
Swagger
```

---

# 22. Practical Queries

تم تنفيذ خمسة استعلامات مستقلة:

```text
sales_by_city
delivered_by_date
customer_orders
high_value_orders
pending_card_orders
```

عرض الاستعلامات:

```text
GET /queries
```

تشغيل استعلام:

```text
GET /queries/{name}
```

---

# 23. Indexes

تم إنشاء الفهارس المطلوبة للمرحلة النهائية.

من الفهارس المستخدمة:

```text
idx_city_order_date
idx_status_order_date
idx_customer_id
```

ومن ضمنها:

```text
idx_city_order_date
```

وهو Compound Index.

كما توجد فهارس مساعدة لبعض الاستعلامات.

إنشاء أو التحقق من الفهارس من خلال:

```text
POST /indexes
```

---

# 24. Explain — executionStats

تم استخدام:

```text
explain("executionStats")
```

لمقارنة أداء الاستعلامات قبل وبعد إنشاء الفهارس.

ومن مؤشرات القياس:

```text
nReturned
executionTimeMillis
totalKeysExamined
totalDocsExamined
winningPlan
```

وتستخدم المقارنة لإظهار تأثير الفهارس على تنفيذ الاستعلامات.

---

# 25. Aggregation Reports

تم تنفيذ خمسة تقارير Aggregation:

```text
sales_by_city
top_products
top_customers
sales_by_period
orders_by_status
```

عرض التقارير:

```text
GET /aggregations
```

تشغيل تقرير:

```text
GET /aggregations/{name}
```

مثال:

```text
GET /aggregations/sales_by_city
```

---

# 26. Materialized Views

تم تنفيذ ملخصين ماديين:

```text
daily_sales_summary
top_products_summary
```

هذه الـCollections هي ملخصات تحليلية وليست نسخة من الـ30 مليون سجل.

مصدر البيانات هو:

```text
orders_validated
```

ولا يتم الاعتماد على:

```text
orders_raw
```

كمصدر مباشر للـMaterialized Views.

---

# 27. Initial Full Refresh

يتم إنشاء الـMaterialized Views لأول مرة باستخدام:

```python
full_refresh(db)
```

ويتم بناء الملخصات من البيانات الموجودة في:

```text
orders_validated
```

ويكون هذا هو الـFull Refresh الأولي.

---

# 28. Incremental Refresh

بعد إنشاء الـViews، يتم استخدام:

```python
incremental_refresh(
    db,
    order_ids=order_ids
)
```

بدل إعادة بناء كل البيانات في كل مرة.

يتم الحصول على آخر Delta order IDs من:

```text
incremental_state.json
```

من خلال:

```python
_latest_delta_order_ids()
```

ثم يمررها الـJob إلى:

```python
refresh(
    db,
    mode="incremental",
    order_ids=order_ids
)
```

---

# 29. آلية Incremental Materialized Views

يتم تحديد الطلبات المتأثرة أولًا:

```text
order_ids
      ↓
orders_validated
      ↓
affected_days
affected_skus
```

ثم:

```text
affected_days
      ↓
daily_sales_summary
```

و:

```text
affected_skus
      ↓
top_products_summary
```

وبذلك لا يتم إعادة بناء جميع الـMaterialized Views في كل تشغيل.

إذا لم يتم توفير `order_ids`، يرجع التحديث التزايدي بدون تنفيذ Full Scan كبير للبيانات.

---

# 30. Scheduled Jobs

تم تنفيذ مهمتين:

```text
refresh_daily_sales
refresh_top_products
```

الإعداد الافتراضي:

```text
refresh_daily_sales  → 00:10 UTC
refresh_top_products → 00:20 UTC
```

ويتم تشغيلهما من خلال:

```text
SimpleDailyScheduler
```

---

# 31. Manual Jobs

يمكن تشغيل الـJob يدويًا أيضًا من خلال:

```text
POST /jobs/{name}/run
```

أو من خلال:

```text
src/final_jobs.py
```

أسماء الـJobs:

```text
refresh_daily_sales
refresh_top_products
```

---

# 32. Job Execution Logging

كل Job يتم تسجيله في:

```text
job_runs
```

ويتم تسجيل:

```text
job_name
started_at
finished_at
status
result
```

وعند الفشل:

```text
error
```

الحالات:

```text
running
success
failed
```

---

# 33. Job Flow

التدفق الكامل للتحديث التزايدي:

```text
Delta Ingestion
      ↓
incremental_state.json
      ↓
order_ids
      ↓
_latest_delta_order_ids()
      ↓
final_jobs.py
      ↓
incremental_refresh()
      ↓
affected_days / affected_skus
      ↓
Materialized Summaries
```

---

# 34. Unified FastAPI

تشمل واجهة FastAPI:

```text
GET  /health
POST /ingest
POST /indexes

GET  /queries
GET  /queries/{name}

GET  /aggregations
GET  /aggregations/{name}

POST /refresh-mv

GET  /jobs
POST /jobs/{name}/run
```

ويتم توفير Swagger تلقائيًا.

---

# 35. تشغيل FastAPI

يتم تشغيل الـAPI باستخدام:

```bash
python -m src.run_api
```

ويستخدم:

```text
src.final_api:app
```

والـAPI يعمل افتراضيًا على:

```text
http://127.0.0.1:8000
```

Swagger:

```text
http://127.0.0.1:8000/docs
```

Health:

```text
http://127.0.0.1:8000/health
```

---

# 36. Ingest عبر API

يمكن تشغيل الـPipeline من خلال:

```text
POST /ingest
```

مثال:

```bash
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d "{\"input\":\"data/orders_sample.csv\",\"stage\":\"normal\"}"
```

ويستخدم `/ingest` نفس Pipeline المشروع النصفي، وليس Pipeline جديدًا.

---

# 37. ملفات المشروع المهمة

من ملفات المرحلة النهائية:

```text
src/
├── final_api.py
├── final_jobs.py
├── final_materialized_views.py
├── incremental_loader.py
├── run_api.py
├── main.py
└── ...

config/
└── settings.py

reports/
└── ...

requirements.txt
example.env
README.md
```

وتبقى ملفات المشروع النصفي الأخرى موجودة وتعمل ضمن البنية الأصلية للمشروع.

---

# 38. تشغيل المشروع كاملًا

## الخطوة 1 — تشغيل MongoDB

تأكد من أن MongoDB يعمل.

## الخطوة 2 — تفعيل البيئة

```bash
.venv\Scripts\activate
```

## الخطوة 3 — تثبيت المتطلبات

```bash
pip install -r requirements.txt
```

## الخطوة 4 — تشغيل Pipeline

```bash
python -m src.main --input data/orders_sample.csv
```

## الخطوة 5 — تشغيل API

```bash
python -m src.run_api
```

## الخطوة 6 — فتح Swagger

```text
http://127.0.0.1:8000/docs
```

---

# 39. اختبار وظائف المشروع

من خلال Swagger يمكن اختبار:

```text
/health
/ingest
/indexes
/queries
/queries/{name}
/aggregations
/aggregations/{name}
/refresh-mv
/jobs
/jobs/{name}/run
```

كما يمكن تشغيل وظائف المشروع النصفي من CLI.

---

# 40. الحفاظ على بيانات المشروع النصفي

المرحلة النهائية لا تنشئ Dataset جديدًا.

تستمر في استخدام:

```text
midterm_data_pipeline_bigdata
```

وتستمر البيانات الأصلية في:

```text
orders_raw
orders_validated
quarantine_orders
```

وتضاف إليها الملخصات والتسجيلات الخاصة بالمرحلة النهائية:

```text
daily_sales_summary
top_products_summary
job_runs
```

---

# 41. عدم إعادة معالجة البيانات الضخمة دون حاجة

تم تصميم التحديث التزايدي بحيث لا يقوم كل Job بإعادة معالجة بيانات `orders_raw` كاملة.

بدل ذلك:

```text
Delta
 ↓
order_ids
 ↓
affected groups
 ↓
incremental refresh
```

وهذا يحافظ على بيانات المشروع النصفي ويقلل إعادة المعالجة غير الضرورية.

---

# 42. Project Structure — Overview

```text
Hybrid ELT Big Data Pipeline
│
├── config/
│   └── settings.py
│
├── src/
│   ├── main.py
│   ├── run_api.py
│   ├── final_api.py
│   ├── final_jobs.py
│   ├── final_materialized_views.py
│   ├── incremental_loader.py
│   └── ...
│
├── reports/
│   └── results.json
│
├── data/
│   └── input files
│
├── requirements.txt
├── example.env
└── README.md
```

---

# 43. ملخص المشروع النصفي

المشروع النصفي يحقق طبقة معالجة البيانات:

```text
File
 ↓
Router
 ↓
Python Batch / PySpark
 ↓
orders_raw
 ↓
Cleaning
 ↓
Validation
 ↓
Audit / Correction
 ↓
orders_validated
       │
       └──→ quarantine_orders
 ↓
Upsert / Idempotency
 ↓
Metrics
```

---

# 44. ملخص المرحلة النهائية

المرحلة النهائية تضيف طبقة التحليل والتشغيل:

```text
orders_validated
       │
       ├── Queries
       ├── Indexes
       ├── Explain
       ├── Aggregations
       │
       └── Materialized Views
                 │
                 └── Incremental Refresh
                         │
                         └── Scheduled Jobs
                                  │
                                  └── FastAPI / Swagger
```

---

# 45. النتيجة النهائية

المشروع النهائي يجمع بين:

- Hybrid Data Processing
- Python Batch
- Apache Spark
- MongoDB
- ELT Architecture
- Data Quality
- Cleaning and Validation
- Quarantine
- Audit
- Upsert
- Idempotency
- Metrics
- Practical Queries
- MongoDB Indexes
- Compound Index
- `executionStats`
- Aggregation Reports
- Materialized Views
- Incremental Refresh
- Scheduled Jobs
- Job Logging
- FastAPI
- Swagger

ويستمر المشروع باستخدام نفس بيانات وبنية المشروع النصفي بدل إنشاء مشروع بيانات منفصل.

---

# 46. معلومات الطالب

**Student Name:** شيماء نعمان القحطاني

**Project:** Hybrid ELT Big Data Pipeline — Midterm + Final Phase
