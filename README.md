# 🚀 Hybrid ELT Big Data Pipeline

## مشروع البيانات الضخمة — المشروع النصفي والمرحلة النهائية

---

### 👩‍💻 إعداد وتطوير

**شيماء نعمان القحطاني**

### 🎓 التخصص والمستوى

**ذكاء اصطناعي — مستوى رابع**

### 🏫 الجامعة

**جامعة الرازي**

### 📚 المقرر

**مادة البيانات الضخمة (Big Data)**

### 👨‍🏫 المهندس

**عمر أبو سند**

---

# 📌 نبذة عن المشروع

مشروع **Hybrid ELT Big Data Pipeline** هو نظام لمعالجة وتحليل بيانات الطلبات باستخدام تقنيات البيانات الضخمة وقواعد البيانات.

يستكمل المشروع مرحلتين رئيسيتين:

- **المشروع النصفي (Midterm Phase)**
- **المرحلة النهائية (Final Phase)**

يعتمد المشروع على بنية هجينة لاختيار طريقة المعالجة المناسبة للبيانات، باستخدام **Python Batch** للملفات الصغيرة و **Apache PySpark** للملفات الكبيرة، مع استخدام **MongoDB** لتخزين البيانات ومعالجتها.

---

# 🎯 أهداف المشروع

يهدف المشروع إلى:

- بناء Hybrid Data Pipeline لمعالجة بيانات الطلبات.
- التعامل مع الملفات الصغيرة والكبيرة بطريقة مناسبة.
- استخدام Python Batch لمعالجة البيانات على دفعات.
- استخدام PySpark لمعالجة البيانات الكبيرة.
- تخزين البيانات الخام في MongoDB.
- تطبيق عمليات تنظيف والتحقق من جودة البيانات.
- عزل السجلات غير الصالحة في Quarantine.
- دعم Upsert وIdempotency.
- إنشاء تقارير ومقاييس لعمليات المعالجة.
- تنفيذ Queries وتحسينها باستخدام Indexes.
- قياس أداء الاستعلامات باستخدام `executionStats`.
- إنشاء Aggregation Reports.
- إنشاء Materialized Views.
- دعم Incremental Refresh.
- تنفيذ Scheduled Jobs.
- توفير Unified REST API باستخدام FastAPI وSwagger.

---

# 🏗️ معمارية المشروع

```text
                    Input CSV File
                          │
                          ▼
                 File Discovery
                          │
                          ▼
                    Hybrid Router
                     /          \
                    /            \
                   ▼              ▼
            Python Batch       PySpark
                   \              /
                    \            /
                     ▼          ▼
                     Raw Data
                         │
                         ▼
                    orders_raw
                         │
                         ▼
                Cleaning & Validation
                         │
                  ┌──────┴──────┐
                  │             │
                  ▼             ▼
          orders_validated   Quarantine
                  │
                  ▼
            Final Phase
                  │
       ┌──────────┼───────────┐
       ▼          ▼           ▼
    Queries   Aggregations  Materialized
    Indexes                  Views
       │          │           │
       └──────────┼───────────┘
                  ▼
             Scheduled Jobs
                  │
                  ▼
              FastAPI
                  │
                  ▼
               Swagger
```

---

# 🧩 تقنيات المشروع

| التقنية | الاستخدام |
|---|---|
| Python | معالجة البيانات والـPipeline |
| Apache PySpark | معالجة الملفات الكبيرة |
| MongoDB | تخزين البيانات |
| PyMongo | الاتصال بـMongoDB |
| FastAPI | بناء واجهة API |
| Uvicorn | تشغيل FastAPI |
| MongoDB Aggregation | التقارير والتحليلات |
| JSON | تخزين الحالة والمقاييس |
| GitHub | إدارة ومشاركة المشروع |

---

# 📦 المشروع النصفي — Midterm Phase

## 1. Hybrid Router

يقوم النظام بتحديد طريقة المعالجة بناءً على حجم ملف الإدخال:

```text
Small File
     ↓
Python Batch
```

أو:

```text
Large File
     ↓
PySpark
```

وبذلك يتم استخدام الأداة المناسبة حسب حجم البيانات.

---

## 2. Python Batch

يتم التعامل مع الملفات الصغيرة باستخدام Python Batch.

بدل تحميل الملف كاملًا في الذاكرة، تتم معالجة البيانات على دفعات.

```text
CSV
 ↓
Read Batch
 ↓
Process
 ↓
Validate
 ↓
MongoDB
```

---

## 3. PySpark

يستخدم المشروع Apache PySpark لمعالجة الملفات الكبيرة.

```text
Large CSV
    ↓
SparkSession
    ↓
DataFrame
    ↓
Processing
    ↓
MongoDB
```

---

# 🗄️ قاعدة البيانات

قاعدة البيانات المستخدمة في المشروع:

```text
midterm_data_pipeline_bigdata
```

ومن أهم الـCollections:

```text
orders_raw
orders_validated
quarantine_orders
```

وفي المرحلة النهائية تضاف:

```text
daily_sales_summary
top_products_summary
job_runs
```

---

# 🔄 ELT Pipeline

يعتمد المشروع على أسلوب ELT:

```text
Input
  ↓
Raw Load
  ↓
orders_raw
  ↓
Cleaning
  ↓
Validation
  ↓
orders_validated
```

أما السجلات التي لا تجتاز قواعد الجودة فتذهب إلى:

```text
quarantine_orders
```

---

# 🧹 Data Quality

يطبق المشروع مجموعة من قواعد التحقق والتنظيف على بيانات الطلبات.

من أمثلة مشاكل البيانات:

```text
ID_ORDER_MISSING
JSON_ITEMS_CORRUPTED
EMAIL_INVALID
PHONE_INVALID
NEGATIVE_VALUE
```

ويتم التعامل مع السجلات حسب حالة كل سجل:

```text
Valid
Corrected
Quarantine
```

---

# 🔐 Upsert & Idempotency

يستخدم المشروع:

```text
order_id
```

كمفتاح أعمال للسجل.

ويتم استخدامه لدعم:

- Upsert
- Idempotency
- منع التكرار عند إعادة تشغيل نفس البيانات.

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

# 🛑 Quarantine

السجلات التي لا يمكن اعتمادها بعد التحقق يتم عزلها في:

```text
quarantine_orders
```

مع الاحتفاظ بسبب العزل.

---

# 📊 Metrics

يسجل المشروع مقاييس التشغيل، مثل:

```text
Total Records
Validated Records
Corrected Records
Quarantine Records
Processing Time
Batch Information
```

وتحفظ نتائج التشغيل في ملفات التقارير الخاصة بالمشروع.

---

# 🏁 تشغيل المشروع النصفي

بعد تفعيل البيئة الافتراضية وتثبيت المتطلبات:

```bash
python -m src.main --input data/orders_sample.csv
```

---

# ⭐ المرحلة النهائية — Final Phase

تستكمل المرحلة النهائية المشروع النصفي بإضافة طبقة التحليل والأداء والتشغيل.

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

# 🔎 Queries

تم تنفيذ خمسة استعلامات عملية:

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

# 📌 Indexes

تم إنشاء فهارس لتحسين أداء الاستعلامات، ومنها:

```text
idx_city_order_date
idx_status_order_date
idx_customer_id
```

ويتضمن المشروع Compound Index:

```text
idx_city_order_date
```

إنشاء أو التحقق من الفهارس:

```text
POST /indexes
```

---

# ⚡ Explain — executionStats

يتم استخدام:

```text
explain("executionStats")
```

لقياس أداء الاستعلامات.

ومن أهم المؤشرات:

```text
nReturned
executionTimeMillis
totalKeysExamined
totalDocsExamined
winningPlan
```

وذلك للمقارنة بين أداء الاستعلامات قبل وبعد إنشاء الفهارس.

---

# 📈 Aggregation Reports

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

---

# 📊 Materialized Views

تم إنشاء ملخصين ماديين:

```text
daily_sales_summary
top_products_summary
```

وهي ملخصات تحليلية وليست نسخة من بيانات الطلبات الأصلية.

المصدر الأساسي للملخصات:

```text
orders_validated
```

---

# 🔄 Incremental Refresh

يدعم المشروع التحديث التزايدي للـMaterialized Views.

يتم استخراج الطلبات المتأثرة من:

```text
incremental_state.json
```

ثم الحصول على:

```text
order_ids
```

الخاصة بآخر Delta Run.

بعد ذلك يتم تمريرها إلى:

```python
refresh(
    db,
    mode="incremental",
    order_ids=order_ids
)
```

ثم يتم تحديد المجموعات المتأثرة:

```text
affected_days
affected_skus
```

وتحديث الأجزاء المتأثرة فقط.

---

# ⏰ Scheduled Jobs

تم تنفيذ مهمتين مجدولتين:

```text
refresh_daily_sales
refresh_top_products
```

الإعداد الافتراضي:

```text
refresh_daily_sales  → 00:10 UTC
refresh_top_products → 00:20 UTC
```

ويتم استخدام:

```text
SimpleDailyScheduler
```

---

# 📝 Job Logging

يتم تسجيل عمليات الـJobs في:

```text
job_runs
```

ويتم حفظ:

```text
job_name
started_at
finished_at
status
result
```

وعند حدوث خطأ:

```text
error
```

حالات التنفيذ:

```text
running
success
failed
```

---

# 🌐 FastAPI

تم إنشاء Unified FastAPI لتوفير وظائف المشروع من خلال REST API.

## Endpoints

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

---

# 📖 Swagger

تشغيل الـAPI:

```bash
python -m src.run_api
```

يستخدم التطبيق:

```text
src.final_api:app
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

# 📥 Ingest عبر API

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

ويستخدم `/ingest` نفس Pipeline المشروع النصفي.

---

# 📁 هيكل المشروع

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
├── data/
│   └── input files
│
├── reports/
│   └── results.json
│
├── requirements.txt
├── example.env
└── README.md
```

---

# ▶️ طريقة تشغيل المشروع

## 1. إنشاء البيئة

```bash
python -m venv .venv
```

## 2. تفعيل البيئة — Windows

```bash
.venv\Scripts\activate
```

## 3. تثبيت المتطلبات

```bash
pip install -r requirements.txt
```

## 4. إعداد MongoDB

تأكد من تشغيل MongoDB وإعداد ملف `.env`.

## 5. تشغيل Pipeline

```bash
python -m src.main --input data/orders_sample.csv
```

## 6. تشغيل FastAPI

```bash
python -m src.run_api
```

## 7. فتح Swagger

```text
http://127.0.0.1:8000/docs
```

---

# 🧪 اختبار وظائف المرحلة النهائية

من Swagger يمكن اختبار:

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

---

# 🔗 العلاقة بين المشروع النصفي والنهائي

المرحلة النهائية لا تستبدل المشروع النصفي، وإنما تبني عليه:

```text
                 MIDTERM
                    │
                    ▼
              Hybrid ELT
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
     Python Batch          PySpark
          │                   │
          └─────────┬─────────┘
                    ▼
               orders_raw
                    │
                    ▼
          Cleaning / Validation
                    │
          ┌─────────┴─────────┐
          ▼                   ▼
orders_validated       quarantine_orders
          │
          ▼
                 FINAL PHASE
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
    Queries     Aggregations   Views
       │            │            │
       ▼            ▼            ▼
    Indexes      Reports     Incremental
       │                         │
       └────────────┬────────────┘
                    ▼
             Scheduled Jobs
                    │
                    ▼
                 FastAPI
                    │
                    ▼
                 Swagger
```

---

# 🎓 معلومات المشروع

| البيان | التفاصيل |
|---|---|
| 👩‍💻 إعداد وتطوير | **شيماء نعمان القحطاني** |
| 🎓 التخصص | **ذكاء اصطناعي** |
| 📚 المستوى | **الرابع** |
| 🏫 الجامعة | **جامعة الرازي** |
| 📖 المادة | **البيانات الضخمة (Big Data)** |
| 👨‍🏫 المهندس | **عمر أبو سند** |
| 📌 المشروع | **Hybrid ELT Big Data Pipeline** |

---

# 👩‍💻 إعداد وتطوير

## شيماء نعمان القحطاني

**ذكاء اصطناعي — مستوى رابع**  
**جامعة الرازي**  
**مادة البيانات الضخمة**  
**المهندس: عمر أبو سند**

---

## © Hybrid ELT Big Data Pipeline

**Midterm + Final Phase**
