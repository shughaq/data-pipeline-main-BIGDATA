# Hybrid ELT Big Data Pipeline — Final Phase

هذا المستودع يستكمل المشروع النصفي ويضيف متطلبات المرحلة الثانية فقط: الاستعلامات والفهارس و`explain("executionStats")`، خمسة Aggregation Reports، عرضين ماديين Materialized Summaries، مهمتين مجدولتين، وواجهة FastAPI موحدة.

## 1. التثبيت

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# Linux/macOS
# source .venv/bin/activate

pip install -r requirements.txt
```

انسخ `example.env` إلى `.env` وعدّل إعدادات MongoDB إذا لزم.

## 2. تشغيل المشروع النصفي

يستخدم `/ingest` نفس بوابة الإدخال الموجودة أصلًا في المشروع النصفي، ولا ينشئ مسار ETL ثاني.

CLI:

```bash
python -m src.main --input data/orders_sample.csv
```

## 3. تشغيل API

```bash
python -m src.run_api
```

Swagger:

- `http://127.0.0.1:8000/docs`

Health:

```bash
curl http://127.0.0.1:8000/health
```

## 4. Ingest عبر API

```bash
curl -X POST http://127.0.0.1:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"input":"data/orders_sample.csv","stage":"normal"}'
```

الـAPI يستدعي `src.pipeline_service.run_pipeline`، وهو نفس الـrouter والـbatch/incremental/Spark pipeline المستخدمة في CLI.

## 5. Queries + Indexes + Explain

يوجد خمسة استعلامات مستقلة:

1. `orders_by_city_status`
2. `customer_orders`
3. `high_value_orders`
4. `sales_by_period`
5. `orders_by_status`

إنشاء الفهارس:

```bash
curl -X POST http://127.0.0.1:8000/indexes
```

الفهارس الجديدة:

- `idx_city_status_date` — Compound Index على `(city, status, order_date)`.
- `idx_customer_date` — `(customer_id, order_date)`.
- `idx_status_date` — `(status, order_date)`.
- `idx_total_amount` — فهرس مساعد للاستعلام عالي القيمة.

تشغيل استعلام:

```text
GET /queries/orders_by_city_status?city=صنعاء&status=تم%20التسليم
```

تشغيل `executionStats`:

```text
GET /queries/orders_by_city_status?city=صنعاء&status=تم%20التسليم&explain=true
```

### مقارنة Before/After

لتنفيذ `explain("executionStats")` لثلاثة استعلامات قبل وبعد الفهارس:

```bash
python scripts_benchmark_indexes.py
```

يُحفظ الناتج في:

`reports/index_explain_comparison.json`

ويحتوي على `nReturned`, `executionTimeMillis`, `totalKeysExamined`, `totalDocsExamined` و`winningPlan` لكل استعلام قبل وبعد.

## 6. Aggregation Reports

خمسة تقارير، وكل واحد له اسم وتشغيل مستقل:

- `sales_by_city` — المبيعات حسب المدينة.
- `top_products` — أفضل المنتجات.
- `top_customers` — أفضل العملاء.
- `sales_by_period` — المبيعات حسب الشهر.
- `orders_by_status` — توزيع الطلبات حسب الحالة.

عرض الأسماء:

```text
GET /aggregations
```

تشغيل تقرير:

```text
GET /aggregations/sales_by_city
```

## 7. Materialized Views

تمت إضافة ملخصين ماديين:

- `daily_sales_summary`
- `top_products_summary`

الـrefresh يستخدم `mv_source_ledger` لتتبع `order_id + record_hash`. عند وصول سجل جديد يُضاف، وعند تغير سجل موجود تُعكس مساهمته القديمة ثم تُضاف المساهمة الجديدة، وعند إعادة نفس السجل بدون تغيير يتم تجاهله. لذلك لا يعاد بناء كل البيانات من الصفر في كل refresh.

تشغيل يدوي:

```text
POST /refresh-mv
```

## 8. Scheduled Jobs

يوجد مهمتان مجدولتان:

1. `refresh_materialized_views` — افتراضيًا يوميًا الساعة 01:00.
2. `daily_reports` — افتراضيًا يوميًا الساعة 02:00.

يمكن تغيير الوقت من `.env`.

عرض المهام وسجل التنفيذ:

```text
GET /jobs
```

تشغيل مهمة يدويًا أثناء المناقشة:

```text
POST /jobs/refresh_materialized_views/run
POST /jobs/daily_reports/run
```

كل تنفيذ يسجل:

- وقت البداية.
- وقت النهاية.
- حالة `success` أو `failed`.
- النتيجة أو رسالة الخطأ.

وسجل التنفيذ محفوظ في collection: `job_runs`.

## 9. الملفات الجديدة للمرحلة النهائية

- `src/final_features.py` — Queries, Indexes, Aggregations, Materialized Views.
- `src/jobs.py` — Scheduled Jobs + Job Logs.
- `src/pipeline_service.py` — بوابة مشتركة للـCLI والـAPI.
- `src/api.py` — FastAPI والـSwagger endpoints.
- `src/run_api.py` — تشغيل API.
- `scripts_benchmark_indexes.py` — Before/After executionStats.
- `config/settings.py` — الإعدادات والـenvironment variables.
- `requirements.txt` — الاعتماديات.
- `example.env` — مثال بدون بيانات حساسة.

## 10. ملاحظة الاختبار

لا يعتمد الكود النهائي على أسماء ملفات التدريب أو عدد سجلات ثابت. الاستعلامات والتجميعات تعمل على البيانات الموجودة في MongoDB، و`/ingest` يستقبل مسار الملف في بيئة التقييم.
