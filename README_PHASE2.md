# Big Data – Phase 2 additions

هذه المرحلة تكمل مشروع النصفي داخل نفس المشروع. لا يتم إنشاء Dataset جديد ولا استبدال `orders_raw`.
مصدر البيانات هو الـ30 مليون سجل الموجود أصلًا في MongoDB، وتبقى Pipeline النصفي كما هي.

## 1. Queries + Indexes + Explain

```powershell
python -m src.phase2_runner indexes
python -m src.phase2_runner explain
python -m src.phase2_runner query sales_by_city
python -m src.phase2_runner query delivered_by_date
python -m src.phase2_runner query customer_orders
python -m src.phase2_runner query high_value_orders
python -m src.phase2_runner query pending_card_orders
```

الفهارس الجديدة:
- `idx_city_order_date` (Compound)
- `idx_status_order_date`
- `idx_customer_id`

`explain` يقيس 3 استعلامات قبل إنشاء فهارس المرحلة الثانية وبعدها باستخدام `executionStats`.

## 2. Aggregation Reports

```powershell
python -m src.phase2_runner aggregation sales_by_city
python -m src.phase2_runner aggregation top_products
python -m src.phase2_runner aggregation top_customers
python -m src.phase2_runner aggregation sales_by_period
python -m src.phase2_runner aggregation orders_by_status
```

## 3. Materialized Views

الـViews هي Collections ملخصة وليست نسخة من الـ30 مليون:
- `daily_sales_summary`
- `top_products_summary`

أول إنشاء فقط:

```powershell
python -m src.phase2_runner refresh-mv --mode full
```

بعد وجود الـViews، التحديث الافتراضي Incremental:

```powershell
python -m src.phase2_runner refresh-mv --mode incremental
```

التحديث التزايدي يعتمد على آخر `run_id` موجود في `orders_raw` ثم يعيد حساب المجموعات المتأثرة فقط.

## 4. Scheduled Jobs

```powershell
python -m src.phase2_runner job refresh_daily_sales
python -m src.phase2_runner job refresh_top_products
```

الـAPI تشغّل scheduler بسيطًا بدون مكتبة خارجية، وفق UTC:
- `refresh_daily_sales` → 00:10
- `refresh_top_products` → 00:20

وكل تنفيذ يسجل البداية والنهاية والحالة في `job_runs`.

## 5. Unified FastAPI

```powershell
uvicorn src.final_api:app --host 0.0.0.0 --port 8000
```

Swagger:
`http://127.0.0.1:8000/docs`

Endpoints المطلوبة:
- `GET /health`
- `POST /ingest`
- `POST /indexes`
- `GET /queries`
- `GET /queries/{name}`
- `GET /aggregations`
- `GET /aggregations/{name}`
- `POST /refresh-mv`
- `GET /jobs`
- `POST /jobs/{name}/run`

`POST /ingest` يستخدم Router وBatch/PySpark loaders الموجودين أصلًا في مشروع النصفي، وليس Pipeline جديدًا.

## ملاحظة مهمة

ملف `config/settings.py` الموجود في مشروعك الأصلي يبقى مصدر إعدادات MongoDB والـcollections. لا نضع بيانات اتصال حساسة داخل الكود الجديد.
