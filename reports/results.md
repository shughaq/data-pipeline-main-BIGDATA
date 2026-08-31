نتائج التشغيل والأدلة التقنية

1. إعدادات المشروع المستخدمة

Plain Text


SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 1000  (في تشغيل العينة الأخير)
MONGO_BATCH_SIZE = 500
SPARK_MASTER = local[2]
SPARK_SHUFFLE_PARTITIONS = 8
SPARK_LOCAL_DIR = D:\spark_temp
COLLECTION_RAW = orders_raw
COLLECTION_VALIDATED = orders_validated
COLLECTION_QUARANTINE = orders_quarantine
RAW_SOURCE_COLLECTION = orders_raw عند تشغيل الاستئناف



2. نتائج تشغيل الملف الكبير — PySpark

المصدر: reports/timing_run.json وtiming_run.log.

JSON


{
  "id_run": "3bb63d5e-e828-46dd-b3d6-c7c668de044f",
  "file_name": "data\\orders_huge_mixed_quality.csv",
  "file_size_mb": 12650.32,
  "used_engine": "pyspark",
  "read_rows": 30000000,
  "loaded_raw": 30000000,
  "count_valid": 23513283,
  "count_corrected": 2866644,
  "count_quarantine": 3620073,
  "seconds_elapsed": 27569.953,
  "throughput_rows_per_sec": 1088.14,
  "partitions": null,
  "size_batch": null,
  "count_inserted": 26379927,
  "count_updated": 0,
  "count_unchanged": 0,
  "consistency_check": {
    "raw_equals_valid_plus_corrected_plus_quarantine": true,
    "loaded_raw": 30000000,
    "expected_from_sum": 30000000
  }
}



توزيع أسباب العزل في التشغيل الكبير:

Plain Text


CORRUPTED_ITEMS_JSON       208978
EMAIL_UNFIXABLE            207761
MULTIPLE_CONFLICTING_ERRORS 228921
MISSING_CUSTOMER_ID        208616
EMPTY_ITEMS                208471
UNKNOWN_PRICE              663395
INVALID_IMPOSSIBLE_DATE    539685
DUPLICATE_ORDER_ID         396641
AMBIGUOUS_NEGATIVE_VALUE   748213
MISSING_ORDER_ID           209392



نهاية timing_run.log:

Plain Text


[main] تم إغلاق SparkSession.
[main] تم إغلاق اتصال MongoDB.
===== ... reports\\timing_run.json =====
END: 2026-08-23T22:39:59.7353911+03:00
EXIT_CODE: 0
POWERSHELL_ELAPSED_SECONDS: 27572.61



3. نتائج تشغيلات Python Batch في reports/results.json

Plain Text


id_run                                   engine       rows     valid  corrected quarantine seconds throughput inserted updated unchanged batch
c8b31653-675a-4720-a3d7-96bbd51e888f     python_batch 12008    8830   2306      872        7.0362  1706.60    11136    0       0         1000
f5fd8e57-a5a7-4d7f-bd17-db4541c39205     python_batch 12008    8830   2306      872        7.3877  1625.40    0        11136   0         1000
280a98b8-1600-48a3-8051-1bc7bacbebd3     python_batch 12008    8830   2306      872       13.6836   877.55    0        11136   0          500
968b1e5d-9d70-4e69-ba58-98ba70dedf38     python_batch 12008    8830   2306      872       10.6886  1123.44    0        11136   0          500
80086ba9-3b0c-453f-b5f9-8c41e4288fe5     python_batch 12008    8830   2306      872        7.4293  1616.30    0            0   11136      500
e9e8da91-e13e-4f0e-9167-aa003ea6dc23     python_batch 12008    8830   2306      872       17.3403   692.49    0            0   11136     1000
c8c34dff-beb5-40b2-b37a-53e593d7e28b     python_batch 12008    8830   2306      872        6.7497  1779.04    0            1   11135     1000



النتيجة الأخيرة أعلاه هي اختبار Update الحقيقي: updated=1 وinserted=0 وunchanged=11135.

4. تشغيل PySpark المقارن على نفس العينة

Plain Text


rows_read       = 12008
valid           = 9293
corrected       = 1193
quarantine      = 1522
seconds_elapsed = 60.3969
throughput      = 198.82 records/second
partitions      = 2



مقارنة Python Batch المسجلة:

Plain Text


rows_read       = 12008
valid           = 8830
corrected       = 2306
quarantine      = 872
seconds_elapsed = 9.7663
throughput      = 1229.53 records/second



5. إنشاء عينة قابلة للإعداد

الأمر:

Plain Text


python src/create_small_sample.py --input data/orders_huge_mixed_quality.csv --output data/orders_sample_500.csv --rows 500 --seed 42



الناتج:

Plain Text


[Sample] source: data\\orders_huge_mixed_quality.csv (30000000 data rows)
[Sample] output: data\\orders_sample_500.csv (500 data rows)
501
500



501 = عدد الأسطر مع Header. 500 = عدد سجلات البيانات.

6. فحص MongoDB — Unique Index وSchema Validation

Plain Text


=== db.orders_validated.getIndexes() ===
[SON([('v', 2), ('key', SON([('_id', 1)])), ('name', '_id_')]),
 SON([('v', 2), ('key', SON([('order_id', 1)])), ('name', 'uniq_order_id'), ('unique', True)])]



Plain Text


=== db.getCollectionInfos({name: orders_validated})[0].options.validator ===
{'$jsonSchema': {'bsonType': 'object',
                 'properties': {'currency': {'bsonType': 'string'},
                                'customer_id': {'bsonType': 'string'},
                                'order_id': {'bsonType': 'string',
                                             'description': 'مفتاح العمل الأساسي - إلزامي'},
                                'total_amount': {'bsonType': ['double', 'int', 'long']}},
                 'required': ['order_id', 'customer_id', 'total_amount', 'currency']}}



7. فحص محرك Python Batch في Raw

Plain Text


[{'_id': 'python_batch', 'count': 12008}]



8. اختبار Update وIdempotency

Plain Text


order_id = طلب-100002
القيمة الأصلية للمدينة = حجة
القيمة بعد التعديل = عدن
count_updated = 1
count_inserted = 0
count_unchanged = 11135
target_matches = 1



تشغيل إعادة البيانات دون تغيير:

Plain Text


count_inserted = 0
count_updated = 0
count_unchanged = 11136



9. الاختبارات

Plain Text


25 passed in 0.13s



10. ملاحظات تقنية ظاهرة في السجلات

Plain Text


Input Partitions في تشغيل الملف الكبير = None
Stage 0 / 409 وStage 50 / 409 = عدد Tasks للمرحلة، وليس قيمة metrics.partitions
EXIT_CODE للـrun الكبير = 0



التصنيف في Python وPySpark ليس متطابقًا على العينة؛ Python يستدعي quality_rules.classify_record عبر elt_pipeline.py، بينما Spark يطبق DataFrame expressions داخل spark_loader.py. لذلك تُعرض مقاييس المحركين كما سُجلت، ولا تُذكر عبارة أن توزيع التصنيف متطابق.

11. الملفات المطلوبة

Plain Text


README.md
requirements.txt
config/settings.py
src/main.py
src/file_router.py
src/create_small_sample.py
src/batch_loader.py
src/spark_loader.py
src/quality_rules.py
src/elt_pipeline.py
src/mongo_setup.py
src/metrics.py
tests/test_classification.py
reports/results.json
reports/timing_run.json
reports/results_final.md
timing_run.log
presentation_samples.json
reports/screenshots/



