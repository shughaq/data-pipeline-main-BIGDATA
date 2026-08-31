import csv
import time

from pymongo.errors import BulkWriteError

from config.settings import BATCH_SIZE, COLLECTION_RAW, COLLECTION_VALIDATED, COLLECTION_QUARANTINE
from src.elt_pipeline import process_row


def _flush_batch(db, id_run, raw_docs, validated_ops, quarantine_docs, batch_number, batch_start_time, metrics):
    """يكتب دفعة واحدة على الـ3 collections ويطبع مقاييسها. يرجع لا شيء، فقط يحدّث metrics."""
    n_records = len(raw_docs)

    try:
        if raw_docs:
            db[COLLECTION_RAW].insert_many(raw_docs, ordered=False)
    except BulkWriteError as exc:
        print(f"[Batch #{batch_number}] خطأ جزئي في كتابة orders_raw: "
              f"{exc.details.get('writeErrors', exc.details)}")

    inserted = updated = unchanged = 0
    if validated_ops:
        try:
            result = db[COLLECTION_VALIDATED].bulk_write(validated_ops, ordered=False)
            inserted = result.upserted_count
            updated = result.modified_count
            unchanged = max(result.matched_count - result.modified_count, 0)
        except BulkWriteError as exc:
            print(f"[Batch #{batch_number}] خطأ جزئي في Upsert لـ orders_validated: "
                  f"{exc.details.get('writeErrors', exc.details)}")

    # 3) orders_quarantine
    try:
        if quarantine_docs:
            db[COLLECTION_QUARANTINE].insert_many(quarantine_docs, ordered=False)
    except BulkWriteError as exc:
        print(f"[Batch #{batch_number}] خطأ جزئي في كتابة orders_quarantine: "
              f"{exc.details.get('writeErrors', exc.details)}")

    elapsed = time.perf_counter() - batch_start_time
    rate = n_records / elapsed if elapsed > 0 else 0.0
    print(f"[Batch #{batch_number}] سجلات: {n_records} | زمن: {elapsed:.3f}s | "
          f"معدل الإدخال: {rate:.1f} سجل/ثانية | inserted={inserted} updated={updated} unchanged={unchanged}")

    metrics.count_inserted += inserted
    metrics.count_updated += updated
    metrics.count_unchanged += unchanged


def run_batch_load(file_path, db, id_run, metrics, batch_size=None, seen_order_ids=None):
    """
    يقرأ ملف CSV بالتدفق (سطر سطر عبر csv.DictReader) ويحمّله بدفعات.
    seen_order_ids: set تُمرَّر من الخارج (تُستخدم أيضًا لو أردنا لاحقًا فحص تكرار
    عبر مصادر متعددة في نفس التشغيل)، افتراضيًا نبدأ set جديدة لكل تشغيل.
    """
    batch_size = batch_size or BATCH_SIZE
    metrics.size_batch = batch_size
    seen_order_ids = seen_order_ids if seen_order_ids is not None else set()

    file_source = str(file_path)
    engine_used = "python_batch"

    raw_docs, validated_ops, quarantine_docs = [], [], []
    batch_number = 0
    batch_start_time = time.perf_counter()

    print(f"[BatchLoader] بدء القراءة Streaming من: {file_source} (حجم الدفعة: {batch_size})")

    with open(file_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)  # قراءة سطر سطر - لا نحوّله لقائمة أبدًا
        row_number = 0

        for row in reader:
            row_number += 1
            metrics.read_rows += 1

            raw_doc, outcome = process_row(
                row, id_run, file_source, row_number, engine_used, seen_order_ids
            )
            raw_docs.append(raw_doc)
            metrics.loaded_raw += 1

            if outcome["status"] == "quarantine":
                metrics.count_quarantine += 1
                metrics.add_error_codes(outcome["error_codes"])
                quarantine_docs.append(outcome["quarantine_doc"])
            elif outcome["status"] == "corrected":
                metrics.count_corrected += 1
                validated_ops.append(outcome["validated_op"])
            else:  # valid
                metrics.count_valid += 1
                validated_ops.append(outcome["validated_op"])

            if len(raw_docs) >= batch_size:
                batch_number += 1
                _flush_batch(db, id_run, raw_docs, validated_ops, quarantine_docs,
                             batch_number, batch_start_time, metrics)
                raw_docs, validated_ops, quarantine_docs = [], [], []
                batch_start_time = time.perf_counter()

    if raw_docs:
        batch_number += 1
        _flush_batch(db, id_run, raw_docs, validated_ops, quarantine_docs,
                     batch_number, batch_start_time, metrics)

    print(f"[BatchLoader] انتهى: {metrics.read_rows} سجل مقروء عبر {batch_number} دفعة.")
    return metrics
