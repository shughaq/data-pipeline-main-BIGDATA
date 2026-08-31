import hashlib
import json
import time
from datetime import datetime, timezone

from config.settings import INCREMENTAL_STATE_PATH
from src.batch_loader import _flush_batch
from src.elt_pipeline import process_row

import csv


def _file_hash(file_path):
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_state():
    if INCREMENTAL_STATE_PATH.exists():
        with INCREMENTAL_STATE_PATH.open(encoding="utf-8") as f:
            return json.load(f)
    return {"runs": []}


def _save_state(state):
    INCREMENTAL_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with INCREMENTAL_STATE_PATH.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def run_incremental_load(file_path, db, id_run, metrics, stage_label, batch_size=1000):
    """
    stage_label: "initial" أو "delta" - فقط لغرض التوثيق/الطباعة، المنطق نفسه.
    يستخدم نفس آلية batch_loader (Streaming + دفعات + Upsert)، لكن يضيف
    تتبع بصمة الملف في data/_incremental_state.json لإثبات الـIdempotency
    عمليًا عند إعادة تشغيل نفس الـDelta (البند: "إثبات المسار بثلاث تجارب").
    """
    state = _load_state()
    file_hash = _file_hash(file_path)

    previous_run = next((r for r in state["runs"] if r["file_hash"] == file_hash), None)
    if previous_run:
        print(f"[Incremental] تنبيه: هذا الملف ({file_path}) سبق معالجته في "
              f"id_run={previous_run['id_run']} ({previous_run['stage']}) بتاريخ "
              f"{previous_run['at']}. سنعيد تطبيق الـUpsert وهو آمن (Idempotent) "
              "لأن الكتابة تعتمد على order_id كمفتاح ثابت وليس على $inc.")
    else:
        print(f"[Incremental] ملف جديد لم يُعالج من قبل: {file_path}")

    print(f"[Incremental] مرحلة: {stage_label} | id_run={id_run}")

    seen_order_ids = set()
    file_source = str(file_path)
    engine_used = f"incremental_{stage_label}"

    raw_docs, validated_ops, quarantine_docs = [], [], []
    batch_number = 0
    batch_start_time = time.perf_counter()

    with open(file_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row_number, row in enumerate(reader, start=1):
            metrics.read_rows += 1
            raw_doc, outcome = process_row(row, id_run, file_source, row_number, engine_used, seen_order_ids)
            raw_docs.append(raw_doc)
            metrics.loaded_raw += 1

            if outcome["status"] == "quarantine":
                metrics.count_quarantine += 1
                metrics.add_error_codes(outcome["error_codes"])
                quarantine_docs.append(outcome["quarantine_doc"])
            else:
                if outcome["status"] == "corrected":
                    metrics.count_corrected += 1
                else:
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

    state["runs"].append({
        "id_run": id_run,
        "stage": stage_label,
        "file": file_source,
        "file_hash": file_hash,
        "at": datetime.now(timezone.utc).isoformat(),
        "count_inserted": metrics.count_inserted,
        "count_updated": metrics.count_updated,
        "count_unchanged": metrics.count_unchanged,
    })
    _save_state(state)

    print(f"[Incremental] انتهت مرحلة {stage_label}: "
          f"inserted={metrics.count_inserted} updated={metrics.count_updated} unchanged={metrics.count_unchanged}")

    return metrics
