from datetime import datetime, timezone
import hashlib
import json

from pymongo import UpdateOne

from src.quality_rules import classify_record


def make_raw_document(row, id_run, file_source, number_row_source, engine_used):
    """
    يبني مستند orders_raw حسب حقول البند 6.5:
    run_id, source_file, source_row_number, ingested_at, engine_used, raw_record
    السجل الخام (record_raw) يُخزَّن كما وصل تمامًا، بدون أي تحويل يفقد قيمته الأصلية.
    """
    return {
        "run_id": id_run,
        "source_file": file_source,
        "source_row_number": number_row_source,
        "ingested_at": datetime.now(timezone.utc),
        "engine_used": engine_used,
        "raw_record": dict(row),
    }


def build_validated_upsert(clean_record, quality_status, corrections, id_run):
    """
    يبني عملية UpdateOne (Upsert) لـorders_validated.
    نستخدم order_id كـStable Business Key (البند 6.10) بدل insert-then-check.
    $setOnInsert لأشياء ما لازم تتغيّر لو السجل موجود من قبل (تاريخ أول دخول)،
    و$set للحالة النهائية الحالية (يضمن Update صحيح بدون Duplicate).
    """
    now = datetime.now(timezone.utc)
    hash_payload = {
        "clean": clean_record,
        "quality_status": quality_status,
        "corrections": corrections,
    }
    canonical = json.dumps(
        hash_payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    record_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    set_fields = dict(clean_record)
    set_fields.update({
        "quality_status": quality_status,
        "corrections": corrections,
        "record_hash": record_hash,
    })

    return UpdateOne(
        {"order_id": clean_record["order_id"]},
        {
      
            "$set": set_fields,
            "$setOnInsert": {
                "at_first_validated": now,
                "last_seen_run": id_run,
            },
        },
        upsert=True,
    )


def build_quarantine_document(row, id_run, file_source, number_row_source, engine_used,
                               error_codes, error_details, corrections):
    """
    يبني مستند orders_quarantine. يحتوي error_codes وerror_details
    والسجل الخام كامل (البند 6.9)، عشان أي مراجعة لاحقة تقدر تفهم السبب
    وترجع للبيانات الأصلية.
    """
    return {
        "run_id": id_run,
        "source_file": file_source,
        "source_row_number": number_row_source,
        "quarantined_at": datetime.now(timezone.utc),
        "engine_used": engine_used,
        "error_codes": error_codes,
        "error_details": error_details,
        "partial_corrections_attempted": corrections,
        "raw_record": dict(row),
    }


def process_row(row, id_run, file_source, number_row_source, engine_used, seen_order_ids):
    """
    يعالج سطر واحد كامل: raw doc + التصنيف + (upsert أو quarantine doc).
    يرجع (raw_doc, outcome) حيث outcome dict فيه:
        status: valid/corrected/quarantine
        validated_op: UpdateOne أو None
        quarantine_doc: dict أو None
        error_codes: list
    هذه الدالة تُستخدم من داخل batch_loader لكل سجل في الدفعة.
    """
    raw_doc = make_raw_document(row, id_run, file_source, number_row_source, engine_used)

    result = classify_record(row, seen_order_ids)

    if result["quality_status"] == "quarantine":
        quarantine_doc = build_quarantine_document(
            row, id_run, file_source, number_row_source, engine_used,
            result["error_codes"], result["error_details"], result["corrections"],
        )
        return raw_doc, {
            "status": "quarantine",
            "validated_op": None,
            "quarantine_doc": quarantine_doc,
            "error_codes": result["error_codes"],
        }

    validated_op = build_validated_upsert(
        result["clean"], result["quality_status"], result["corrections"], id_run
    )
    return raw_doc, {
        "status": result["quality_status"],  # valid / corrected
        "validated_op": validated_op,
        "quarantine_doc": None,
        "error_codes": [],
    }
