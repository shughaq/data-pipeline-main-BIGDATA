# tests/test_classification.py
# اختبارات لدالة التصنيف الكاملة classify_record - تغطي الحالات الثلاث:
# valid / corrected / quarantine، بالإضافة لقاعدة الاتساق الأساسية.

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.quality_rules import classify_record


def _base_row(**overrides):
    row = {
        "order_id": "ORD-1",
        "order_date": "2025-01-15T10:00:00",
        "status": "مؤكد",
        "customer_id": "CUS-1",
        "customer_name": "أحمد",
        "customer_phone": "777123456",
        "customer_email": "user@example.com",
        "city": "صنعاء",
        "district": "حدة",
        "delivery_type": "عادي",
        "delivery_cost": "2000.0",
        "payment_method": "بطاقة",
        "payment_status": "تم الدفع",
        "payment_amount": "12000.0",
        "currency": "YER",
        "total_amount": "12000.0",
        "items_json": '[{"sku":"A","qty":1,"unit_price":10000.0,"total":10000.0}]',
    }
    row.update(overrides)
    return row


def test_clean_record_is_valid():
    result = classify_record(_base_row(), set())
    assert result["quality_status"] == "valid"
    assert result["clean"]["order_id"] == "ORD-1"


def test_record_with_arabic_digits_is_corrected():
    row = _base_row(total_amount="١٢٠٠٠", delivery_cost="٢٠٠٠")
    result = classify_record(row, set())
    assert result["quality_status"] == "corrected"
    assert result["clean"]["total_amount"] == 12000.0


def test_missing_order_id_goes_to_quarantine():
    row = _base_row(order_id="")
    result = classify_record(row, set())
    assert result["quality_status"] == "quarantine"
    assert "MISSING_ORDER_ID" in result["error_codes"]


def test_corrupted_items_json_goes_to_quarantine():
    row = _base_row(items_json="not-json")
    result = classify_record(row, set())
    assert result["quality_status"] == "quarantine"
    assert "CORRUPTED_ITEMS_JSON" in result["error_codes"]


def test_negative_qty_is_ambiguous_quarantine():
    row = _base_row(items_json='[{"sku":"A","qty":-1,"unit_price":10000.0,"total":10000.0}]')
    result = classify_record(row, set())
    assert result["quality_status"] == "quarantine"
    assert "AMBIGUOUS_NEGATIVE_VALUE" in result["error_codes"]


def test_duplicate_order_id_within_same_run_is_quarantined():
    seen = set()
    first = classify_record(_base_row(order_id="ORD-DUP"), seen)
    second = classify_record(_base_row(order_id="ORD-DUP"), seen)
    assert first["quality_status"] != "quarantine"
    assert second["quality_status"] == "quarantine"
    assert "DUPLICATE_ORDER_ID" in second["error_codes"]


def test_multiple_core_errors_map_to_conflicting_code():
    row = _base_row(order_id="", customer_id="", items_json="not-json")
    result = classify_record(row, set())
    assert result["quality_status"] == "quarantine"
    assert result["error_codes"] == ["MULTIPLE_CONFLICTING_ERRORS"]
    assert "MISSING_ORDER_ID" in result["error_details"]


def test_total_mismatch_gets_recalculated():
    row = _base_row(total_amount="999999.0")  # لا يطابق مجموع العناصر + التوصيل
    result = classify_record(row, set())
    assert result["quality_status"] == "corrected"
    assert result["clean"]["total_amount"] == 12000.0
    codes = [c["rule_code"] for c in result["corrections"]]
    assert "ORDER_TOTAL_RECALCULATED" in codes


def test_consistency_rule_every_record_gets_exactly_one_outcome():
    rows = [
        _base_row(),
        _base_row(order_id="", customer_id=""),  # quarantine
        _base_row(order_id="ORD-2", total_amount="١٢٠٠٠"),  # corrected
    ]
    seen = set()
    outcomes = [classify_record(r, seen)["quality_status"] for r in rows]
    assert set(outcomes) == {"valid", "quarantine", "corrected"}
    assert len(outcomes) == 3  # كل سجل ناتج واحد بالضبط - لا مضاعفة ولا فقد
