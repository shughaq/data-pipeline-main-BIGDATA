# tests/test_cleaning_rules.py
# اختبارات لقواعد التنظيف الفردية في src/quality_rules.py

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.quality_rules import (
    normalize_arabic_digits,
    parse_money,
    clean_currency,
    clean_phone,
    clean_email,
    clean_date,
    parse_items_json,
    canonicalize_choice,
    ALLOWED_STATUS,
)


def test_arabic_digits_conversion():
    assert normalize_arabic_digits("٥٠٠٠") == "5000"
    assert normalize_arabic_digits("١٢٣٫٥") == "123.5"


def test_parse_money_thousands_separator():
    corrections = []
    value, ok = parse_money("125,000.00", corrections, "total_amount")
    assert ok is True
    assert value == 125000.00
    assert len(corrections) == 1


def test_parse_money_currency_word():
    corrections = []
    value, ok = parse_money("5000 لاير", corrections, "delivery_cost")
    assert ok is True
    assert value == 5000.0


def test_parse_money_word_number_known_value():
    corrections = []
    value, ok = parse_money("خمسة آلاف", corrections, "total_amount")
    assert ok is True
    assert value == 5000.0


def test_parse_money_unrecognized_text_fails_safely():
    corrections = []
    value, ok = parse_money("???", corrections, "total_amount")
    assert ok is False
    assert value is None


def test_clean_currency_unifies_to_yer():
    value, corrected = clean_currency("لاير يمني")
    assert value == "YER"
    assert corrected is True

    value2, corrected2 = clean_currency("YER")
    assert value2 == "YER"
    assert corrected2 is False


def test_clean_phone_removes_country_code_and_spaces():
    corrections = []
    value, changed = clean_phone("+967 77 123 4567", corrections)
    assert value == "771234567"
    assert changed is True


def test_clean_email_fixes_repeated_symbols():
    corrections = []
    value, unfixable = clean_email("user@@mail..com", corrections)
    assert value == "user@mail.com"
    assert unfixable is False
    assert corrections[0]["rule_code"] == "EMAIL_REPEATED_SYMBOLS"


def test_clean_email_unfixable_case():
    corrections = []
    value, unfixable = clean_email("@@", corrections)
    assert unfixable is True


def test_clean_date_normalizes_slash_format():
    corrections = []
    iso, invalid = clean_date("2025/01/31", corrections)
    assert invalid is False
    assert iso.startswith("2025-01-31")


def test_clean_date_normalizes_dayfirst_format():
    corrections = []
    iso, invalid = clean_date("17-01-2025 04:50:00", corrections)
    assert invalid is False
    assert iso == "2025-01-17T04:50:00"


def test_clean_date_rejects_impossible_date():
    corrections = []
    iso, invalid = clean_date("not-a-date", corrections)
    assert invalid is True
    assert iso is None


def test_parse_items_json_valid():
    items, corrupted = parse_items_json('[{"sku":"A","qty":1,"unit_price":100.0,"total":100.0}]')
    assert corrupted is False
    assert len(items) == 1


def test_parse_items_json_corrupted():
    items, corrupted = parse_items_json("not-json")
    assert corrupted is True
    assert items is None


def test_canonicalize_choice_trims_spaces():
    # _clean_text() يشيل المسافات الطرفية من البداية، فالقيمة الناتجة نظيفة
    # سواء سُجّلت "تصحيح" رسمي أو لا (المهم إنها تطابق القاموس القياسي).
    corrections = []
    value = canonicalize_choice("  مؤكد  ", ALLOWED_STATUS, "status", corrections)
    assert value == "مؤكد"


def test_canonicalize_choice_leaves_valid_value_unchanged():
    corrections = []
    value = canonicalize_choice("مؤكد", ALLOWED_STATUS, "status", corrections)
    assert value == "مؤكد"
    assert corrections == []
