import json
import re
from datetime import datetime, timezone

from dateutil import parser as date_parser

from config.settings import TARGET_CURRENCY

# ---------------------------------------------------------------------------
# جداول ثابتة (Lookup tables) تُستخدم في التصحيح
# ---------------------------------------------------------------------------

# تحويل الأرقام العربية-الهندية (والفارسية) إلى أرقام لاتينية
_ARABIC_DIGITS = "٠١٢٣٤٥٦٧٨٩"
_PERSIAN_DIGITS = "۰۱۲۳۴۵۶۷۸۹"
_LATIN_DIGITS = "0123456789"
_DIGIT_TRANSLATION = str.maketrans(
    _ARABIC_DIGITS + _PERSIAN_DIGITS,
    _LATIN_DIGITS + _LATIN_DIGITS,
)
# الفاصلة العشرية العربية (٫) مقابل النقطة، وفاصلة الآلاف العربية (٬) مقابل الفاصلة
_ARABIC_DECIMAL_SEP = "٫"
_ARABIC_THOUSANDS_SEP = "٬"

# كلمات/رموز العملة المعروفة ليمن (كلها تعني نفس العملة YER حسب معطيات المشروع)
_CURRENCY_WORDS = [
    "لاير يمني", "لاير", "ريال يمني", "ريال", "YER", "yer", "ر.ي", "ر.ي.",
]

# أرقام مكتوبة بالكلمات (قيم محددة معروفة فقط - لا نخمّن قيم غير مذكورة هنا)
_WORD_NUMBERS = {
    "ألف": 1000, "الف": 1000,
    "ألفان": 2000, "الفان": 2000,
    "ألفين": 2000, "الفين": 2000,
    "ثلاثة آلاف": 3000, "ثلاثة الاف": 3000,
    "أربعة آلاف": 4000, "اربعة الاف": 4000,
    "خمسة آلاف": 5000, "خمسة الاف": 5000,
    "ستة آلاف": 6000, "ستة الاف": 6000,
    "سبعة آلاف": 7000, "سبعة الاف": 7000,
    "ثمانية آلاف": 8000, "ثمانية الاف": 8000,
    "تسعة آلاف": 9000, "تسعة الاف": 9000,
    "عشرة آلاف": 10000, "عشرة الاف": 10000,
}

PHONE_PATTERN = re.compile(r"^(967)?(77|73|70|71)\d{7}$")
EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

ALLOWED_STATUS = {
    "قيد الانتظار", "مؤكد", "قيد الشحن", "تم التسليم", "مرتجع", "ملغي",
}
ALLOWED_PAYMENT_METHOD = {"نقدًا عند التسليم", "بطاقة", "محفظة إلكترونية"}
ALLOWED_PAYMENT_STATUS = {"بانتظار الدفع", "تم الدفع", "مرفوض"}
ALLOWED_DELIVERY_TYPE = {"عادي", "سريع"}

# رموز أسباب العزل (البند 6.8) + كودين أضفناهم لتغطية حالات نص عليها البند 6.6
# ما كان لها كود جاهز في الجدول (بريد غير قابل للإصلاح، وتضارب أخطاء).
# موثّق أيضًا في README تحت "قرارات هندسية إضافية".
Q_ID_ORDER_MISSING = "MISSING_ORDER_ID"
Q_ID_CUSTOMER_MISSING = "MISSING_CUSTOMER_ID"
Q_DATE_IMPOSSIBLE_INVALID = "INVALID_IMPOSSIBLE_DATE"
Q_JSON_ITEMS_CORRUPTED = "CORRUPTED_ITEMS_JSON"
Q_ITEMS_EMPTY = "EMPTY_ITEMS"
Q_PRICE_UNKNOWN = "UNKNOWN_PRICE"
Q_VALUE_NEGATIVE_AMBIGUOUS = "AMBIGUOUS_NEGATIVE_VALUE"
Q_ID_ORDER_DUPLICATE = "DUPLICATE_ORDER_ID"
Q_ERRORS_CONFLICTING_MULTIPLE = "MULTIPLE_CONFLICTING_ERRORS"
# إضافتنا (موثقة في الـREADME):
Q_EMAIL_UNFIXABLE = "EMAIL_UNFIXABLE"


# ---------------------------------------------------------------------------
# أدوات مساعدة عامة
# ---------------------------------------------------------------------------

def _clean_text(value):
    """trim بسيط + توحيد المسافات المتكررة/غير المرئية (rule 6.6: المسافات)."""
    if value is None:
        return ""
    text = str(value)
    # مسافات غير مرئية شائعة (NBSP، zero-width) قد تتسرب من نسخ/لصق البيانات
    text = text.replace("\u00a0", " ").replace("\u200b", "")
    return text.strip()


def normalize_arabic_digits(text):
    """rule: الأرقام العربية -> لاتينية، وتوحيد الفاصلة العشرية/الآلاف."""
    if text is None:
        return None
    text = str(text).translate(_DIGIT_TRANSLATION)
    text = text.replace(_ARABIC_DECIMAL_SEP, ".")
    text = text.replace(_ARABIC_THOUSANDS_SEP, ",")
    return text


def parse_money(raw_value, corrections, field_name):
    """
    يحاول يحوّل قيمة سعر/مبلغ خام (نص) إلى float نظيف.
    يطبق بالترتيب: تحويل أرقام عربية -> إزالة نص عملة -> إزالة فواصل آلاف
    -> تحويل كلمات معروفة -> تحويل لرقم.
    يرجع (value: float|None, ok: bool)
    """
    if raw_value is None:
        return None, False

    original = str(raw_value)
    text = _clean_text(original)
    if text == "":
        return None, False

    changed = False

    # 1) أرقام عربية/فارسية وفواصل عشرية عربية
    normalized_digits = normalize_arabic_digits(text)
    if normalized_digits != text:
        changed = True
    text = normalized_digits

    # 2) كلمات/رموز عملة معروفة (نشيلها لأننا نخزن السعر كرقم + حقل currency منفصل)
    for word in _CURRENCY_WORDS:
        if word in text:
            text = text.replace(word, "")
            changed = True
    text = text.strip()

    # 3) فواصل الآلاف الغربية (125,000.00 -> 125000.00)
    if re.search(r"\d,\d{3}", text):
        text = text.replace(",", "")
        changed = True

    # 4) أرقام مكتوبة بالكلمات (قيم معروفة ومحددة فقط)
    if text in _WORD_NUMBERS:
        text = str(_WORD_NUMBERS[text])
        changed = True

    text = text.strip()
    if text in ("", "؟؟؟", "???", "null", "None", "NaN"):
        return None, False

    try:
        value = float(text)
    except ValueError:
        return None, False

    if changed:
        corrections.append({
            "field": field_name,
            "original_value": original,
            "corrected_value": value,
            "rule_code": "ARABIC_DIGITS_CONVERTED"
                         if normalized_digits != _clean_text(original)
                         else "CURRENCY_OR_FORMAT_NORMALIZED",
        })
    return value, True


def clean_currency(raw_currency):
    """rule: رمز/اسم العملة -> يوحّد دائمًا إلى TARGET_CURRENCY (YER هنا)."""
    text = _clean_text(raw_currency)
    if text == "":
        return TARGET_CURRENCY, False
    if text.upper() == TARGET_CURRENCY:
        return TARGET_CURRENCY, False
    # أي قيمة عملة أخرى مذكورة (لاير، ريال...) نعرف أنها كلها نفس عملة المشروع
    return TARGET_CURRENCY, True


def clean_phone(raw_phone, corrections):
    """rule: رقم الهاتف -> إزالة مسافات/رموز وتوحيد الصيغة لما يكون واضحًا."""
    original = _clean_text(raw_phone)
    if original == "":
        return "", False

    digits = normalize_arabic_digits(original)
    digits = re.sub(r"[^\d+]", "", digits)   # نشيل مسافات وشرطات وأقواس
    digits = digits.replace("+", "")

    if digits.startswith("00967"):
        digits = digits[5:]
    elif digits.startswith("967"):
        digits = digits[3:]

    cleaned = digits
    if PHONE_PATTERN.match(digits) or (len(digits) == 9 and digits[:2] in ("77", "73", "70", "71")):
        cleaned = digits
    # ما نرفض الرقم حتى لو ما طابق النمط بالضبط - الهاتف مو حقل جوهري يستوجب عزل
    # حسب قائمة أسباب العزل في التكليف، فقط ننظفه أفضل ما نقدر.

    if cleaned != original:
        corrections.append({
            "field": "customer_phone",
            "original_value": original,
            "corrected_value": cleaned,
            "rule_code": "PHONE_FORMAT_NORMALIZED",
        })
    return cleaned, cleaned != original


def clean_email(raw_email, corrections):
    """
    rule: البريد الإلكتروني -> نصلح فقط التكرار الواضح (@@ أو نقطتين متتاليتين).
    غير كذا نتركه للتصنيف يقرر عزل السجل (rule 6.6 + 6.9).
    يرجع (value, unfixable: bool)
    """
    original = _clean_text(raw_email)
    if original == "":
        return "", True  # بريد فاضي = غير قابل للإصلاح (لكن ليس بالضرورة سبب عزل جوهري لوحده)

    text = original
    while "@@" in text:
        text = text.replace("@@", "@")
    while ".." in text:
        text = text.replace("..", ".")

    if text != original:
        corrections.append({
            "field": "customer_email",
            "original_value": original,
            "corrected_value": text,
            "rule_code": "EMAIL_REPEATED_SYMBOLS",
        })

    if EMAIL_PATTERN.match(text):
        return text, False
    return text, True


def clean_date(raw_date, corrections):
    """
    rule: التاريخ -> تحويل أي صيغة معقولة إلى ISO 8601 قياسية.
    يرجع (iso_string|None, invalid: bool)
    """
    original = _clean_text(raw_date)
    if original == "":
        return None, True

    text = normalize_arabic_digits(original)
    text = text.replace("/", "-")

    parsed = None
    # نجرب dayfirst=False أولاً (لأن أغلب البيانات بصيغة YYYY-MM-DD)،
    # ولو فشل أو طلع تاريخ غير منطقي نجرب dayfirst=True (لصيغ زي 17-01-2025)
    for dayfirst in (False, True):
        try:
            parsed = date_parser.parse(text, dayfirst=dayfirst)
            break
        except (ValueError, OverflowError):
            continue

    if parsed is None:
        return None, True

    # تاريخ "مستحيل" منطقيًا: سنة بعيدة جدًا (بيانات تالفة غالبًا)
    if parsed.year < 2000 or parsed.year > 2100:
        return None, True

    iso = parsed.strftime("%Y-%m-%dT%H:%M:%S")
    if iso != original:
        corrections.append({
            "field": "order_date",
            "original_value": original,
            "corrected_value": iso,
            "rule_code": "DATE_FORMAT_NORMALIZED",
        })
    return iso, False


def canonicalize_choice(raw_value, allowed_set, field_name, corrections):
    """rule: المسافات والمرادفات -> trim وتوحيد للقاموس القياسي إن أمكن."""
    original = _clean_text(raw_value)
    trimmed = original
    # نشيل مسافات مكررة بالنص كامل
    trimmed = re.sub(r"\s+", " ", trimmed).strip()

    value = trimmed
    if trimmed not in allowed_set:
        # نحاول تطابق غير حساس لطول المسافات فقط (best-effort، بدون تخمين المعنى)
        for candidate in allowed_set:
            if candidate.strip() == trimmed.strip():
                value = candidate
                break

    if value != original:
        corrections.append({
            "field": field_name,
            "original_value": original,
            "corrected_value": value,
            "rule_code": "TEXT_FIELD_TRIMMED_NORMALIZED",
        })
    return value


def parse_items_json(raw_items):
    """rule/quarantine: يحلل items_json. يرجع (items:list|None, corrupted:bool)."""
    text = _clean_text(raw_items)
    if text == "":
        return None, True
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None, True
    if not isinstance(data, list):
        return None, True
    return data, False


def clean_items(items, corrections):
    """
    ينظف كل عنصر داخل items (أرقام عربية، فواصل آلاف...) ويكتشف الكميات/المبالغ
    السالبة الغامضة. يرجع (cleaned_items, has_negative_ambiguous: bool, ok: bool)
    """
    cleaned_items = []
    has_negative = False
    ok = True

    for idx, item in enumerate(items):
        if not isinstance(item, dict):
            ok = False
            continue

        new_item = dict(item)

        qty_raw = item.get("qty")
        price_raw = item.get("unit_price")
        total_raw = item.get("total")

        qty_val, qty_ok = parse_money(qty_raw, corrections, f"items[{idx}].qty")
        price_val, price_ok = parse_money(price_raw, corrections, f"items[{idx}].unit_price")
        total_val, total_ok = parse_money(total_raw, corrections, f"items[{idx}].total")

        if not (qty_ok and price_ok and total_ok):
            ok = False
            continue

        if qty_val < 0 or price_val < 0 or total_val < 0:
            has_negative = True
            continue

        new_item["qty"] = qty_val
        new_item["unit_price"] = price_val
        new_item["total"] = total_val
        cleaned_items.append(new_item)

    return cleaned_items, has_negative, ok


def recompute_total(items, delivery_cost, current_total, corrections):
    """
    rule: إجمالي الطلب -> إعادة الحساب من العناصر + التوصيل إذا كانت المكونات
    صالحة ولا تطابق المجموع الحالي.
    """
    items_sum = sum(item["total"] for item in items)
    expected_total = round(items_sum + (delivery_cost or 0.0), 2)

    if current_total is None:
        corrections.append({
            "field": "total_amount",
            "original_value": None,
            "corrected_value": expected_total,
            "rule_code": "ORDER_TOTAL_RECALCULATED",
        })
        return expected_total

    if abs(current_total - expected_total) > 0.5:  # هامش بسيط للتقريب العشري
        corrections.append({
            "field": "total_amount",
            "original_value": current_total,
            "corrected_value": expected_total,
            "rule_code": "ORDER_TOTAL_RECALCULATED",
        })
        return expected_total

    return current_total


# ---------------------------------------------------------------------------
# الدالة الرئيسية: تصنيف سجل واحد
# ---------------------------------------------------------------------------

def classify_record(row, seen_order_ids):
    """
    row: dict خام (كما جاء من CSV/Spark) لسجل طلب واحد.
    seen_order_ids: set مشتركة عبر نفس التشغيل (id_run) لاكتشاف التكرار الداخلي.
        (بالنسبة لـ Idempotency عبر تشغيلات مختلفة، هذا غير مسؤول عنه - ذاك يخص
        Upsert على orders_validated، مو مسؤولية هذه الدالة).

    يرجع dict فيه:
        quality_status: "valid" | "corrected" | "quarantine"
        clean: dict الحقول النظيفة (لو valid/corrected)
        corrections: list
        error_codes: list[str]  (فاضية لو مو quarantine)
        error_details: str  (وصف مختصر يجمع الأسباب)
    """
    corrections = []
    error_codes = []

    order_id = _clean_text(row.get("order_id"))
    customer_id = _clean_text(row.get("customer_id"))

    if order_id == "":
        error_codes.append(Q_ID_ORDER_MISSING)
    if customer_id == "":
        error_codes.append(Q_ID_CUSTOMER_MISSING)

    # تكرار id_order داخل نفس التشغيل (نفحصه بس لو أصلاً عنده order_id)
    if order_id and order_id in seen_order_ids:
        error_codes.append(Q_ID_ORDER_DUPLICATE)
    elif order_id:
        seen_order_ids.add(order_id)

    # التاريخ
    order_date_iso, date_invalid = clean_date(row.get("order_date"), corrections)
    if date_invalid:
        error_codes.append(Q_DATE_IMPOSSIBLE_INVALID)

    # items_json
    items_raw, items_corrupted = parse_items_json(row.get("items_json"))
    items_clean, items_empty, negative_ambiguous = [], False, False
    if items_corrupted:
        error_codes.append(Q_JSON_ITEMS_CORRUPTED)
    else:
        if len(items_raw) == 0:
            items_empty = True
            error_codes.append(Q_ITEMS_EMPTY)
        else:
            items_clean, negative_ambiguous, items_ok = clean_items(items_raw, corrections)
            if negative_ambiguous:
                error_codes.append(Q_VALUE_NEGATIVE_AMBIGUOUS)
            if len(items_clean) == 0 and not negative_ambiguous:
                # كل العناصر فشل تفسيرها رقميًا -> نعتبرها فارغة فعليًا
                items_empty = True
                error_codes.append(Q_ITEMS_EMPTY)

    # الحقول المالية الأساسية
    delivery_cost, delivery_ok = parse_money(row.get("delivery_cost"), corrections, "delivery_cost")
    total_amount, total_ok = parse_money(row.get("total_amount"), corrections, "total_amount")
    currency, currency_corrected = clean_currency(row.get("currency"))
    if currency_corrected:
        corrections.append({
            "field": "currency",
            "original_value": row.get("currency"),
            "corrected_value": currency,
            "rule_code": "CURRENCY_UNIFIED",
        })

    price_unknown = False
    final_total = None
    if not items_corrupted and not items_empty and not negative_ambiguous:
        # نقدر نعيد حساب الإجمالي لأن العناصر سليمة
        delivery_for_calc = delivery_cost if delivery_ok else 0.0
        final_total = recompute_total(items_clean, delivery_for_calc, total_amount if total_ok else None, corrections)
    else:
        # ما نقدر نتأكد من الإجمالي عن طريق العناصر (لأنها تالفة/فارغة)
        if total_ok:
            final_total = total_amount
        else:
            price_unknown = True
            error_codes.append(Q_PRICE_UNKNOWN)

    # الهاتف والبريد (ما توديان لعزل لوحدها إلا البريد لو غير قابل للإصلاح)
    phone_clean, _ = clean_phone(row.get("customer_phone"), corrections)
    email_clean, email_unfixable = clean_email(row.get("customer_email"), corrections)
    if email_unfixable and _clean_text(row.get("customer_email")) != "":
        # بريد موجود لكن مكسور وما قدرنا نصلحه بثقة
        error_codes.append(Q_EMAIL_UNFIXABLE)

    # حقول نصية بسيطة (trim + توحيد قاموس)
    status_clean = canonicalize_choice(row.get("status"), ALLOWED_STATUS, "status", corrections)
    payment_method_clean = canonicalize_choice(row.get("payment_method"), ALLOWED_PAYMENT_METHOD, "payment_method", corrections)
    payment_status_clean = canonicalize_choice(row.get("payment_status"), ALLOWED_PAYMENT_STATUS, "payment_status", corrections)
    delivery_type_clean = canonicalize_choice(row.get("delivery_type"), ALLOWED_DELIVERY_TYPE, "delivery_type", corrections)

    payment_amount, _ = parse_money(row.get("payment_amount"), corrections, "payment_amount")

    # ---- القرار النهائي ----
    unique_error_codes = list(dict.fromkeys(error_codes))  # نحافظ على الترتيب ونشيل التكرار

    if len(unique_error_codes) >= 2:
        final_codes = [Q_ERRORS_CONFLICTING_MULTIPLE]
        # نحتفظ بالتفاصيل الحقيقية في error_details حتى لو الكود النهائي عام
        detail_codes = unique_error_codes
    elif len(unique_error_codes) == 1:
        final_codes = unique_error_codes
        detail_codes = unique_error_codes
    else:
        final_codes = []
        detail_codes = []

    if final_codes:
        return {
            "quality_status": "quarantine",
            "clean": None,
            "corrections": corrections,
            "error_codes": final_codes,
            "error_details": "; ".join(detail_codes),
        }

    clean_record = {
        "order_id": order_id,
        "order_date": order_date_iso,
        "status": status_clean,
        "customer_id": customer_id,
        "customer_name": _clean_text(row.get("customer_name")),
        "customer_phone": phone_clean,
        "customer_email": email_clean,
        "city": _clean_text(row.get("city")),
        "district": _clean_text(row.get("district")),
        "delivery_type": delivery_type_clean,
        "delivery_cost": delivery_cost if delivery_ok else 0.0,
        "payment_method": payment_method_clean,
        "payment_status": payment_status_clean,
        "payment_amount": payment_amount,
        "currency": currency,
        "total_amount": final_total,
        "items": items_clean,
    }

    return {
        "quality_status": "corrected" if corrections else "valid",
        "clean": clean_record,
        "corrections": corrections,
        "error_codes": [],
        "error_details": "",
    }
