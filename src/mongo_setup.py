from pymongo import ASCENDING
from pymongo.errors import CollectionInvalid

from config.settings import (
    COLLECTION_RAW,
    COLLECTION_VALIDATED,
    COLLECTION_QUARANTINE,
)

_VALIDATED_SCHEMA = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["order_id", "customer_id", "total_amount", "currency"],
        "properties": {
            "order_id": {"bsonType": "string", "description": "مفتاح العمل الأساسي - إلزامي"},
            "customer_id": {"bsonType": "string"},
            "total_amount": {"bsonType": ["double", "int", "long"]},
            "currency": {"bsonType": "string"},
        },
    }
}


def ensure_collections(db):
    """
    ينشئ الـcollections الثلاث لو مو موجودة، ويضبط الـSchema Validation
    والـIndex المطلوبين. آمن يُستدعى أكثر من مرة (idempotent).
    """
    existing = set(db.list_collection_names())

    # 1) orders_raw: بدون أي Validator أو Unique Index (نص صريح في التكليف)
    if COLLECTION_RAW not in existing:
        db.create_collection(COLLECTION_RAW)
        print(f"[MongoSetup] أُنشئت collection: {COLLECTION_RAW} (بدون Validator/Index)")

    db[COLLECTION_RAW].create_index([("id_run", ASCENDING)])
    db[COLLECTION_RAW].create_index([("order_id", ASCENDING)])

    # 2) orders_validated: Schema Validation + Unique Index على order_id
    if COLLECTION_VALIDATED not in existing:
        try:
            db.create_collection(
                COLLECTION_VALIDATED,
                validator=_VALIDATED_SCHEMA,
                validationLevel="moderate",
                validationAction="warn",  # warn بدل error: نوثّق التحذير بدل ما نوقف الـUpsert
            )
            print(f"[MongoSetup] أُنشئت collection: {COLLECTION_VALIDATED} (مع Schema Validation)")
        except CollectionInvalid:
            pass
    else:
        try:
            db.command({
                "collMod": COLLECTION_VALIDATED,
                "validator": _VALIDATED_SCHEMA,
                "validationLevel": "moderate",
                "validationAction": "warn",
            })
        except Exception as exc:  # noqa: BLE001 - نطبع تحذير فقط، ما نوقف المشروع
            print(f"[MongoSetup] تحذير: تعذّر تحديث الـSchema Validation ({exc})")

    db[COLLECTION_VALIDATED].create_index(
        [("order_id", ASCENDING)], unique=True, name="uniq_order_id"
    )

    # 3) orders_quarantine: بدون قيود صارمة (نريد نقدر نعزل أي سجل مهما كان شكله)
    if COLLECTION_QUARANTINE not in existing:
        db.create_collection(COLLECTION_QUARANTINE)
        print(f"[MongoSetup] أُنشئت collection: {COLLECTION_QUARANTINE}")
    db[COLLECTION_QUARANTINE].create_index([("id_run", ASCENDING)])

    print("[MongoSetup] كل الـcollections والـIndexes جاهزة.")
