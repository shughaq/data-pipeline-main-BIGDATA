"""Final-phase MongoDB features: queries, indexes, aggregations and materialized summaries."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from pymongo import ASCENDING, DESCENDING

from config.settings import COLLECTION_VALIDATED, MV_DAILY_SALES, MV_TOP_PRODUCTS, MV_LEDGER, MV_STATE


QUERY_DEFINITIONS = {
    "orders_by_city_status": {
        "description": "الطلبات حسب المدينة والحالة مرتبة بالأحدث",
        "filter": lambda p: {"city": p.get("city", "صنعاء"), "status": p.get("status", "تم التسليم")},
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
        "index": "idx_city_status_date",
    },
    "customer_orders": {
        "description": "طلبات عميل محدد حسب التاريخ",
        "filter": lambda p: {"customer_id": p.get("customer_id", "")},
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
        "index": "idx_customer_date",
    },
    "high_value_orders": {
        "description": "الطلبات مرتفعة القيمة",
        "filter": lambda p: {"total_amount": {"$gte": float(p.get("min_total", 100000))}},
        "sort": [("total_amount", DESCENDING)],
        "limit": 20,
        "index": "idx_total_amount",
    },
    "sales_by_period": {
        "description": "الطلبات خلال فترة زمنية",
        "filter": lambda p: _date_range_filter(p),
        "sort": [("order_date", ASCENDING)],
        "limit": 100,
        "index": "idx_city_status_date",
    },
    "orders_by_status": {
        "description": "توزيع الطلبات وفق حالة الطلب",
        "filter": lambda p: {"status": p.get("status", "تم التسليم")},
        "sort": [("order_date", DESCENDING)],
        "limit": 50,
        "index": "idx_status_date",
    },
}


def _parse_dt(value: str | None):
    if not value:
        return None
    value = value.strip().replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        raise ValueError(f"Invalid ISO date: {value}")


def _date_range_filter(params: dict[str, Any]):
    start = _parse_dt(params.get("start"))
    end = _parse_dt(params.get("end"))
    if not start or not end:
        raise ValueError("start and end are required for sales_by_period")
    return {"order_date": {"$gte": start, "$lte": end}}


def create_query_indexes(db) -> list[str]:
    """Create the three required query indexes (including one compound index)."""
    specs = [
        ([('city', ASCENDING), ('status', ASCENDING), ('order_date', DESCENDING)], "idx_city_status_date"),
        ([('customer_id', ASCENDING), ('order_date', DESCENDING)], "idx_customer_date"),
        ([('status', ASCENDING), ('order_date', DESCENDING)], "idx_status_date"),
        # Supporting index for the high-value query; kept separate from the required 3.
        ([('total_amount', DESCENDING)], "idx_total_amount"),
    ]
    created = []
    for keys, name in specs:
        db[COLLECTION_VALIDATED].create_index(keys, name=name)
        created.append(name)
    return created


def list_query_names():
    return [
        {"name": name, "description": cfg["description"], "index": cfg["index"]}
        for name, cfg in QUERY_DEFINITIONS.items()
    ]


def run_query(db, name: str, params: dict[str, Any] | None = None):
    if name not in QUERY_DEFINITIONS:
        raise KeyError(f"Unknown query: {name}")
    params = params or {}
    cfg = QUERY_DEFINITIONS[name]
    cursor = (
        db[COLLECTION_VALIDATED]
        .find(cfg["filter"](params), {"_id": 0})
        .sort(cfg["sort"])
        .limit(int(params.get("limit", cfg["limit"])))
    )
    return list(cursor)


def explain_query(db, name: str, params: dict[str, Any] | None = None):
    if name not in QUERY_DEFINITIONS:
        raise KeyError(f"Unknown query: {name}")
    params = params or {}
    cfg = QUERY_DEFINITIONS[name]
    cursor = db[COLLECTION_VALIDATED].find(cfg["filter"](params)).sort(cfg["sort"]).limit(cfg["limit"])
    return cursor.explain("executionStats")


AGGREGATION_DEFINITIONS = {
    "sales_by_city": {
        "description": "إجمالي المبيعات وعدد الطلبات حسب المدينة",
        "pipeline": [
            {"$group": {"_id": "$city", "orders": {"$sum": 1}, "sales": {"$sum": "$total_amount"}}},
            {"$sort": {"sales": -1}},
            {"$project": {"_id": 0, "city": "$_id", "orders": 1, "sales": 1}},
        ],
    },
    "top_products": {
        "description": "أفضل المنتجات حسب الكمية والمبيعات",
        "pipeline": [
            {"$unwind": "$items"},
            {"$group": {"_id": {"sku": "$items.sku", "name": "$items.name"}, "quantity": {"$sum": "$items.qty"}, "sales": {"$sum": "$items.total"}}},
            {"$sort": {"sales": -1}},
            {"$limit": 20},
            {"$project": {"_id": 0, "sku": "$_id.sku", "name": "$_id.name", "quantity": 1, "sales": 1}},
        ],
    },
    "top_customers": {
        "description": "أفضل العملاء حسب قيمة المبيعات",
        "pipeline": [
            {"$group": {"_id": "$customer_id", "orders": {"$sum": 1}, "sales": {"$sum": "$total_amount"}}},
            {"$sort": {"sales": -1}},
            {"$limit": 20},
            {"$project": {"_id": 0, "customer_id": "$_id", "orders": 1, "sales": 1}},
        ],
    },
    "sales_by_period": {
        "description": "المبيعات حسب الشهر",
        "pipeline": [
            {"$set": {"order_date_dt": {"$convert": {"input": "$order_date", "to": "date", "onError": None, "onNull": None}}}},
            {"$match": {"order_date_dt": {"$ne": None}}},
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$order_date_dt"}}, "orders": {"$sum": 1}, "sales": {"$sum": "$total_amount"}}},
            {"$sort": {"_id": 1}},
            {"$project": {"_id": 0, "period": "$_id", "orders": 1, "sales": 1}},
        ],
    },
    "orders_by_status": {
        "description": "توزيع الطلبات حسب الحالة",
        "pipeline": [
            {"$group": {"_id": "$status", "orders": {"$sum": 1}, "sales": {"$sum": "$total_amount"}}},
            {"$sort": {"orders": -1}},
            {"$project": {"_id": 0, "status": "$_id", "orders": 1, "sales": 1}},
        ],
    },
}


def list_aggregation_names():
    return [
        {"name": name, "description": cfg["description"]}
        for name, cfg in AGGREGATION_DEFINITIONS.items()
    ]


def run_aggregation(db, name: str):
    if name not in AGGREGATION_DEFINITIONS:
        raise KeyError(f"Unknown aggregation: {name}")
    return list(db[COLLECTION_VALIDATED].aggregate(AGGREGATION_DEFINITIONS[name]["pipeline"]))


def _safe_date(doc):
    value = doc.get("order_date")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return _parse_dt(value)
        except ValueError:
            return None
    return None


def _contributions(doc):
    """Return additive contributions used by the incremental MV ledger."""
    date = _safe_date(doc)
    if not date:
        return None
    day = date.strftime("%Y-%m-%d")
    sales = float(doc.get("total_amount") or 0)
    products = []
    for item in doc.get("items") or []:
        products.append({
            "sku": str(item.get("sku", "")),
            "name": str(item.get("name", "")),
            "quantity": float(item.get("qty") or 0),
            "sales": float(item.get("total") or 0),
        })
    return {"day": day, "sales": sales, "products": products}


def _apply_contribution(db, contribution, sign: int):
    if not contribution:
        return
    db[MV_DAILY_SALES].update_one(
        {"date": contribution["day"]},
        {"$inc": {"orders": sign, "sales": sign * contribution["sales"]}, "$set": {"updated_at": datetime.now(timezone.utc)}},
        upsert=True,
    )
    for product in contribution["products"]:
        db[MV_TOP_PRODUCTS].update_one(
            {"sku": product["sku"]},
            {"$inc": {"quantity": sign * product["quantity"], "sales": sign * product["sales"]}, "$set": {"name": product["name"], "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )


def refresh_materialized_views(db) -> dict[str, Any]:
    """Incrementally refresh both materialized summaries using a source ledger."""
    ledger = db[MV_LEDGER]
    state = db[MV_STATE].find_one({"_id": "materialized_views"}) or {"_id": "materialized_views"}
    last_run = state.get("last_refresh_at")

    # The ledger is the idempotency boundary: same order_id + same record_hash is skipped.
    query = {}
    if last_run:
        query = {"last_updated_at": {"$gte": last_run}}

    processed = inserted = updated = unchanged = 0
    cursor = db[COLLECTION_VALIDATED].find(query, {"_id": 0})
    for doc in cursor:
        processed += 1
        order_id = doc.get("order_id")
        record_hash = doc.get("record_hash")
        if not order_id:
            continue
        old = ledger.find_one({"order_id": order_id})
        if old and old.get("record_hash") == record_hash:
            unchanged += 1
            continue
        if old:
            _apply_contribution(db, old.get("contribution"), -1)
            updated += 1
        else:
            inserted += 1
        contribution = _contributions(doc)
        _apply_contribution(db, contribution, +1)
        ledger.update_one(
            {"order_id": order_id},
            {"$set": {"record_hash": record_hash, "contribution": contribution, "updated_at": datetime.now(timezone.utc)}},
            upsert=True,
        )

    now = datetime.now(timezone.utc)
    db[MV_STATE].update_one(
        {"_id": "materialized_views"},
        {"$set": {"last_refresh_at": now, "last_result": {"processed": processed, "inserted": inserted, "updated": updated, "unchanged": unchanged}}},
        upsert=True,
    )
    # Remove zero/negative artifacts caused by reversing updates.
    db[MV_DAILY_SALES].delete_many({"orders": {"$lte": 0}})
    db[MV_TOP_PRODUCTS].delete_many({"quantity": {"$lte": 0}})
    return {"processed": processed, "inserted": inserted, "updated": updated, "unchanged": unchanged, "refreshed_at": now.isoformat()}


def materialized_view_names():
    return [MV_DAILY_SALES, MV_TOP_PRODUCTS]
