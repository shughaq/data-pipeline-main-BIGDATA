"""Five independent aggregation reports for Phase 2."""
from __future__ import annotations

from typing import Any

from config.settings import COLLECTION_VALIDATED


REPORT_DEFINITIONS = {
    "sales_by_city": "Total sales and order count by city.",
    "top_products": "Top products by quantity and sales using the items array.",
    "top_customers": "Top customers by total sales.",
    "sales_by_period": "Monthly sales totals and order counts.",
    "orders_by_status": "Order distribution by status.",
}


def _run(db, name: str, pipeline: list[dict]) -> dict:
    if name not in REPORT_DEFINITIONS:
        raise KeyError(f"Unknown aggregation: {name}")
    results = list(db[COLLECTION_VALIDATED].aggregate(pipeline, allowDiskUse=True))
    for doc in results:
        doc.pop("_id", None)
    return {"name": name, "description": REPORT_DEFINITIONS[name], "count": len(results), "results": results}


def sales_by_city(db):
    return _run(db, "sales_by_city", [
        {"$group": {"_id": "$city", "orders": {"$sum": 1}, "sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}}},
        {"$sort": {"sales": -1}},
        {"$limit": 100},
        {"$project": {"_id": 0, "city": "$_id", "orders": 1, "sales": 1}},
    ])


def top_products(db):
    return _run(db, "top_products", [
        {"$unwind": "$items"},
        {"$group": {
            "_id": {"sku": "$items.sku", "name": "$items.name"},
            "quantity": {"$sum": {"$ifNull": ["$items.qty", 0]}},
            "sales": {"$sum": {"$ifNull": ["$items.total", 0]}},
        }},
        {"$sort": {"sales": -1}},
        {"$limit": 100},
        {"$project": {"_id": 0, "sku": "$_id.sku", "product": "$_id.name", "quantity": 1, "sales": 1}},
    ])


def top_customers(db):
    return _run(db, "top_customers", [
        {"$group": {"_id": "$customer_id", "orders": {"$sum": 1}, "sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}}},
        {"$sort": {"sales": -1}},
        {"$limit": 100},
        {"$project": {"_id": 0, "customer_id": "$_id", "orders": 1, "sales": 1}},
    ])


def sales_by_period(db):
    return _run(db, "sales_by_period", [
        {"$set": {"order_date_value": {"$convert": {"input": "$order_date", "to": "date", "onError": None, "onNull": None}}}},
        {"$match": {"order_date_value": {"$ne": None}}},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m", "date": "$order_date_value"}},
            "orders": {"$sum": 1},
            "sales": {"$sum": {"$ifNull": ["$total_amount", 0]}},
        }},
        {"$sort": {"_id": 1}},
        {"$project": {"_id": 0, "period": "$_id", "orders": 1, "sales": 1}},
    ])


def orders_by_status(db):
    return _run(db, "orders_by_status", [
        {"$group": {"_id": "$status", "orders": {"$sum": 1}, "sales": {"$sum": {"$ifNull": ["$total_amount", 0]}}}},
        {"$sort": {"orders": -1}},
        {"$project": {"_id": 0, "status": "$_id", "orders": 1, "sales": 1}},
    ])


REPORT_RUNNERS = {
    "sales_by_city": sales_by_city,
    "top_products": top_products,
    "top_customers": top_customers,
    "sales_by_period": sales_by_period,
    "orders_by_status": orders_by_status,
}


def list_aggregations():
    return [{"name": name, "description": description} for name, description in REPORT_DEFINITIONS.items()]


def run_aggregation(db, name: str):
    runner = REPORT_RUNNERS.get(name)
    if runner is None:
        raise KeyError(f"Unknown aggregation: {name}")
    return runner(db)
