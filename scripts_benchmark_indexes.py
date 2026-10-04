"""Run explain("executionStats") for three queries before and after final indexes."""
from __future__ import annotations
import json
from pathlib import Path
from pymongo import MongoClient
from config.settings import MONGODB_URI, DB_NAME, REPORTS_DIR, COLLECTION_VALIDATED
from src.final_features import create_query_indexes, explain_query

REQUIRED_INDEXES = ["idx_city_status_date", "idx_customer_date", "idx_status_date", "idx_total_amount"]
CASES = [
    ("orders_by_city_status", {"city": "صنعاء", "status": "تم التسليم", "limit": 20}),
    ("customer_orders", {"customer_id": "CUST-001", "limit": 20}),
    ("orders_by_status", {"status": "تم التسليم", "limit": 20}),
]

def summarize(explain):
    stats = explain.get("executionStats", {})
    qp = explain.get("queryPlanner", {})
    return {
        "nReturned": stats.get("nReturned"),
        "executionTimeMillis": stats.get("executionTimeMillis"),
        "totalKeysExamined": stats.get("totalKeysExamined"),
        "totalDocsExamined": stats.get("totalDocsExamined"),
        "winningPlan": qp.get("winningPlan"),
    }

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
try:
    for idx in REQUIRED_INDEXES:
        try:
            db[COLLECTION_VALIDATED].drop_index(idx)
        except Exception:
            pass
    before = {name: summarize(explain_query(db, name, params)) for name, params in CASES}
    create_query_indexes(db)
    after = {name: summarize(explain_query(db, name, params)) for name, params in CASES}
    report = {"mode": "executionStats", "before_indexes": before, "after_indexes": after, "indexes": REQUIRED_INDEXES}
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / "index_explain_comparison.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
finally:
    client.close()
