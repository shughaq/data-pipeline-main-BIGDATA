"""Phase 2 queries, indexes and explain plans.

Uses the existing orders_validated collection from the midterm project.
No new source dataset is created.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from pymongo import ASCENDING, DESCENDING

from config.settings import COLLECTION_VALIDATED


QUERY_DEFINITIONS = {
    "sales_by_city": {
        "description": "Orders in a city, sorted by newest order date.",
        "filter": {"city": "صنعاء"},
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
    },
    "delivered_by_date": {
        "description": "Delivered orders in a date range, newest first.",
        "filter": {
            "status": "تم التسليم",
            "order_date": {"$gte": "2025-01-01", "$lt": "2026-01-01"},
        },
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
    },
    "customer_orders": {
        "description": "All orders for one customer.",
        "filter": {"customer_id": "عميل-16541874"},
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
    },
    "high_value_orders": {
        "description": "High-value orders, newest first.",
        "filter": {"total_amount": {"$gte": 100000}},
        "sort": [("total_amount", DESCENDING)],
        "limit": 20,
    },
    "pending_card_orders": {
        "description": "Pending card orders in a city, newest first.",
        "filter": {"payment_status": "بانتظار الدفع", "payment_method": "بطاقة", "city": "صنعاء"},
        "sort": [("order_date", DESCENDING)],
        "limit": 20,
    },
}

INDEX_DEFINITIONS = [
    {
        "name": "idx_city_order_date",
        "keys": [("city", ASCENDING), ("order_date", DESCENDING)],
        "purpose": "Supports city filtering and newest-first date ordering.",
    },
    {
        "name": "idx_status_order_date",
        "keys": [("status", ASCENDING), ("order_date", DESCENDING)],
        "purpose": "Supports status filtering and date sorting.",
    },
    {
        "name": "idx_customer_id",
        "keys": [("customer_id", ASCENDING)],
        "purpose": "Supports direct customer order lookup.",
    },
]


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _serialize(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_serialize(v) for v in value]
    return value


def list_queries() -> list[dict]:
    return [
        {"name": name, "description": definition["description"]}
        for name, definition in QUERY_DEFINITIONS.items()
    ]


def run_query(db, name: str) -> dict:
    if name not in QUERY_DEFINITIONS:
        raise KeyError(f"Unknown query: {name}")
    spec = QUERY_DEFINITIONS[name]
    cursor = db[COLLECTION_VALIDATED].find(spec["filter"])
    if spec.get("sort"):
        cursor = cursor.sort(spec["sort"])
    if spec.get("limit"):
        cursor = cursor.limit(spec["limit"])
    docs = list(cursor)
    return {
        "name": name,
        "description": spec["description"],
        "count_returned": len(docs),
        "results": _serialize(docs),
    }


def create_indexes(db) -> list[dict]:
    collection = db[COLLECTION_VALIDATED]
    existing = collection.index_information()
    created = []

    for spec in INDEX_DEFINITIONS:
        expected_keys = [tuple(key) for key in spec["keys"]]
        found_name = None

        for index_name, index_info in existing.items():
            actual_keys = [
                tuple(key) for key in index_info.get("key", [])
            ]

            if actual_keys == expected_keys:
                found_name = index_name
                break

        if found_name:
            name = found_name
        else:
            name = collection.create_index(
                spec["keys"],
                name=spec["name"]
            )

        created.append({
            "name": name,
            "keys": spec["keys"],
            "purpose": spec["purpose"],
        })

    return created

def _explain(
    db,
    spec: dict,
    hint: dict | None = None,
) -> dict:

    command = {
        "find": COLLECTION_VALIDATED,
        "filter": spec["filter"],
        "sort": dict(spec.get("sort", [])),
        "limit": spec.get("limit", 20),
    }

    # Before = force a collection scan.
    # This gives us a real no-index baseline without deleting
    # the existing project indexes.
    if hint is not None:
        command["hint"] = hint

    return db.command(
        "explain",
        command,
        verbosity="executionStats",
    )


def explain_queries(
    db,
    names: list[str] | None = None,
) -> list[dict]:

    names = names or [
        "sales_by_city",
        "delivered_by_date",
        "customer_orders",
    ]

    report = []

    # -------------------------------------------------
    # BEFORE
    # -------------------------------------------------
    # Force COLLSCAN using MongoDB's natural order.
    # This is our real "before indexes" baseline.
    before_plans = {}

    for name in names:

        spec = QUERY_DEFINITIONS[name]

        before_plans[name] = _explain(
            db,
            spec,
            hint={"$natural": 1},
        )

    # -------------------------------------------------
    # AFTER
    # -------------------------------------------------
    # Existing indexes are reused if their key pattern
    # already exists; otherwise they are created.
    create_indexes(db)

    for name in names:

        spec = QUERY_DEFINITIONS[name]

        before = before_plans[name]

        after = _explain(
            db,
            spec,
        )

        after_indexes = _index_names_after(
            after
        )

        report.append(
            {
                "name": name,

                "before": _explain_summary(
                    before
                ),

                "after": _explain_summary(
                    after
                ),

                "index_reason": next(
                    (
                        x["purpose"]
                        for x in INDEX_DEFINITIONS
                        if x["name"] in after_indexes
                    ),
                    "The selected index supports the filter/sort pattern of this query.",
                ),
            }
        )

    return report


def _index_names_after(
    explain: dict,
) -> set[str]:

    names: set[str] = set()

    def walk(node: Any):

        if isinstance(node, dict):

            if (
                node.get("stage") == "IXSCAN"
                and node.get("indexName")
            ):
                names.add(
                    node["indexName"]
                )

            for value in node.values():
                walk(value)

        elif isinstance(node, list):

            for value in node:
                walk(value)

    walk(
        explain.get(
            "queryPlanner",
            {},
        )
    )

    return names


def _plan_stages(
    explain: dict,
) -> list[str]:

    stages: list[str] = []

    def walk(node: Any):

        if isinstance(node, dict):

            stage = node.get("stage")

            if stage and stage not in stages:
                stages.append(stage)

            for value in node.values():
                walk(value)

        elif isinstance(node, list):

            for value in node:
                walk(value)

    walk(
        explain.get(
            "queryPlanner",
            {},
        )
    )

    return stages


def _explain_summary(
    explain: dict,
) -> dict:

    execution = explain.get(
        "executionStats",
        {},
    )

    planner = explain.get(
        "queryPlanner",
        {},
    )

    winning_plan = planner.get(
        "winningPlan",
        {},
    )

    stages = _plan_stages(
        explain
    )

    return {
        "winning_plan_stage": winning_plan.get(
            "stage"
        ),

        "plan_stages": stages,

        "scan_type": (
            "IXSCAN"
            if "IXSCAN" in stages
            else "COLLSCAN"
            if "COLLSCAN" in stages
            else "OTHER"
        ),

        "index_names": sorted(
            _index_names_after(
                explain
            )
        ),

        "execution_time_ms": execution.get(
            "executionTimeMillis"
        ),

        "total_keys_examined": execution.get(
            "totalKeysExamined"
        ),

        "total_docs_examined": execution.get(
            "totalDocsExamined"
        ),

        "n_returned": execution.get(
            "nReturned"
        ),
    }