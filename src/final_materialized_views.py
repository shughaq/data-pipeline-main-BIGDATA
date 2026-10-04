"""Incrementally refreshed materialized summaries.

The source remains the existing orders_validated collection.

Initial refresh:
    Builds the materialized views once from orders_validated.

Incremental refresh:
    Uses only explicitly supplied order IDs.
    It does NOT scan the full orders_raw collection.

This keeps the existing midterm data intact and avoids
re-processing the 30M+ raw records on every refresh.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

from config.settings import COLLECTION_VALIDATED


DAILY_VIEW = "daily_sales_summary"
PRODUCT_VIEW = "top_products_summary"


def _to_date(value):
    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        try:
            return datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except ValueError:
            return None

    return None


def _ensure_views(db):
    existing = db.list_collection_names()

    if DAILY_VIEW not in existing:
        db.create_collection(DAILY_VIEW)

    db[DAILY_VIEW].create_index(
        [("day", 1)],
        unique=True,
    )

    if PRODUCT_VIEW not in existing:
        db.create_collection(PRODUCT_VIEW)

    db[PRODUCT_VIEW].create_index(
        [("sku", 1)],
        unique=True,
    )

    db[PRODUCT_VIEW].create_index(
        [("sales", -1)]
    )


def _refresh_daily_groups(
    db,
    affected_days: set[str],
):
    updated = 0

    for day in affected_days:

        pipeline = [
            {
                "$set": {
                    "order_date_value": {
                        "$convert": {
                            "input": "$order_date",
                            "to": "date",
                            "onError": None,
                            "onNull": None,
                        }
                    }
                }
            },
            {
                "$match": {
                    "order_date_value": {
                        "$ne": None
                    }
                }
            },
            {
                "$set": {
                    "day": {
                        "$dateToString": {
                            "format": "%Y-%m-%d",
                            "date": "$order_date_value",
                        }
                    }
                }
            },
            {
                "$match": {
                    "day": day
                }
            },
            {
                "$group": {
                    "_id": "$day",
                    "orders": {
                        "$sum": 1
                    },
                    "sales": {
                        "$sum": {
                            "$ifNull": [
                                "$total_amount",
                                0
                            ]
                        }
                    },
                }
            },
        ]

        rows = list(
            db[COLLECTION_VALIDATED].aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

        if rows:
            row = rows[0]

            db[DAILY_VIEW].replace_one(
                {"day": day},
                {
                    "day": day,
                    "orders": row["orders"],
                    "sales": row["sales"],
                    "updated_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )

            updated += 1

        else:
            db[DAILY_VIEW].delete_one(
                {"day": day}
            )

    return updated


def _refresh_product_groups(
    db,
    affected_skus: set[str],
):
    updated = 0

    for sku in affected_skus:

        pipeline = [
            {
                "$unwind": "$items"
            },
            {
                "$match": {
                    "items.sku": sku
                }
            },
            {
                "$group": {
                    "_id": {
                        "sku": "$items.sku",
                        "name": "$items.name",
                    },
                    "quantity": {
                        "$sum": {
                            "$ifNull": [
                                "$items.qty",
                                0
                            ]
                        }
                    },
                    "sales": {
                        "$sum": {
                            "$ifNull": [
                                "$items.total",
                                0
                            ]
                        }
                    },
                }
            },
        ]

        rows = list(
            db[COLLECTION_VALIDATED].aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

        if rows:
            row = rows[0]

            db[PRODUCT_VIEW].replace_one(
                {"sku": sku},
                {
                    "sku": sku,
                    "product": row["_id"].get("name"),
                    "quantity": row["quantity"],
                    "sales": row["sales"],
                    "updated_at": datetime.now(timezone.utc),
                },
                upsert=True,
            )

            updated += 1

        else:
            db[PRODUCT_VIEW].delete_one(
                {"sku": sku}
            )

    return updated


def full_refresh(db) -> dict:
    """One-time initial build from orders_validated.

    This does NOT read orders_raw and does NOT re-ingest data.
    """

    _ensure_views(db)

    # =============================
    # DAILY SALES
    # =============================

    daily = list(
        db[COLLECTION_VALIDATED].aggregate(
            [
                {
                    "$set": {
                        "order_date_value": {
                            "$convert": {
                                "input": "$order_date",
                                "to": "date",
                                "onError": None,
                                "onNull": None,
                            }
                        }
                    }
                },
                {
                    "$match": {
                        "order_date_value": {
                            "$ne": None
                        }
                    }
                },
                {
                    "$group": {
                        "_id": {
                            "$dateToString": {
                                "format": "%Y-%m-%d",
                                "date": "$order_date_value",
                            }
                        },
                        "orders": {
                            "$sum": 1
                        },
                        "sales": {
                            "$sum": {
                                "$ifNull": [
                                    "$total_amount",
                                    0
                                ]
                            }
                        },
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    db[DAILY_VIEW].delete_many({})

    if daily:
        db[DAILY_VIEW].insert_many(
            [
                {
                    "day": row["_id"],
                    "orders": row["orders"],
                    "sales": row["sales"],
                    "updated_at": datetime.now(timezone.utc),
                }
                for row in daily
            ]
        )

    # =============================
    # TOP PRODUCTS
    # =============================

    products = list(
        db[COLLECTION_VALIDATED].aggregate(
            [
                {
                    "$unwind": "$items"
                },
                {
                    "$group": {
                        "_id": {
                            "sku": "$items.sku",
                            "name": "$items.name",
                        },
                        "quantity": {
                            "$sum": {
                                "$ifNull": [
                                    "$items.qty",
                                    0
                                ]
                            }
                        },
                        "sales": {
                            "$sum": {
                                "$ifNull": [
                                    "$items.total",
                                    0
                                ]
                            }
                        },
                    }
                },
            ],
            allowDiskUse=True,
        )
    )

    db[PRODUCT_VIEW].delete_many({})

    valid_products = [
        {
            "sku": row["_id"].get("sku"),
            "product": row["_id"].get("name"),
            "quantity": row["quantity"],
            "sales": row["sales"],
            "updated_at": datetime.now(timezone.utc),
        }
        for row in products
        if row["_id"].get("sku")
    ]

    if valid_products:
        db[PRODUCT_VIEW].insert_many(valid_products)

    return {
        "mode": "full_initial_refresh",
        "daily_rows": len(daily),
        "product_rows": len(valid_products),
    }


def incremental_refresh(
    db,
    order_ids: Iterable[str] | None = None,
) -> dict:
    """Refresh only affected orders.

    IMPORTANT:
    This function never scans orders_raw.

    If order_ids are supplied, only those validated orders
    are examined.

    If no order_ids are supplied, the function safely returns
    without performing a huge full-data scan.
    """

    _ensure_views(db)

    if not order_ids:
        return {
            "mode": "incremental",
            "affected_orders": 0,
            "updated_days": 0,
            "updated_products": 0,
            "message": (
                "No affected order IDs were supplied. "
                "No full raw-data scan was performed."
            ),
        }

    order_ids = [
        str(order_id).strip()
        for order_id in order_ids
        if order_id
    ]

    order_ids = list(dict.fromkeys(order_ids))

    if not order_ids:
        return {
            "mode": "incremental",
            "affected_orders": 0,
            "updated_days": 0,
            "updated_products": 0,
        }

    affected_days: set[str] = set()
    affected_skus: set[str] = set()

    cursor = db[COLLECTION_VALIDATED].find(
        {
            "order_id": {
                "$in": order_ids
            }
        },
        {
            "order_date": 1,
            "items.sku": 1,
            "_id": 0,
        },
    )

    matched_orders = 0

    for row in cursor:

        matched_orders += 1

        dt = _to_date(
            row.get("order_date")
        )

        if dt:
            affected_days.add(
                dt.strftime("%Y-%m-%d")
            )

        for item in row.get("items") or []:

            sku = item.get("sku")

            if sku:
                affected_skus.add(
                    str(sku).strip()
                )

    updated_days = _refresh_daily_groups(
        db,
        affected_days,
    )

    updated_products = _refresh_product_groups(
        db,
        affected_skus,
    )

    return {
        "mode": "incremental",
        "affected_orders": matched_orders,
        "requested_order_ids": len(order_ids),
        "updated_days": updated_days,
        "updated_products": updated_products,
    }


def refresh(
    db,
    mode: str = "incremental",
    order_ids: Iterable[str] | None = None,
) -> dict:

    if mode == "full":
        return full_refresh(db)

    if mode == "incremental":
        return incremental_refresh(
            db,
            order_ids=order_ids,
        )

    raise ValueError(
        "mode must be 'incremental' or 'full'"
    )


def top_products_from_view(
    db,
    limit: int = 20,
) -> list[dict]:

    return list(
        db[PRODUCT_VIEW]
        .find(
            {},
            {"_id": 0}
        )
        .sort(
            "sales",
            -1
        )
        .limit(limit)
    )