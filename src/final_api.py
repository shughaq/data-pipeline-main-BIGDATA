"""Unified FastAPI control surface for Phase 2.

This is only an API facade. It calls the existing midterm pipeline functions
rather than implementing a second ingestion pipeline.
"""

from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient

from config.settings import DB_NAME, MONGODB_URI

from src.batch_loader import run_batch_load
from src.file_router import decide_engine

from src.final_aggregations import (
    list_aggregations,
    run_aggregation,
)

from src.final_jobs import (
    list_jobs,
    run_job,
    SimpleDailyScheduler,
)

from src.final_materialized_views import refresh

from src.final_queries import (
    explain_queries,
    list_queries,
    run_query,
)

from src.metrics import RunMetrics
from src.mongo_setup import ensure_collections


# ============================================================
# FASTAPI APP
# ============================================================

app = FastAPI(
    title="Big Data Phase 2 API",
    version="2.0",
)


# ============================================================
# SCHEDULER
# ============================================================

_scheduler = SimpleDailyScheduler()


# ============================================================
# REQUEST MODELS
# ============================================================

class IngestRequest(BaseModel):
    input: str
    threshold_mb: float | None = None
    batch_size: int | None = None


# ============================================================
# STARTUP / SHUTDOWN
# ============================================================

@app.on_event("startup")
def startup():
    _scheduler.start()


@app.on_event("shutdown")
def shutdown():
    _scheduler.stop()


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    client = MongoClient(MONGODB_URI)

    try:
        client.admin.command("ping")

        return {
            "status": "ok",
            "mongodb": True,
            "database": DB_NAME,
        }

    finally:
        client.close()


# ============================================================
# INGEST
# ============================================================

@app.post("/ingest")
def ingest(request: IngestRequest):
    """
    Uses the existing midterm router + batch pipeline.
    No new ingestion pipeline is created.
    """

    path = Path(request.input)

    if not path.exists():
        raise HTTPException(
            status_code=404,
            detail=f"Input file not found: {path}",
        )

    decision = decide_engine(
        path,
        threshold_mb=request.threshold_mb,
    )

    client = MongoClient(MONGODB_URI)
    spark_session = None

    try:
        db = client[DB_NAME]

        # Existing midterm collection setup
        ensure_collections(db)

        id_run = str(uuid.uuid4())

        metrics = RunMetrics(
            id_run=id_run,
            file_name=str(path),
            file_size_mb=decision["file_size_mb"],
            used_engine=decision["engine"],
        )

        # ----------------------------------------------------
        # Python Batch
        # ----------------------------------------------------

        if decision["engine"] == "python_batch":

            run_batch_load(
                path,
                db,
                id_run,
                metrics,
                batch_size=request.batch_size,
            )

        # ----------------------------------------------------
        # PySpark
        # ----------------------------------------------------

        else:

            from src.spark_loader import (
                build_spark_session,
                run_spark_load,
            )

            spark_session = build_spark_session()

            run_spark_load(
                path,
                spark_session,
                id_run,
                metrics,
            )

        metrics.finalize()

        return metrics.to_dict()

    finally:

        if spark_session is not None:
            spark_session.stop()

        client.close()


# ============================================================
# INDEXES
# ============================================================

@app.post("/indexes")
def indexes():
    """
    Returns the indexes that already exist on orders_validated.

    Phase 2 must use the existing midterm database and collections.
    Therefore this endpoint does not recreate existing indexes.
    """

    client = MongoClient(MONGODB_URI)

    try:
        collection = client[DB_NAME]["orders_validated"]

        existing_indexes = collection.index_information()

        result = []

        for name, info in existing_indexes.items():

            result.append(
                {
                    "name": name,
                    "keys": list(info.get("key", [])),
                    "unique": info.get("unique", False),
                }
            )

        return {
            "collection": "orders_validated",
            "count": len(result),
            "indexes": result,
        }

    finally:
        client.close()


# ============================================================
# QUERIES - LIST
# ============================================================

@app.get("/queries")
def queries():
    return {
        "queries": list_queries()
    }


# ============================================================
# QUERY - RUN ONE
# ============================================================

@app.get("/queries/{name}")
def query(name: str):

    client = MongoClient(MONGODB_URI)

    try:

        try:

            return run_query(
                client[DB_NAME],
                name,
            )

        except KeyError as exc:

            raise HTTPException(
                status_code=404,
                detail=str(exc),
            )

    finally:
        client.close()


# ============================================================
# EXPLAIN
# ============================================================

@app.get("/explain")
def explain():

    client = MongoClient(MONGODB_URI)

    try:

        return {
            "explain": explain_queries(
                client[DB_NAME]
            )
        }

    finally:
        client.close()


# ============================================================
# AGGREGATIONS - LIST
# ============================================================

@app.get("/aggregations")
def aggregations():

    return {
        "aggregations": list_aggregations()
    }


# ============================================================
# AGGREGATION - RUN ONE
# ============================================================

@app.get("/aggregations/{name}")
def aggregation(name: str):

    client = MongoClient(MONGODB_URI)

    try:

        try:

            return run_aggregation(
                client[DB_NAME],
                name,
            )

        except KeyError as exc:

            raise HTTPException(
                status_code=404,
                detail=str(exc),
            )

    finally:
        client.close()


# ============================================================
# MATERIALIZED VIEWS
# ============================================================

@app.post("/refresh-mv")
def refresh_mv(mode: str = "incremental"):

    if mode not in {
        "incremental",
        "full",
    }:

        raise HTTPException(
            status_code=400,
            detail="mode must be incremental or full",
        )

    client = MongoClient(MONGODB_URI)

    try:

        return refresh(
            client[DB_NAME],
            mode=mode,
        )

    finally:
        client.close()


# ============================================================
# JOBS - LIST
# ============================================================

@app.get("/jobs")
def jobs():

    client = MongoClient(MONGODB_URI)

    try:

        return {
            "jobs": list_jobs(
                client[DB_NAME]
            ),
            "schedule_utc": {
                "refresh_daily_sales": "00:10",
                "refresh_top_products": "00:20",
            },
        }

    finally:
        client.close()


# ============================================================
# JOB - RUN MANUALLY
# ============================================================

@app.post("/jobs/{name}/run")
def run_scheduled_job(name: str):

    try:

        return run_job(name)

    except KeyError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc),
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )