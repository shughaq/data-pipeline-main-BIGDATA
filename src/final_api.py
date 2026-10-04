"""
Unified FastAPI control surface for Phase 2.

This API is only a control interface.
It uses the existing midterm pipeline and
the same MongoDB database/collections.

Phase 2 endpoints:
    GET  /health
    POST /ingest
    POST /indexes
    GET  /queries
    GET  /queries/{name}
    GET  /explain
    GET  /aggregations
    GET  /aggregations/{name}
    POST /refresh-mv
    GET  /jobs
    POST /jobs/{name}/run
"""

from __future__ import annotations

import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pymongo import MongoClient

from config.settings import (
    DB_NAME,
    MONGODB_URI,
)

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
    create_indexes,
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
    description=(
        "Unified API for the Big Data final project. "
        "Uses the existing midterm pipeline, MongoDB collections, "
        "queries, indexes, aggregations, materialized views and jobs."
    ),
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
    """
    Start the internal daily scheduler when FastAPI starts.
    """
    _scheduler.start()


@app.on_event("shutdown")
def shutdown():
    """
    Stop the scheduler when FastAPI shuts down.
    """
    _scheduler.stop()


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    """
    Check API and MongoDB availability.
    """

    client = MongoClient(MONGODB_URI)

    try:
        client.admin.command("ping")

        return {
            "status": "ok",
            "mongodb": True,
            "database": DB_NAME,
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"MongoDB connection failed: {exc}",
        )

    finally:
        client.close()


# ============================================================
# INGEST
# ============================================================

@app.post("/ingest")
def ingest(request: IngestRequest):
    """
    Run the existing midterm ingestion pipeline.

    The API does NOT create a new ingestion pipeline.

    It uses:
        File Router
        Python Batch
        PySpark
        MongoDB
        Existing ELT pipeline
    """

    path = Path(request.input)

    if not path.exists():

        raise HTTPException(
            status_code=404,
            detail=f"Input file not found: {path}",
        )

    # --------------------------------------------------------
    # Decide engine using the existing File Router
    # --------------------------------------------------------

    try:

        decision = decide_engine(
            path,
            threshold_mb=request.threshold_mb,
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"File Router failed: {exc}",
        )

    client = MongoClient(MONGODB_URI)
    spark_session = None

    try:

        db = client[DB_NAME]

        # ----------------------------------------------------
        # Existing midterm MongoDB collection setup
        # ----------------------------------------------------

        ensure_collections(db)

        # ----------------------------------------------------
        # Create unique run ID
        # ----------------------------------------------------

        id_run = str(uuid.uuid4())

        metrics = RunMetrics(
            id_run=id_run,
            file_name=str(path),
            file_size_mb=decision["file_size_mb"],
            used_engine=decision["engine"],
        )

        # ====================================================
        # PYTHON BATCH
        # ====================================================

        if decision["engine"] == "python_batch":

            run_batch_load(
                path,
                db,
                id_run,
                metrics,
                batch_size=request.batch_size,
            )

        # ====================================================
        # PYSPARK
        # ====================================================

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

        # ----------------------------------------------------
        # Finalize metrics
        # ----------------------------------------------------

        metrics.finalize()

        return metrics.to_dict()

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Ingestion failed: {exc}",
        )

    finally:

        if spark_session is not None:

            try:
                spark_session.stop()
            except Exception:
                pass

        client.close()


# ============================================================
# INDEXES
# ============================================================

@app.post("/indexes")
def indexes():
    """
    Create the required Phase 2 indexes on orders_validated.

    The required indexes are defined in:
        src/final_queries.py

    The function create_indexes(db) checks whether an equivalent
    index already exists before creating a new one.

    This endpoint therefore:
        1. Connects to the existing database.
        2. Creates the required indexes if missing.
        3. Reads the indexes after creation.
        4. Returns the final index information.
    """

    client = MongoClient(MONGODB_URI)

    try:

        db = client[DB_NAME]

        # ----------------------------------------------------
        # IMPORTANT:
        # Actually CREATE the required indexes.
        # ----------------------------------------------------

        created_indexes = create_indexes(db)

        # ----------------------------------------------------
        # Read indexes after creation
        # ----------------------------------------------------

        collection = db["orders_validated"]

        existing_indexes = collection.index_information()

        result = []

        for name, info in existing_indexes.items():

            result.append(
                {
                    "name": name,
                    "keys": [
                        list(key)
                        for key in info.get("key", [])
                    ],
                    "unique": info.get(
                        "unique",
                        False,
                    ),
                }
            )

        return {
            "status": "success",
            "message": "Indexes created/verified successfully",
            "database": DB_NAME,
            "collection": "orders_validated",

            # The indexes required by Phase 2
            "created_indexes": [
                {
                    "name": item["name"],
                    "keys": [
                        list(key)
                        for key in item["keys"]
                    ],
                    "purpose": item["purpose"],
                }
                for item in created_indexes
            ],

            # All indexes currently existing
            "count": len(result),
            "indexes": result,
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Index creation failed: {exc}",
        )

    finally:

        client.close()


# ============================================================
# QUERIES - LIST
# ============================================================

@app.get("/queries")
def queries():
    """
    Return the list of available Phase 2 queries.
    """

    return {
        "queries": list_queries()
    }


# ============================================================
# QUERY - RUN ONE
# ============================================================

@app.get("/queries/{name}")
def query(name: str):
    """
    Execute one predefined query.
    """

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

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Query failed: {exc}",
        )

    finally:

        client.close()


# ============================================================
# EXPLAIN
# ============================================================

@app.get("/explain")
def explain():
    """
    Return executionStats/explain information
    for the Phase 2 queries.
    """

    client = MongoClient(MONGODB_URI)

    try:

        return {
            "status": "success",
            "explain": explain_queries(
                client[DB_NAME]
            ),
        }

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Explain failed: {exc}",
        )

    finally:

        client.close()


# ============================================================
# AGGREGATIONS - LIST
# ============================================================

@app.get("/aggregations")
def aggregations():
    """
    Return all available aggregation reports.
    """

    return {
        "aggregations": list_aggregations()
    }


# ============================================================
# AGGREGATION - RUN ONE
# ============================================================

@app.get("/aggregations/{name}")
def aggregation(name: str):
    """
    Execute one aggregation report.
    """

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

    except HTTPException:
        raise

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Aggregation failed: {exc}",
        )

    finally:

        client.close()


# ============================================================
# MATERIALIZED VIEWS
# ============================================================

@app.post("/refresh-mv")
def refresh_mv(
    mode: str = "incremental",
):
    """
    Refresh the two Materialized Views.

    Supported modes:
        incremental
        full
    """

    if mode not in {
        "incremental",
        "full",
    }:

        raise HTTPException(
            status_code=400,
            detail=(
                "Invalid mode. "
                "mode must be 'incremental' or 'full'."
            ),
        )

    client = MongoClient(MONGODB_URI)

    try:

        return refresh(
            client[DB_NAME],
            mode=mode,
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Materialized View refresh failed: {exc}",
        )

    finally:

        client.close()


# ============================================================
# JOBS - LIST
# ============================================================

@app.get("/jobs")
def jobs():
    """
    Return scheduled jobs and their schedules.
    """

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

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=f"Unable to list jobs: {exc}",
        )

    finally:

        client.close()


# ============================================================
# JOB - RUN MANUALLY
# ============================================================

@app.post("/jobs/{name}/run")
def run_scheduled_job(name: str):
    """
    Run one scheduled job manually.

    This endpoint is required so the professor can test
    each scheduled job during the demonstration.
    """

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
            detail=f"Scheduled job failed: {exc}",
        )