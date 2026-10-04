"""Unified FastAPI execution/testing interface required by the final project."""
from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from pymongo import MongoClient

from config.settings import DB_NAME, MONGODB_URI
from src.final_features import (
    create_query_indexes,
    explain_query,
    list_aggregation_names,
    list_query_names,
    materialized_view_names,
    refresh_materialized_views,
    run_aggregation,
    run_query,
)
from src.jobs import list_jobs, recent_job_runs, run_job, start_scheduler
from src.mongo_setup import ensure_collections
from src.pipeline_service import run_pipeline

app = FastAPI(
    title="Hybrid ELT Final Project API",
    version="2.0",
    description="واجهة تشغيل موحدة لمتطلبات المرحلة الثانية: ingestion, indexes, queries, aggregations, materialized views and jobs.",
)


class IngestRequest(BaseModel):
    input: str = Field(..., description="مسار ملف CSV المتاح لنظام التقييم")
    threshold_mb: float | None = None
    batch_size: int | None = None
    stage: str = "normal"


class RefreshRequest(BaseModel):
    view: str | None = None


@app.on_event("startup")
def startup():
    client = MongoClient(MONGODB_URI)
    try:
        ensure_collections(client[DB_NAME])
    finally:
        client.close()
    global _scheduler_stop
    _scheduler_stop, _scheduler_thread = start_scheduler()


_scheduler_stop = None
_scheduler_thread = None


def _db_client():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME]


@app.get("/health")
def health():
    client, db = _db_client()
    try:
        ping = db.command("ping")
        return {"status": "ok", "mongodb": ping.get("ok") == 1}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MongoDB unavailable: {exc}")
    finally:
        client.close()


@app.post("/ingest")
def ingest(request: IngestRequest):
    if request.stage not in {"normal", "initial", "delta"}:
        raise HTTPException(status_code=400, detail="stage must be normal, initial or delta")
    try:
        return run_pipeline(request.input, request.threshold_mb, request.batch_size, request.stage)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/indexes")
def indexes():
    client, db = _db_client()
    try:
        names = create_query_indexes(db)
        return {"created_or_verified": names, "required_count": 3, "compound_index": "idx_city_status_date"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        client.close()


@app.get("/queries")
def queries():
    return {"queries": list_query_names()}


@app.get("/queries/{name}")
def query(
    name: str,
    city: str = "صنعاء",
    status: str = "تم التسليم",
    customer_id: str = "",
    min_total: float = 100000,
    start: str | None = None,
    end: str | None = None,
    limit: int = 20,
    explain: bool = False,
):
    client, db = _db_client()
    try:
        params: dict[str, Any] = {
            "city": city, "status": status, "customer_id": customer_id,
            "min_total": min_total, "start": start, "end": end, "limit": limit,
        }
        if explain:
            return {"query": name, "mode": "executionStats", "explain": explain_query(db, name, params)}
        return {"query": name, "rows": run_query(db, name, params)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    finally:
        client.close()


@app.get("/aggregations")
def aggregations():
    return {"aggregations": list_aggregation_names()}


@app.get("/aggregations/{name}")
def aggregation(name: str):
    client, db = _db_client()
    try:
        return {"aggregation": name, "rows": run_aggregation(db, name)}
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    finally:
        client.close()


@app.post("/refresh-mv")
def refresh_mv(request: RefreshRequest | None = None):
    client, db = _db_client()
    try:
        if request and request.view and request.view not in materialized_view_names():
            raise HTTPException(status_code=400, detail=f"Unknown view: {request.view}")
        return {"views": materialized_view_names(), "result": refresh_materialized_views(db)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        client.close()


@app.get("/jobs")
def jobs():
    return {"jobs": list_jobs(), "recent_runs": recent_job_runs()}


@app.post("/jobs/{name}/run")
def run_scheduled_job(name: str):
    if name not in {j["name"] for j in list_jobs()}:
        raise HTTPException(status_code=404, detail=f"Unknown job: {name}")
    return run_job(name)
