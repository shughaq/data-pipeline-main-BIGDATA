"""Two scheduled project jobs plus manual execution and execution logging."""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Callable

from pymongo import MongoClient

from config.settings import (
    DB_NAME,
    JOB_LOG_COLLECTION,
    MONGODB_URI,
    SCHEDULE_POLL_SECONDS,
    SCHEDULE_REFRESH_HOUR,
    SCHEDULE_REPORT_HOUR,
)
from src.final_features import refresh_materialized_views, run_aggregation


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "description": "تحديث daily_sales_summary و top_products_summary بشكل تزايدي",
        "schedule": f"يوميًا {SCHEDULE_REFRESH_HOUR:02d}:00",
    },
    "daily_reports": {
        "description": "إنشاء تقرير يومي من Aggregations وحفظه في reports/daily_reports.json",
        "schedule": f"يوميًا {SCHEDULE_REPORT_HOUR:02d}:00",
    },
}

_lock = threading.Lock()


def _client_db():
    client = MongoClient(MONGODB_URI)
    return client, client[DB_NAME]


def _log_job(db, name, started_at, ended_at, status, result=None, error=None):
    db[JOB_LOG_COLLECTION].insert_one({
        "job_name": name,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "result": result,
        "error": error,
    })


def _run_job(name: str):
    if name not in JOB_DEFINITIONS:
        raise KeyError(f"Unknown job: {name}")
    if not _lock.acquire(blocking=False):
        return {"status": "skipped", "reason": "another job is running"}
    started = datetime.now(timezone.utc)
    client = None
    try:
        client, db = _client_db()
        if name == "refresh_materialized_views":
            result = refresh_materialized_views(db)
        else:
            from pathlib import Path
            from datetime import date
            from config.settings import REPORTS_DIR
            report = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "sales_by_city": run_aggregation(db, "sales_by_city"),
                "top_products": run_aggregation(db, "top_products"),
                "top_customers": run_aggregation(db, "top_customers"),
                "sales_by_period": run_aggregation(db, "sales_by_period"),
                "orders_by_status": run_aggregation(db, "orders_by_status"),
            }
            REPORTS_DIR.mkdir(parents=True, exist_ok=True)
            path = REPORTS_DIR / "daily_reports.json"
            import json
            path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
            result = {"path": str(path), "reports": 5}
        ended = datetime.now(timezone.utc)
        _log_job(db, name, started, ended, "success", result=result)
        return {"job_name": name, "status": "success", "started_at": started.isoformat(), "ended_at": ended.isoformat(), "result": result}
    except Exception as exc:
        ended = datetime.now(timezone.utc)
        if client is not None:
            try:
                _log_job(db, name, started, ended, "failed", error=str(exc))
            except Exception:
                pass
        return {"job_name": name, "status": "failed", "started_at": started.isoformat(), "ended_at": ended.isoformat(), "error": str(exc)}
    finally:
        if client is not None:
            client.close()
        _lock.release()


def run_job(name: str):
    return _run_job(name)


def list_jobs():
    return [
        {"name": name, **definition}
        for name, definition in JOB_DEFINITIONS.items()
    ]


def recent_job_runs(limit=20):
    client, db = _client_db()
    try:
        docs = list(db[JOB_LOG_COLLECTION].find({}, {"_id": 0}).sort("started_at", -1).limit(limit))
        return docs
    finally:
        client.close()


def _scheduler_loop(stop_event: threading.Event):
    fired = set()
    while not stop_event.is_set():
        now = datetime.now()
        today = now.date().isoformat()
        targets = {
            "refresh_materialized_views": SCHEDULE_REFRESH_HOUR,
            "daily_reports": SCHEDULE_REPORT_HOUR,
        }
        for name, hour in targets.items():
            key = f"{today}:{name}"
            if now.hour == hour and now.minute == 0 and key not in fired:
                fired.add(key)
                threading.Thread(target=_run_job, args=(name,), daemon=True).start()
        # Keep memory bounded when the process runs for a long time.
        if len(fired) > 100:
            fired = {x for x in fired if x.startswith(today + ":")}
        stop_event.wait(SCHEDULE_POLL_SECONDS)


def start_scheduler():
    stop_event = threading.Event()
    thread = threading.Thread(target=_scheduler_loop, args=(stop_event,), daemon=True, name="project-scheduler")
    thread.start()
    return stop_event, thread
