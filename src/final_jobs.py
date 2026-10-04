"""Scheduled/manual jobs with execution logging."""

from __future__ import annotations

import json
import threading

from datetime import datetime, timezone

from config.settings import (
    DB_NAME,
    MONGODB_URI,
    INCREMENTAL_STATE_PATH,
)

from pymongo import MongoClient

from src.final_materialized_views import refresh


JOB_DEFINITIONS = {
    "refresh_daily_sales": {
        "description": "Incrementally refresh daily_sales_summary",
        "handler": "daily",
    },
    "refresh_top_products": {
        "description": "Incrementally refresh top_products_summary",
        "handler": "products",
    },
}


def _log_start(db, name: str):

    started = datetime.now(timezone.utc)

    result = db["job_runs"].insert_one(
        {
            "job_name": name,
            "started_at": started,
            "status": "running",
        }
    )

    return result.inserted_id, started


def _latest_delta_order_ids():

    if not INCREMENTAL_STATE_PATH.exists():
        return []

    try:

        with INCREMENTAL_STATE_PATH.open(
            encoding="utf-8"
        ) as f:

            state = json.load(f)

    except (
        OSError,
        json.JSONDecodeError,
    ):
        return []

    delta_runs = [
        run
        for run in state.get("runs", [])
        if run.get("stage") == "delta"
    ]

    if not delta_runs:
        return []

    latest = delta_runs[-1]

    return latest.get(
        "order_ids",
        [],
    )


def run_job(
    name: str,
    *,
    mode: str = "incremental",
) -> dict:

    if name not in JOB_DEFINITIONS:
        raise KeyError(
            f"Unknown job: {name}"
        )

    client = MongoClient(
        MONGODB_URI
    )

    try:

        db = client[DB_NAME]

        job_id, started = _log_start(
            db,
            name,
        )

        try:

            if mode == "incremental":

                order_ids = (
                    _latest_delta_order_ids()
                )

                result = refresh(
                    db,
                    mode="incremental",
                    order_ids=order_ids,
                )

            else:

                result = refresh(
                    db,
                    mode=mode,
                )

            finished = datetime.now(
                timezone.utc
            )

            db["job_runs"].update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "finished_at": finished,
                        "status": "success",
                        "result": result,
                    }
                },
            )

            return {
                "job_name": name,
                "status": "success",
                "started_at": started.isoformat(),
                "finished_at": finished.isoformat(),
                "result": result,
            }

        except Exception as exc:

            finished = datetime.now(
                timezone.utc
            )

            db["job_runs"].update_one(
                {"_id": job_id},
                {
                    "$set": {
                        "finished_at": finished,
                        "status": "failed",
                        "error": str(exc),
                    }
                },
            )

            raise

    finally:
        client.close()


def list_jobs(db) -> list[dict]:

    return [
        {
            "name": name,
            "description": value["description"],
        }
        for name, value in JOB_DEFINITIONS.items()
    ]


class SimpleDailyScheduler:
    """Dependency-free scheduler."""

    def __init__(
        self,
        schedule: dict[str, str] | None = None,
    ):

        self.schedule = schedule or {
            "refresh_daily_sales": "00:10",
            "refresh_top_products": "00:20",
        }

        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_run: dict[str, str] = {}

    def start(self):

        if (
            self._thread
            and self._thread.is_alive()
        ):
            return

        self._stop.clear()

        self._thread = threading.Thread(
            target=self._loop,
            name="phase2-scheduler",
            daemon=True,
        )

        self._thread.start()

    def stop(self):

        self._stop.set()

        if self._thread:
            self._thread.join(
                timeout=2
            )

    def _loop(self):

        while not self._stop.is_set():

            now = datetime.now(
                timezone.utc
            )

            minute = now.strftime(
                "%H:%M"
            )

            day_key = now.strftime(
                "%Y-%m-%d"
            )

            for name, at in self.schedule.items():

                if (
                    minute == at
                    and self._last_run.get(name)
                    != day_key
                ):

                    try:
                        run_job(name)

                    except Exception:
                        pass

                    self._last_run[name] = day_key

            self._stop.wait(30)