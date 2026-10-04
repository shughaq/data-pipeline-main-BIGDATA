"""Reusable ingestion gateway shared by CLI and FastAPI."""
from __future__ import annotations

import uuid
from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME
from src.file_router import decide_engine
from src.mongo_setup import ensure_collections
from src.metrics import RunMetrics, append_run_to_results_file
from src.batch_loader import run_batch_load
from src.incremental_loader import run_incremental_load


def run_pipeline(input_path: str, threshold_mb=None, batch_size=None, stage="normal"):
    id_run = str(uuid.uuid4())
    decision = decide_engine(input_path, threshold_mb=threshold_mb)
    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]
    metrics = RunMetrics(
        id_run=id_run,
        file_name=decision["file_path"],
        file_size_mb=decision["file_size_mb"],
        used_engine=decision["engine"] if stage == "normal" else f"incremental_{stage}",
    )
    spark_session = None
    try:
        ensure_collections(db)
        if stage in ("initial", "delta"):
            run_incremental_load(input_path, db, id_run, metrics, stage_label=stage, batch_size=batch_size or 1000)
        elif decision["engine"] == "python_batch":
            run_batch_load(input_path, db, id_run, metrics, batch_size=batch_size)
        else:
            from src.spark_loader import build_spark_session, run_spark_load
            spark_session = build_spark_session()
            run_spark_load(input_path, spark_session, id_run, metrics)
        metrics.finalize()
        result = metrics.to_dict()
        append_run_to_results_file(result)
        return result
    finally:
        if spark_session is not None:
            spark_session.stop()
        client.close()
