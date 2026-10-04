import hashlib
import json
import time
from datetime import datetime, timezone
import csv

from config.settings import INCREMENTAL_STATE_PATH
from src.batch_loader import _flush_batch
from src.elt_pipeline import process_row
from src.console_ui import section, info, warning


def _file_hash(file_path):
    h = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(
            lambda: f.read(1024 * 1024),
            b"",
        ):
            h.update(chunk)

    return h.hexdigest()


def _load_state():
    if INCREMENTAL_STATE_PATH.exists():
        with INCREMENTAL_STATE_PATH.open(
            encoding="utf-8"
        ) as f:
            return json.load(f)

    return {"runs": []}


def _save_state(state):
    INCREMENTAL_STATE_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with INCREMENTAL_STATE_PATH.open(
        "w",
        encoding="utf-8",
    ) as f:
        json.dump(
            state,
            f,
            ensure_ascii=False,
            indent=2,
        )


def run_incremental_load(
    file_path,
    db,
    id_run,
    metrics,
    stage_label,
    batch_size=1000,
):
    """
    Incremental streaming loader.

    For Delta runs, the order IDs processed by this run
    are saved in the incremental state file so that
    materialized views can refresh only the affected orders.

    IMPORTANT:
    This does not scan orders_raw.
    """

    state = _load_state()

    file_hash = _file_hash(file_path)

    previous_run = next(
        (
            r
            for r in state["runs"]
            if r["file_hash"] == file_hash
        ),
        None,
    )

    if previous_run:
        warning(
            f"File already processed: {file_path} | "
            f"previous run={previous_run['id_run']} | "
            f"stage={previous_run['stage']} | "
            f"{previous_run['at']}. "
            f"Reapplying idempotent upsert."
        )
    else:
        info(
            "FILE",
            f"new incremental source: {file_path}",
        )

    section(
        f"INCREMENTAL • {stage_label.upper()}"
    )

    info(
        "RUN ID",
        id_run,
    )

    # IDs used to detect duplicates within this run.
    seen_order_ids = set()

    # IDs affected by the current Delta.
    # These are later consumed by the Materialized Views
    # incremental refresh and Scheduled Jobs.
    affected_order_ids = set()

    file_source = str(file_path)
    engine_used = f"incremental_{stage_label}"

    raw_docs = []
    validated_ops = []
    quarantine_docs = []

    batch_number = 0
    batch_start_time = time.perf_counter()

    with open(
        file_path,
        encoding="utf-8-sig",
        newline="",
    ) as f:

        reader = csv.DictReader(f)

        for row_number, row in enumerate(
            reader,
            start=1,
        ):

            metrics.read_rows += 1

            raw_doc, outcome = process_row(
                row,
                id_run,
                file_source,
                row_number,
                engine_used,
                seen_order_ids,
            )

            raw_docs.append(raw_doc)

            metrics.loaded_raw += 1

            # Track only IDs from Delta runs.
            # These IDs will be used later by the
            # incremental Materialized View refresh.
            if stage_label == "delta":

                order_id = row.get(
                    "order_id"
                )

                if order_id:

                    order_id = str(
                        order_id
                    ).strip()

                    if order_id:
                        affected_order_ids.add(
                            order_id
                        )

            if outcome["status"] == "quarantine":

                metrics.count_quarantine += 1

                metrics.add_error_codes(
                    outcome["error_codes"]
                )

                quarantine_docs.append(
                    outcome["quarantine_doc"]
                )

            else:

                if outcome["status"] == "corrected":

                    metrics.count_corrected += 1

                else:

                    metrics.count_valid += 1

                validated_ops.append(
                    outcome["validated_op"]
                )

            # Flush batches to MongoDB.
            if len(raw_docs) >= batch_size:

                batch_number += 1

                _flush_batch(
                    db,
                    id_run,
                    raw_docs,
                    validated_ops,
                    quarantine_docs,
                    batch_number,
                    batch_start_time,
                    metrics,
                )

                raw_docs = []
                validated_ops = []
                quarantine_docs = []

                batch_start_time = (
                    time.perf_counter()
                )

    # Flush the final incomplete batch.
    if raw_docs:

        batch_number += 1

        _flush_batch(
            db,
            id_run,
            raw_docs,
            validated_ops,
            quarantine_docs,
            batch_number,
            batch_start_time,
            metrics,
        )

    # -------------------------------------------------
    # Save execution state
    # -------------------------------------------------

    run_state = {
        "id_run": id_run,
        "stage": stage_label,
        "file": file_source,
        "file_hash": file_hash,
        "at": datetime.now(
            timezone.utc
        ).isoformat(),
        "count_inserted": metrics.count_inserted,
        "count_updated": metrics.count_updated,
        "count_unchanged": metrics.count_unchanged,
    }

    # Only Delta runs need affected IDs for
    # Materialized View incremental refresh.
    if stage_label == "delta":

        run_state["order_ids"] = sorted(
            affected_order_ids
        )

        run_state["order_ids_count"] = len(
            affected_order_ids
        )

    state["runs"].append(
        run_state
    )

    _save_state(state)

    # -------------------------------------------------
    # Console output
    # -------------------------------------------------

    section(
        "INCREMENTAL COMPLETE"
    )

    info(
        "STAGE",
        stage_label,
    )

    info(
        "UPSERT",
        f"inserted={metrics.count_inserted} | "
        f"updated={metrics.count_updated} | "
        f"unchanged={metrics.count_unchanged}",
    )

    if stage_label == "delta":

        info(
            "MV AFFECTED ORDERS",
            len(affected_order_ids),
        )

    return metrics