import json
import time
from datetime import datetime, timezone

from config.settings import RESULTS_JSON_PATH


class RunMetrics:
    def __init__(self, id_run, file_name, file_size_mb, used_engine):
        self.id_run = id_run
        self.file_name = file_name
        self.file_size_mb = file_size_mb
        self.used_engine = used_engine

        self.read_rows = 0
        self.loaded_raw = 0
        self.count_valid = 0
        self.count_corrected = 0
        self.count_quarantine = 0

        self.counts_case_error = {}  # {error_code: count}

        self.count_inserted = 0
        self.count_updated = 0
        self.count_unchanged = 0

        self.partitions = None      # لملف PySpark
        self.size_batch = None      # لملف Python Batch

        self._start_time = time.perf_counter()
        self.seconds_elapsed = None
        self.throughput = None

    def add_error_codes(self, codes):
        for code in codes:
            self.counts_case_error[code] = self.counts_case_error.get(code, 0) + 1

    def finalize(self):
        self.seconds_elapsed = round(time.perf_counter() - self._start_time, 4)
        self.throughput = round(
            self.read_rows / self.seconds_elapsed, 2
        ) if self.seconds_elapsed > 0 else 0.0
        return self

    def consistency_check(self):
        """
        شرط القبول (البند 6.11):
        run_raw_count = run_valid_count + run_corrected_count + run_quarantine_count
        """
        expected = self.count_valid + self.count_corrected + self.count_quarantine
        ok = (self.loaded_raw == expected)
        return ok, expected

    def to_dict(self):
        ok, expected = self.consistency_check()
        return {
            "id_run": self.id_run,
            "file_name": self.file_name,
            "file_size_mb": self.file_size_mb,
            "used_engine": self.used_engine,
            "read_rows": self.read_rows,
            "loaded_raw": self.loaded_raw,
            "count_valid": self.count_valid,
            "count_corrected": self.count_corrected,
            "count_quarantine": self.count_quarantine,
            "seconds_elapsed": self.seconds_elapsed,
            "throughput_rows_per_sec": self.throughput,
            "partitions": self.partitions,
            "size_batch": self.size_batch,
            "counts_case_error": self.counts_case_error,
            "count_inserted": self.count_inserted,
            "count_updated": self.count_updated,
            "count_unchanged": self.count_unchanged,
            "consistency_check": {
                "raw_equals_valid_plus_corrected_plus_quarantine": ok,
                "loaded_raw": self.loaded_raw,
                "expected_from_sum": expected,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }


def append_run_to_results_file(run_metrics_dict, results_path=None):
    """
    يضيف نتيجة التشغيل الحالي إلى reports/results.json.
    الملف نفسه عبارة عن list من كل التشغيلات (عشان نقدر نقارن Python Batch
    بـPySpark، ونشوف تاريخ التشغيلات المتكررة لاختبار Idempotency).
    """
    results_path = results_path or RESULTS_JSON_PATH
    results_path.parent.mkdir(parents=True, exist_ok=True)

    history = []
    if results_path.exists():
        try:
            with results_path.open(encoding="utf-8") as f:
                history = json.load(f)
            if not isinstance(history, list):
                history = [history]
        except (json.JSONDecodeError, OSError):
            history = []

    history.append(run_metrics_dict)

    with results_path.open("w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)

    print(f"[Metrics] تم حفظ نتائج التشغيل في: {results_path}")
    return results_path
