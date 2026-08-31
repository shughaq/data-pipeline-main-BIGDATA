import argparse
import sys
import uuid

from pymongo import MongoClient

from config.settings import MONGODB_URI, DB_NAME, RESULTS_JSON_PATH
from src.file_router import decide_engine
from src.mongo_setup import ensure_collections
from src.metrics import RunMetrics, append_run_to_results_file
from src.batch_loader import run_batch_load
from src.incremental_loader import run_incremental_load


def parse_args():
    parser = argparse.ArgumentParser(description="خط البيانات الهجين - نقطة التشغيل الرئيسية")
    parser.add_argument("--input", required=True, help="مسار ملف CSV المراد معالجته")
    parser.add_argument("--threshold-mb", type=float, default=None,
                         help="تجاوز الحد الفاصل بين Python Batch وPySpark لهذا التشغيل فقط")
    parser.add_argument("--batch-size", type=int, default=None, help="حجم الدفعة لمحرك Python Batch")
    parser.add_argument(
        "--stage", choices=["normal", "initial", "delta"], default="normal",
        help="normal = تشغيل عادي حسب Router. initial/delta = مسار B التزايدي (يفرض Python Batch دائمًا)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    id_run = str(uuid.uuid4())

    print(f"===== بدء تشغيل جديد | id_run = {id_run} =====")

    decision = decide_engine(args.input, threshold_mb=args.threshold_mb)

    client = MongoClient(MONGODB_URI)
    db = client[DB_NAME]

    metrics = RunMetrics(
        id_run=id_run,
        file_name=decision["file_path"],
        file_size_mb=decision["file_size_mb"],
        used_engine=decision["engine"] if args.stage == "normal" else f"incremental_{args.stage}",
    )

    spark_session = None
    try:
        ensure_collections(db)

        if args.stage in ("initial", "delta"):
            # مسار B يفرض Python Batch عمدًا: الـDelta files صغيرة بطبيعتها
            run_incremental_load(args.input, db, id_run, metrics, stage_label=args.stage,
                                  batch_size=args.batch_size or 1000)
        elif decision["engine"] == "python_batch":
            run_batch_load(args.input, db, id_run, metrics, batch_size=args.batch_size)
        else:
            # نستورد pyspark فقط لو فعلاً محتاجينه (تجنّب كلفة إقلاع JVM على الملفات الصغيرة)
            from src.spark_loader import build_spark_session, run_spark_load
            spark_session = build_spark_session()
            run_spark_load(args.input, spark_session, id_run, metrics)

        metrics.finalize()
        result_dict = metrics.to_dict()

        ok, expected = metrics.consistency_check()
        if not ok:
            print(f"[main] تحذير: فشل اختبار الاتساق! loaded_raw={metrics.loaded_raw} "
                  f"لكن valid+corrected+quarantine={expected}")
        else:
            print("[main] اجتاز اختبار الاتساق (البند 6.11): raw = valid + corrected + quarantine [OK]")

        append_run_to_results_file(result_dict)

        print("===== ملخص التشغيل =====")
        for key in ("used_engine", "read_rows", "loaded_raw", "count_valid", "count_corrected",
                    "count_quarantine", "seconds_elapsed", "throughput_rows_per_sec",
                    "count_inserted", "count_updated", "count_unchanged"):
            print(f"  {key}: {result_dict[key]}")

    finally:
        # إغلاق سليم لكل الاتصالات (البند 9: try/finally لـSpark وMongo)
        if spark_session is not None:
            spark_session.stop()
            print("[main] تم إغلاق SparkSession.")
        client.close()
        print("[main] تم إغلاق اتصال MongoDB.")

    print(f"===== انتهى التشغيل | النتائج في {RESULTS_JSON_PATH} =====")


if __name__ == "__main__":
    sys.exit(main() or 0)
