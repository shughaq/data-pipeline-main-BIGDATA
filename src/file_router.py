import os

from config.settings import SMALL_FILE_THRESHOLD_MB


def get_file_size_mb(file_path):
    size_bytes = os.path.getsize(file_path)
    return size_bytes / (1024 * 1024)


def decide_engine(file_path, threshold_mb=None):
    """
    يرجع dict فيه: engine ("python_batch" أو "pyspark")، وحجم الملف، والسبب.
    السبب نطبعه دائمًا (البند 6.2: يجب طباعة حجم الملف والمحرك المختار وسبب الاختيار).
    """
    threshold_mb = threshold_mb if threshold_mb is not None else SMALL_FILE_THRESHOLD_MB
    file_size_mb = get_file_size_mb(file_path)

    if file_size_mb <= threshold_mb:
        engine = "python_batch"
        reason = (
            f"حجم الملف {file_size_mb:.2f}MB <= الحد الفاصل {threshold_mb:.0f}MB، "
            "فالقراءة التسلسلية بدفعات كافية وأسرع من كلفة إقلاع Spark."
        )
    else:
        engine = "pyspark"
        reason = (
            f"حجم الملف {file_size_mb:.2f}MB > الحد الفاصل {threshold_mb:.0f}MB، "
            "نحتاج معالجة موزّعة على أنوية/عقد متعددة عشان الأداء."
        )

    decision = {
        "file_path": str(file_path),
        "file_size_mb": round(file_size_mb, 3),
        "threshold_mb": threshold_mb,
        "engine": engine,
        "reason": reason,
    }

    print(f"[Router] الملف: {decision['file_path']}")
    print(f"[Router] الحجم: {decision['file_size_mb']} MB (الحد الفاصل: {threshold_mb} MB)")
    print(f"[Router] المحرك المختار: {engine}")
    print(f"[Router] السبب: {reason}")

    return decision
