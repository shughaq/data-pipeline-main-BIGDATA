import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
RESULTS_JSON_PATH = Path(os.getenv("RESULTS_JSON_PATH", str(REPORTS_DIR / "results.json")))

# ---------------------------------------------------------------------------
# MongoDB
# ---------------------------------------------------------------------------
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "midterm_data_pipeline")

COLLECTION_RAW = os.getenv("COLLECTION_RAW", "orders_raw")
COLLECTION_VALIDATED = os.getenv("COLLECTION_VALIDATED", "orders_validated")
COLLECTION_QUARANTINE = os.getenv("COLLECTION_QUARANTINE", "orders_quarantine")
RAW_SOURCE_COLLECTION = os.getenv("RAW_SOURCE_COLLECTION", COLLECTION_RAW)


# ---------------------------------------------------------------------------
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "200"))

# ---------------------------------------------------------------------------
# Python Batch Loader
# ---------------------------------------------------------------------------
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))

# ---------------------------------------------------------------------------
# PySpark Loader
# ---------------------------------------------------------------------------
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "midterm-hybrid-pipeline")
# spark://192.168.1.10:7077
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
SPARK_LOCAL_DIR = os.getenv("SPARK_LOCAL_DIR", r"D:\\spark_temp")
SPARK_MAX_PARTITION_BYTES = int(os.getenv("SPARK_MAX_PARTITION_BYTES", str(128 * 1024 * 1024)))
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "500"))
MONGO_SPARK_CONNECTOR_PACKAGE = os.getenv(
    "MONGO_SPARK_CONNECTOR_PACKAGE",
    "org.mongodb.spark:mongo-spark-connector_2.13:10.4.1",
)

# ---------------------------------------------------------------------------
TARGET_CURRENCY = "YER"

# ---------------------------------------------------------------------------
INCREMENTAL_WATERMARK_FIELD = "at_updated"
INCREMENTAL_STATE_PATH = DATA_DIR / "_incremental_state.json"
