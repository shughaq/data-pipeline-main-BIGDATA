from dotenv import load_dotenv
load_dotenv()

from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
DB_NAME = os.getenv("DB_NAME", "midterm_data_pipeline_bigdata")
TARGET_CURRENCY = os.getenv("TARGET_CURRENCY", "YER")
SMALL_FILE_THRESHOLD_MB = float(os.getenv("SMALL_FILE_THRESHOLD_MB", "100"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1000"))
RESULTS_JSON_PATH = REPORTS_DIR / "results.json"
INCREMENTAL_STATE_PATH = DATA_DIR / "_incremental_state.json"

COLLECTION_RAW = os.getenv("COLLECTION_RAW", "orders_raw")
COLLECTION_VALIDATED = os.getenv("COLLECTION_VALIDATED", "orders_validated")
COLLECTION_QUARANTINE = os.getenv("COLLECTION_QUARANTINE", "orders_quarantine")

MV_DAILY_SALES = os.getenv("MV_DAILY_SALES", "daily_sales_summary")
MV_TOP_PRODUCTS = os.getenv("MV_TOP_PRODUCTS", "top_products_summary")
MV_LEDGER = os.getenv("MV_LEDGER", "mv_source_ledger")
MV_STATE = os.getenv("MV_STATE", "mv_refresh_state")
JOB_LOG_COLLECTION = os.getenv("JOB_LOG_COLLECTION", "job_runs")

API_HOST = os.getenv("API_HOST", "127.0.0.1")
API_PORT = int(os.getenv("API_PORT", "8000"))
SCHEDULE_REFRESH_HOUR = int(os.getenv("SCHEDULE_REFRESH_HOUR", "1"))
SCHEDULE_REPORT_HOUR = int(os.getenv("SCHEDULE_REPORT_HOUR", "2"))
SCHEDULE_POLL_SECONDS = int(os.getenv("SCHEDULE_POLL_SECONDS", "30"))

# Existing Spark settings from the midterm pipeline.
MONGO_SPARK_CONNECTOR_PACKAGE = os.getenv("MONGO_SPARK_CONNECTOR_PACKAGE", "org.mongodb.spark:mongo-spark-connector_2.13:10.4.0")
MONGO_BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "10000"))
SPARK_APP_NAME = os.getenv("SPARK_APP_NAME", "HybridELT")
SPARK_LOCAL_DIR = os.getenv("SPARK_LOCAL_DIR", str(DATA_DIR / "spark_tmp"))
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_SHUFFLE_PARTITIONS = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
SPARK_MAX_PARTITION_BYTES = int(os.getenv("SPARK_MAX_PARTITION_BYTES", str(128 * 1024 * 1024)))
RAW_SOURCE_COLLECTION = COLLECTION_RAW
