from pymongo import MongoClient

MONGO_URI = "mongodb://127.0.0.1:27017"
DB_NAME = "midterm_orders_pipeline"

client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
db = client[DB_NAME]

results = []

def check(name, condition, detail=""):
    status = "PASS OK" if condition else "FAIL XX"
    results.append((name, status))
    line = f"{status} | {name}"
    if detail:
        line += f"  ->  {detail}"
    print(line)

print("=" * 70)
print("Fahs sarii3 limutatalabat almashrou")
print("=" * 70)

collections = db.list_collection_names()
check("orders_raw mawjooda", "orders_raw" in collections)
check("orders_validated mawjooda", "orders_validated" in collections)
check("orders_quarantine mawjooda", "orders_quarantine" in collections)

raw_count = db.orders_raw.estimated_document_count()
validated_count = db.orders_validated.estimated_document_count()
quarantine_count = db.orders_quarantine.estimated_document_count()
print(f"\nAdad taqriibi: orders_raw={raw_count} | orders_validated={validated_count} | orders_quarantine={quarantine_count}\n")

indexes = list(db.orders_validated.list_indexes())
has_unique_index = any(
    idx.get("unique") and "order_id" in idx.get("key", {})
    for idx in indexes
)
check("Unique Index 3ala order_id (orders_validated)", has_unique_index)

validator = db.get_collection("orders_validated").options().get("validator")
check("Schema Validation fi orders_validated", bool(validator))

raw_validator = db.get_collection("orders_raw").options().get("validator")
check("orders_raw bidoon Validator (6.9)", not bool(raw_validator))

sample_raw = db.orders_raw.find_one()
if sample_raw:
    required_raw_fields = ["id_run", "file_source", "number_row_source", "at_ingested", "engine_used", "record_raw"]
    missing = [f for f in required_raw_fields if f not in sample_raw]
    check("orders_raw feeha kul alhuqool (6.5)", len(missing) == 0,
          f"na2is: {missing}" if missing else "")
else:
    check("orders_raw feeha kul alhuqool (6.5)", False, "la yojad mustanadat")

sample_q = db.orders_quarantine.find_one()
if sample_q:
    check("orders_quarantine feeha error_codes", "error_codes" in sample_q)
    check("orders_quarantine feeha error_details", "error_details" in sample_q)
else:
    check("orders_quarantine feeha error_codes/error_details", False, "la yojad mustanadat")

sample_corrected = db.orders_validated.find_one({"quality_status": "corrected"})
if sample_corrected:
    check("sijil corrected feeh corrections ghair fadiya", bool(sample_corrected.get("corrections")))
else:
    check("sijil corrected feeh corrections ghair fadiya", False, "la yojad sijil corrected")

sample_valid = db.orders_validated.find_one({"quality_status": "valid"})
check("yojad sijil quality_status = valid", sample_valid is not None)

print("\n" + "=" * 70)
passed = sum(1 for _, s in results if "PASS" in s)
total_checks = len(results)
print(f"Alnatija: {passed} min {total_checks} fuhoosat najiha")
print("=" * 70)

client.close()
