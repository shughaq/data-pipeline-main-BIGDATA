"""CLI for demonstrating every Phase 2 requirement without replacing the midterm CLI."""
from __future__ import annotations

import argparse
from pymongo import MongoClient

from config.settings import DB_NAME, MONGODB_URI
from src.final_aggregations import list_aggregations, run_aggregation
from src.final_jobs import run_job
from src.final_materialized_views import refresh, top_products_from_view
from src.final_queries import create_indexes, explain_queries, list_queries, run_query


def main():
    parser = argparse.ArgumentParser(description="Big Data Phase 2 runner")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("indexes")
    sub.add_parser("explain")
    q = sub.add_parser("query"); q.add_argument("name", choices=[x["name"] for x in list_queries()])
    a = sub.add_parser("aggregation"); a.add_argument("name", choices=[x["name"] for x in list_aggregations()])
    r = sub.add_parser("refresh-mv"); r.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    j = sub.add_parser("job"); j.add_argument("name", choices=["refresh_daily_sales", "refresh_top_products"])
    sub.add_parser("top-products")
    args = parser.parse_args()

    client = MongoClient(MONGODB_URI)
    try:
        db = client[DB_NAME]
        if args.command == "indexes":
            print(create_indexes(db))
        elif args.command == "explain":
            print(explain_queries(db))
        elif args.command == "query":
            print(run_query(db, args.name))
        elif args.command == "aggregation":
            print(run_aggregation(db, args.name))
        elif args.command == "refresh-mv":
            print(refresh(db, mode=args.mode))
        elif args.command == "job":
            print(run_job(args.name))
        elif args.command == "top-products":
            print(top_products_from_view(db))
    finally:
        client.close()


if __name__ == "__main__":
    main()
