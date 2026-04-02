"""Print all entries from MySQL tables and MongoDB collections.

Uses .env / environment variables:
- MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
- MONGO_HOST, MONGO_PORT, MONGO_DATABASE

Optional row/doc cap for very large datasets:
- DUMP_MAX_ROWS (default: 0 => no limit)

Exit behavior:
- DUMP_STRICT_BACKENDS=1 -> exit non-zero if either backend fails
- default (0) -> exit non-zero only if both backends fail
"""

from __future__ import annotations

import os
import sys
from typing import Any, Dict, Iterable, List

from dotenv import load_dotenv

try:
    import mysql.connector
except Exception:  # pragma: no cover
    mysql = None
else:
    mysql = mysql.connector

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None


def _max_rows() -> int:
    raw = os.getenv("DUMP_MAX_ROWS", "0").strip()
    try:
        return max(int(raw), 0)
    except ValueError:
        return 0


def _print_header(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def _safe_repr(value: Any, limit: int = 200) -> str:
    text = repr(value)
    return text if len(text) <= limit else text[:limit] + "..."


def dump_mysql() -> bool:
    if mysql is None:
        print("MySQL driver not available (`mysql-connector-python` not installed).")
        return False

    cfg = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", "streaming_db"),
    }

    _print_header(f"MySQL dump: {cfg['database']} @ {cfg['host']}")

    try:
        conn = mysql.connect(**cfg)
        cursor = conn.cursor()

        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            print("No tables found.")
            return True

        print(f"Found {len(tables)} tables: {tables}")
        row_cap = _max_rows()

        for table in tables:
            print(f"\n--- TABLE: {table} ---")
            cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
            total = cursor.fetchone()[0]
            print(f"Total rows: {total}")
            if total == 0:
                print("(empty)")
                continue

            query = f"SELECT * FROM `{table}`"
            if row_cap > 0:
                query += f" LIMIT {row_cap}"
            cursor.execute(query)
            rows = cursor.fetchall()
            colnames = [desc[0] for desc in cursor.description]
            for idx, row in enumerate(rows, start=1):
                data = ", ".join(f"{col}={_safe_repr(val)}" for col, val in zip(colnames, row))
                print(f"{idx}. {data}")

            if row_cap > 0 and total > row_cap:
                print(f"... truncated: showing {row_cap}/{total} rows (set DUMP_MAX_ROWS=0 for all)")

        return True
    except Exception as exc:
        print(f"MySQL dump failed: {exc}")
        return False
    finally:
        try:
            cursor.close()  # type: ignore[name-defined]
            conn.close()  # type: ignore[name-defined]
        except Exception:
            pass


def _iter_documents(coll, cap: int) -> Iterable[Dict[str, Any]]:
    if cap > 0:
        return coll.find().limit(cap)
    return coll.find()


def dump_mongodb() -> bool:
    if MongoClient is None:
        print("MongoDB driver not available (`pymongo` not installed).")
        return False

    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", "27017"))
    db_name = os.getenv("MONGO_DATABASE", "streaming_db")

    _print_header(f"MongoDB dump: {db_name} @ {host}:{port}")

    client = None
    try:
        client = MongoClient(host=host, port=port, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        db = client[db_name]

        collections: List[str] = db.list_collection_names()
        if not collections:
            print("No collections found.")
            return True

        print(f"Found {len(collections)} collections: {collections}")
        row_cap = _max_rows()

        for coll_name in collections:
            coll = db[coll_name]
            total = coll.count_documents({})
            print(f"\n--- COLLECTION: {coll_name} ---")
            print(f"Total documents: {total}")
            if total == 0:
                print("(empty)")
                continue

            for idx, doc in enumerate(_iter_documents(coll, row_cap), start=1):
                print(f"{idx}. {_safe_repr(doc, limit=400)}")

            if row_cap > 0 and total > row_cap:
                print(f"... truncated: showing {row_cap}/{total} documents (set DUMP_MAX_ROWS=0 for all)")

        return True
    except Exception as exc:
        print(f"MongoDB dump failed: {exc}")
        return False
    finally:
        if client is not None:
            client.close()


def main() -> int:
    load_dotenv()
    mysql_ok = dump_mysql()
    mongo_ok = dump_mongodb()
    strict = os.getenv("DUMP_STRICT_BACKENDS", "0").strip().lower() in {"1", "true", "yes", "on"}

    if mysql_ok and mongo_ok:
        print("\nDone: SQL + Mongo dumps completed.")
        return 0

    if mysql_ok or mongo_ok:
        failed = []
        if not mysql_ok:
            failed.append("MySQL")
        if not mongo_ok:
            failed.append("MongoDB")
        failed_text = ", ".join(failed)

        if strict:
            print(
                f"\nCompleted with partial success ({failed_text} failed). "
                "Strict mode is enabled (DUMP_STRICT_BACKENDS=1), so exiting with code 1."
            )
            return 1

        print(
            f"\nCompleted with partial success: {failed_text} failed; available backend dump succeeded. "
            "Set DUMP_STRICT_BACKENDS=1 to treat this as an error."
        )
        return 0

    print("\nCompleted with errors: both MySQL and MongoDB dumps failed (see logs above).")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
