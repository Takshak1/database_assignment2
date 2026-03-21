import os
import sys
from typing import Optional
import mysql.connector
from dotenv import load_dotenv

MAX_ROWS = int(os.getenv("DUMP_MAX_ROWS", "200"))  


def connect_mysql():
    load_dotenv()
    cfg = {
        "host": os.getenv("MYSQL_HOST", "localhost"),
        "user": os.getenv("MYSQL_USER", "root"),
        "password": os.getenv("MYSQL_PASSWORD", ""),
        "database": os.getenv("MYSQL_DATABASE", "streaming_db"),
    }
    return mysql.connector.connect(**cfg)


def dump_table(cursor, table: str, limit: Optional[int] = MAX_ROWS):
    print(f"\n=== TABLE: {table} ===")
    cursor.execute(f"DESCRIBE `{table}`")
    cols = cursor.fetchall()
    print("Columns:")
    for field, type_info, null, key, default, extra in cols:
        key_info = f" [{key}]" if key else ""
        null_info = " NULL" if null == "YES" else " NOT NULL"
        default_info = f" DEFAULT({default})" if default is not None else ""
        print(f"  - {field}: {type_info}{key_info}{null_info}{default_info}")

    cursor.execute(f"SELECT COUNT(*) FROM `{table}`")
    total = cursor.fetchone()[0]
    print(f"Rows: {total}")

    if total == 0:
        print("(empty)")
        return
    dump_limit = limit if limit is not None else total
    cursor.execute(f"SELECT * FROM `{table}` LIMIT {dump_limit}")
    rows = cursor.fetchall()

    colnames = [desc[0] for desc in cursor.description]
    print(f"\nFirst {len(rows)} rows:")
    for i, row in enumerate(rows, 1):
        pairs = ", ".join(f"{col}={repr(val)[:200]}" for col, val in zip(colnames, row))
        print(f"  {i}. {pairs}")


def main():
    try:
        conn = connect_mysql()
        cursor = conn.cursor()
        schema = os.getenv("MYSQL_DATABASE", "streaming_db")
        print("=" * 80)
        print(f"Dumping MySQL database: {schema}")
        print("=" * 80)

        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
        if not tables:
            print("No tables found.")
        else:
            print(f"Found {len(tables)} tables: {tables}")
            for t in tables:
                dump_table(cursor, t, MAX_ROWS)

        cursor.close()
        conn.close()
        print("\nDone.")
    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
