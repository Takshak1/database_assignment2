"""Send insert requests one-by-one for each JSON entry.

Defaults:
- Input file: university_data.json
- Endpoint: http://127.0.0.1:8002/crud_auto

Usage (example):
    python bulk_insert.py
    python bulk_insert.py --file data.json --endpoint http://127.0.0.1:8002/crud_auto --entity university
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List

import requests


def _load_entries(path: str, unwrap_key: str | None = None) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)

    if unwrap_key:
        if isinstance(data, list):
            data = [entry.get(unwrap_key) for entry in data if isinstance(entry, dict)]
        elif isinstance(data, dict):
            data = data.get(unwrap_key)

    if isinstance(data, list):
        return [entry for entry in data if isinstance(entry, dict)]
    if isinstance(data, dict):
        return [data]
    raise ValueError("Input JSON must be an object or list of objects")


def _post_entry(endpoint: str, entity: str, entry: Dict[str, Any], execute: bool) -> Dict[str, Any]:
    payload = {
        "entity": entity,
        "operation": "insert",
        "payload": entry,
        "execute": execute,
    }
    response = requests.post(endpoint, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def main() -> None:
    parser = argparse.ArgumentParser(description="Send insert requests one-by-one for each JSON entry")
    parser.add_argument("--file", default="university_data.json", help="Path to JSON file")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8002/crud_auto", help="CRUD auto endpoint")
    parser.add_argument("--entity", default="university", help="Entity name for auto-registration")
    parser.add_argument("--execute", action="store_true", help="Execute inserts against live backends")
    parser.add_argument(
        "--unwrap-key",
        default=None,
        help="If set, extract this key from each JSON object before inserting",
    )
    args = parser.parse_args()

    entries = _load_entries(args.file, unwrap_key=args.unwrap_key)
    successes = 0
    failures: List[Dict[str, Any]] = []

    for index, entry in enumerate(entries, start=1):
        try:
            result = _post_entry(args.endpoint, args.entity, entry, args.execute)
            successes += 1
            print(f"[{index}/{len(entries)}] OK: schema_id={result.get('schema_id')}")
        except Exception as exc:  # pragma: no cover - runtime safety
            failures.append({"index": index, "error": str(exc)})
            print(f"[{index}/{len(entries)}] FAILED: {exc}")

    print("\nSummary")
    print(f"  Successes: {successes}")
    print(f"  Failures: {len(failures)}")
    if failures:
        print(json.dumps(failures, indent=2))


if __name__ == "__main__":
    main()
