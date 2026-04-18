"""Insert university_data.json into the hybrid database via /crud_auto.

Splits the dataset into normalized entities so SQL tables receive columns.

Usage (examples):
    python insert_university_dataset.py --dry-run
    python insert_university_dataset.py --execute
    python insert_university_dataset.py --file university_data.json --execute
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter
from typing import Any, Dict, Iterable, List, Tuple

import requests


def _load_university(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and "university" in data:
        university = data["university"]
    elif isinstance(data, dict):
        university = data
    else:
        raise ValueError("Expected a JSON object with a 'university' key")
    if not isinstance(university, dict):
        raise ValueError("University payload must be an object")
    return university


def _flatten(prefix: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    flattened: Dict[str, Any] = {}
    for key, value in payload.items():
        flattened[f"{prefix}{key}"] = value
    return flattened


def _build_payloads(university: Dict[str, Any]) -> List[Tuple[str, Dict[str, Any]]]:
    payloads: List[Tuple[str, Dict[str, Any]]] = []
    university_id = university.get("id") or university.get("university_id") or "UNI-UNKNOWN"

    base = {
        "university_id": university_id,
        "name": university.get("name"),
        "established": university.get("established"),
        "type": university.get("type"),
    }
    contact = university.get("contact") or {}
    if isinstance(contact, dict):
        base.update(_flatten("contact_", contact))
    stats = university.get("stats") or {}
    if isinstance(stats, dict):
        base.update(_flatten("stats_", stats))
    payloads.append(("university", base))

    placements = university.get("placements")
    if isinstance(placements, dict):
        placement_payload = {"university_id": university_id, **placements}
        payloads.append(("placement", placement_payload))

    for department in university.get("departments", []) or []:
        if not isinstance(department, dict):
            continue
        dept_payload = {
            "university_id": university_id,
            **{k: v for k, v in department.items() if k != "programs"},
        }
        payloads.append(("department", dept_payload))
        for program in department.get("programs", []) or []:
            if not isinstance(program, dict):
                continue
            program_payload = {
                "university_id": university_id,
                "dept_id": department.get("dept_id"),
                **program,
            }
            payloads.append(("program", program_payload))

    for faculty in university.get("faculty_members", []) or []:
        if not isinstance(faculty, dict):
            continue
        faculty_payload = {
            "university_id": university_id,
            **faculty,
        }
        payloads.append(("faculty_member", faculty_payload))

    for student in university.get("students", []) or []:
        if not isinstance(student, dict):
            continue
        student_payload = {
            "university_id": university_id,
            **student,
        }
        payloads.append(("student", student_payload))

    for course in university.get("courses", []) or []:
        if not isinstance(course, dict):
            continue
        course_payload = {
            "university_id": university_id,
            **course,
        }
        payloads.append(("course", course_payload))

    return payloads


def _post_payload(
    endpoint: str,
    entity: str,
    payload: Dict[str, Any],
    execute: bool,
    timeout: int,
) -> Dict[str, Any]:
    request_body = {
        "entity": entity,
        "operation": "insert",
        "payload": payload,
        "execute": execute,
    }
    response = requests.post(endpoint, json=request_body, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _summarize_payloads(payloads: Iterable[Tuple[str, Dict[str, Any]]]) -> None:
    entity_counts = Counter(entity for entity, _ in payloads)
    print("Prepared payloads:")
    for entity, count in sorted(entity_counts.items()):
        print(f"  {entity}: {count}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Insert university_data.json across normalized entities")
    parser.add_argument("--file", default="university_data.json", help="Path to the dataset JSON")
    parser.add_argument(
        "--endpoint",
        default=os.getenv("SCHEMA_REGISTRY_API_ENDPOINT", "http://127.0.0.1:8002/crud_auto"),
        help="CRUD auto endpoint",
    )
    parser.add_argument("--execute", action="store_true", help="Execute inserts against live backends")
    parser.add_argument("--dry-run", action="store_true", help="Only print planned inserts without calling API")
    parser.add_argument("--timeout", type=int, default=30, help="HTTP timeout (seconds)")
    args = parser.parse_args()

    university = _load_university(args.file)
    payloads = _build_payloads(university)

    _summarize_payloads(payloads)
    if args.dry_run:
        print("Dry-run complete. No API calls were made.")
        return

    successes = 0
    failures: List[Dict[str, Any]] = []
    total = len(payloads)

    for index, (entity, payload) in enumerate(payloads, start=1):
        try:
            result = _post_payload(args.endpoint, entity, payload, args.execute, args.timeout)
            successes += 1
            schema_id = result.get("schema_id")
            print(f"[{index}/{total}] OK: {entity} schema_id={schema_id}")
        except Exception as exc:  # pragma: no cover - runtime safety
            failures.append({"index": index, "entity": entity, "error": str(exc)})
            print(f"[{index}/{total}] FAILED: {entity} -> {exc}")

    print("\nSummary")
    print(f"  Successes: {successes}")
    print(f"  Failures: {len(failures)}")
    if failures:
        print(json.dumps(failures, indent=2))


if __name__ == "__main__":
    main()
