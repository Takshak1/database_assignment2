"""Split university_data.json into multiple entities and insert via /crud_auto.

This script avoids the "single field only" schema issue by inserting each logical
entity (university, department, program, faculty, student, course, placement)
individually. Use --execute to push into SQL/Mongo, or omit it for a dry-run.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import requests


Job = Tuple[str, Dict[str, Any]]


def _load_university(path: Path, root_key: str) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and root_key and root_key in data and isinstance(data[root_key], dict):
        return data[root_key]
    if isinstance(data, dict):
        return data
    raise ValueError("Input JSON must be an object with a university payload")


def _build_jobs(university: Dict[str, Any]) -> List[Job]:
    jobs: List[Job] = []
    university_id = university.get("id") or university.get("university_id")
    if not university_id:
        university_id = "UNIVERSITY-001"

    skip_keys = {"departments", "faculty_members", "students", "courses", "placements"}
    university_payload = {k: v for k, v in university.items() if k not in skip_keys}
    university_payload.setdefault("university_id", university_id)
    jobs.append(("university", university_payload))

    program_to_dept: Dict[str, str] = {}
    for dept in university.get("departments", []) or []:
        dept_payload = {k: v for k, v in dept.items() if k != "programs"}
        dept_payload.setdefault("university_id", university_id)
        jobs.append(("department", dept_payload))

        dept_id = dept_payload.get("dept_id")
        for program in dept.get("programs", []) or []:
            program_payload = dict(program)
            if dept_id:
                program_payload.setdefault("dept_id", dept_id)
            program_payload.setdefault("university_id", university_id)
            jobs.append(("program", program_payload))
            program_id = program_payload.get("program_id")
            if program_id and dept_id:
                program_to_dept[str(program_id)] = str(dept_id)

    for faculty in university.get("faculty_members", []) or []:
        faculty_payload = dict(faculty)
        faculty_payload.setdefault("university_id", university_id)
        jobs.append(("faculty_member", faculty_payload))

    for student in university.get("students", []) or []:
        student_payload = dict(student)
        student_payload.setdefault("university_id", university_id)
        program_id = student_payload.get("program_id")
        if program_id and str(program_id) in program_to_dept:
            student_payload.setdefault("dept_id", program_to_dept[str(program_id)])
        jobs.append(("student", student_payload))

    for course in university.get("courses", []) or []:
        course_payload = dict(course)
        course_payload.setdefault("university_id", university_id)
        jobs.append(("course", course_payload))

    placements = university.get("placements")
    if isinstance(placements, dict) and placements:
        placement_payload = dict(placements)
        placement_payload.setdefault("university_id", university_id)
        jobs.append(("placement", placement_payload))

    return jobs


def _post_job(endpoint: str, entity: str, payload: Dict[str, Any], execute: bool) -> Dict[str, Any]:
    response = requests.post(
        endpoint,
        json={
            "entity": entity,
            "operation": "insert",
            "payload": payload,
            "execute": execute,
        },
        timeout=40,
    )
    response.raise_for_status()
    return response.json()


def _summarize_jobs(jobs: Iterable[Job]) -> Counter:
    counter: Counter = Counter()
    for entity, _ in jobs:
        counter[entity] += 1
    return counter


def main() -> None:
    parser = argparse.ArgumentParser(description="Insert university dataset into the registry")
    parser.add_argument("--file", default="university_data.json", help="Path to university_data.json")
    parser.add_argument("--endpoint", default="http://127.0.0.1:8002/crud_auto", help="CRUD auto endpoint")
    parser.add_argument("--root-key", default="university", help="Root key to unwrap in JSON")
    parser.add_argument("--execute", action="store_true", help="Execute inserts against live backends")
    parser.add_argument("--show-sample", action="store_true", help="Print one sample payload per entity")
    args = parser.parse_args()

    university = _load_university(Path(args.file), args.root_key)
    jobs = _build_jobs(university)
    counts = _summarize_jobs(jobs)

    print("Planned inserts:")
    for entity, count in counts.items():
        print(f"  {entity}: {count}")

    if args.show_sample:
        printed = set()
        for entity, payload in jobs:
            if entity in printed:
                continue
            print(f"\nSample payload for {entity}:")
            print(json.dumps(payload, indent=2))
            printed.add(entity)

    if not args.execute:
        print("\nDry-run complete (use --execute to write to SQL/Mongo).")
        return

    successes = 0
    failures: List[Dict[str, Any]] = []
    total = len(jobs)

    for index, (entity, payload) in enumerate(jobs, start=1):
        try:
            result = _post_job(args.endpoint, entity, payload, execute=True)
            successes += 1
            schema_id = result.get("schema_id")
            print(f"[{index}/{total}] OK {entity}: schema_id={schema_id}")
        except Exception as exc:  # pragma: no cover - runtime safety
            failures.append({"index": index, "entity": entity, "error": str(exc)})
            print(f"[{index}/{total}] FAILED {entity}: {exc}")

    print("\nSummary")
    print(f"  Successes: {successes}")
    print(f"  Failures: {len(failures)}")
    if failures:
        print(json.dumps(failures, indent=2))


if __name__ == "__main__":
    main()
