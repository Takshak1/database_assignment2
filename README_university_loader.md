# University dataset loader

This helper script loads `university_data.json` and inserts the data into the hybrid storage system by splitting the dataset into normalized logical entities (university, department, program, faculty member, student, course, placement).

## Prerequisites

- The schema registry API should be running on `http://127.0.0.1:8002`.
- Dependencies installed from `requirements.txt` (uses `requests`).

## Quick start

Dry-run (no API calls):

```bash
python insert_university_dataset.py --dry-run
```

Execute inserts:

```bash
python insert_university_dataset.py --execute
```

Custom file and endpoint:

```bash
python insert_university_dataset.py --file university_data.json --endpoint http://127.0.0.1:8002/crud_auto --execute
```

## What gets inserted

- `university` (flattened contact + stats)
- `placement`
- `department`
- `program`
- `faculty_member`
- `student`
- `course`

The script automatically attaches `university_id` to child entities and `dept_id` to programs so they can be joined later.
