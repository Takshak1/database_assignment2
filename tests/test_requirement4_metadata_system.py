"""Requirement 4: Metadata system captures structural + semantic information."""

from __future__ import annotations

from typing import Dict


def test_metadata_tracks_field_level_attributes(stored_schema: Dict[str, object]) -> None:
    fields = {field["field_name"]: field for field in stored_schema["fields"]}
    username = fields["username"]

    assert username["metadata"]["type"] == "string"
    assert username["is_unique"] is True
    assert username["parent_field"] is None


def test_analysis_entries_capture_pipeline_and_classification(stored_schema: Dict[str, object]) -> None:
    analysis_entries = {entry["field_path"]: entry for entry in stored_schema["analysis"]["entries"]}

    assert analysis_entries["comments"]["classification"] == "repeating_entity"
    assert analysis_entries["comments"]["pipeline"] == "sql"

    profile_entry = analysis_entries["profile"]
    assert profile_entry["pipeline"] in {"sql", "mongo", "buffer"}
    assert profile_entry["pipeline_reason"]


def test_analysis_summary_reports_totals(stored_schema: Dict[str, object]) -> None:
    summary = stored_schema["analysis"]["summary"]
    assert summary["pipelines"]["sql"] >= 1
    assert "pipelines" in summary
    assert summary.get("pipeline_reasons"), "Expected reason index to be populated for explainability"


def test_payload_field_named_type_is_not_misread_as_schema_type(registry) -> None:
    payload = {
        "university": {
            "id": "UNI-2024-001",
            "name": "Greenfield University",
            "type": "Public Research University",
            "established": 1892,
        }
    }

    stored = registry.register_schema("university", payload)
    fields = stored["fields"]

    root = next(f for f in fields if f["field_name"] == "university" and f["parent_field"] is None)
    nested_type = next(f for f in fields if f["field_name"] == "type" and f["parent_field"] == "university")

    assert root["data_type"] == "object"
    assert nested_type["data_type"] == "string"
