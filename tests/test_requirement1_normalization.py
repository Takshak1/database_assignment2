from __future__ import annotations

from typing import Dict


def test_array_of_objects_creates_child_table(stored_schema: Dict[str, object]) -> None:
    """Arrays of objects must be promoted into their own SQL tables."""

    blueprint = stored_schema["sql_blueprint"]
    tables = {table["name"]: table for table in blueprint["tables"]}

    assert "comments" in tables, "Expected comments[] to become a relational table"
    comment_reasons = tables["comments"]["reasons"]
    assert "array_of_objects" in comment_reasons
    assert any(column["source"] == "comments.text" for column in tables["comments"]["columns"])


def test_nested_object_generates_table_and_fk(stored_schema: Dict[str, object]) -> None:
    """Nested objects should normalize into their own table linked back to the root."""

    blueprint = stored_schema["sql_blueprint"]
    tables = {table["name"]: table for table in blueprint["tables"]}

    assert "profile" in tables, "Nested profile object should become its own table"
    profile_table = tables["profile"]
    assert profile_table["reasons"] == ["nested_object"]
    assert any(column["source"] == "profile.address" for column in profile_table["columns"])
    fk = profile_table["foreign_keys"][0]
    assert fk["from_table"] == "profile"
    assert fk["to_table"] == "post"


def test_root_table_recorded_with_rules(stored_schema: Dict[str, object]) -> None:
    """Root entity should be the blueprint root and listed in normalization rules."""

    blueprint = stored_schema["sql_blueprint"]
    assert blueprint["root_table"] == "post"
    assert "post" in blueprint["rules"]["root_entities"]
    post_table = next(table for table in blueprint["tables"] if table["name"] == "post")
    assert post_table["reasons"] == ["root_entity"]
