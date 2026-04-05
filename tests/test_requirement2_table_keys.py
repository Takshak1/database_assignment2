
from __future__ import annotations

from typing import Dict


def test_primary_keys_follow_convention(stored_schema: Dict[str, object]) -> None:
    """Every generated table should expose a SERIAL primary key named <table>_id."""

    blueprint = stored_schema["sql_blueprint"]
    for table in blueprint["tables"]:
        pk = table["primary_key"]
        assert pk.endswith("_id"), f"{table['name']} primary key must follow naming convention"
        pk_column = next(column for column in table["columns"] if column["name"] == pk)
        assert "PRIMARY KEY" in pk_column["constraints"]
        assert pk_column["type"] == "SERIAL"


def test_foreign_keys_point_to_root(stored_schema: Dict[str, object]) -> None:
    """Child tables should include FK columns referencing the root entity."""

    blueprint = stored_schema["sql_blueprint"]
    relationships = blueprint["relationships"]
    assert {rel["from_table"] for rel in relationships} == {"comments", "profile"}
    for rel in relationships:
        assert rel["to_table"] == "post"
        assert rel["from_column"] == "post_id"
        assert rel["to_column"] == "post_id"


def test_child_tables_include_fk_columns(stored_schema: Dict[str, object]) -> None:
    blueprint = stored_schema["sql_blueprint"]
    tables = {table["name"]: table for table in blueprint["tables"]}
    for table_name in ("comments", "profile"):
        fk_column = next(column for column in tables[table_name]["columns"] if column["name"] == "post_id")
        assert fk_column["type"] == "BIGINT"
        assert fk_column["nullable"] is False
