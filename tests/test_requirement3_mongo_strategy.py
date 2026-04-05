from __future__ import annotations

from mongo_strategy_engine import MongoDocumentStrategyEngine


ENGINE = MongoDocumentStrategyEngine()


def test_tightly_coupled_fields_are_embedded() -> None:
    entries = [
        {
            "field_path": "profile.address",
            "parent": "profile",
            "pipeline": "mongo",
            "classification": "nested_object",
            "pattern": "nested object",
            "nesting_level": 1,
            "flags": [],
        }
    ]

    strategy = ENGINE.generate_strategy(entity_name="user", entries=entries)
    assert "profile.address" in strategy["rules"]["embed"]
    doc = next(doc for doc in strategy["documents"] if doc["collection"] == "user")
    assert any(field["field_path"] == "profile.address" for field in doc["embedded_fields"])


def test_repeating_entities_become_references() -> None:
    entries = [
        {
            "field_path": "activity.events",
            "parent": "activity",
            "pipeline": "mongo",
            "classification": "repeating_entity",
            "pattern": "array of objects",
            "nesting_level": 3,
            "flags": ["deep_nesting"],
        }
    ]

    strategy = ENGINE.generate_strategy(entity_name="user", entries=entries)
    assert "activity.events" in strategy["rules"]["reference"]
    root_doc = next(doc for doc in strategy["documents"] if doc["collection"] == "user")
    assert any(ref["field_path"] == "activity.events" for ref in root_doc["references"])
    target_doc = next(doc for doc in strategy["documents"] if doc["collection"] == "activity_events")
    assert "activity.events" in target_doc["sources"]


def test_array_of_primitives_embeds_when_shallow() -> None:
    entries = [
        {
            "field_path": "tags",
            "parent": None,
            "pipeline": "mongo",
            "classification": "embedded_list",
            "pattern": "array of primitives",
            "nesting_level": 1,
            "flags": [],
        }
    ]

    strategy = ENGINE.generate_strategy(entity_name="article", entries=entries)
    assert "tags" in strategy["rules"]["embed"]
    root_doc = next(doc for doc in strategy["documents"] if doc["collection"] == "article")
    assert any(field["field_path"] == "tags" for field in root_doc["embedded_fields"])
