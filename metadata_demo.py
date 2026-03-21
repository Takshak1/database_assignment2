"""Quick demonstration of the enhanced MetadataManager structural registry."""

from __future__ import annotations

import json
import os

from metadata_manager import MetadataManager


def build_demo_registry() -> None:
    demo_file = "metadata_demo.json"
    if os.path.exists(demo_file):
        os.remove(demo_file)

    manager = MetadataManager(metadata_file=demo_file)
    analyzer_stats = {"total": 100}

    manager.update_field_metadata(
        "post_id",
        {
            "freq": 1.0,
            "unique_count": 100,
            "uniqueness_ratio": 1.0,
            "is_unique_field": True,
            "types": {"int"},
            "stability": 1.0,
            "semantic_info": {"detected_kind": "identifier", "semantic_weight": 0.9},
        },
        {
            "decision": "sql",
            "reason": "primary_identifier",
            "confidence": 0.98,
            "signals": {"identifier": True},
        },
        analyzer_stats,
    )

    manager.update_field_metadata(
        "comments.text",
        {
            "freq": 0.62,
            "unique_count": 80,
            "uniqueness_ratio": 0.8,
            "types": {"str"},
            "stability": 0.92,
            "semantic_info": {"detected_kind": "text", "semantic_weight": 0.2},
            "nested": True,
            "parent_field": "comments",
            "nesting_level": 2,
        },
        {
            "decision": "sql",
            "reason": "belongs_to_parent",
            "confidence": 0.82,
            "signals": {"parent": "comments", "array": True},
        },
        analyzer_stats,
    )

    manager.save_metadata()

    print("STRUCTURAL REGISTRY SAMPLE:\n---------------------------")
    for entry in manager.get_structural_registry():
        print(json.dumps(entry, indent=2))
    print("\nFull metadata saved to", demo_file)


if __name__ == "__main__":
    build_demo_registry()
