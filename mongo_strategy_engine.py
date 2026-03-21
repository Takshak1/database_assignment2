"""Step 6 MongoDB document strategy engine."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


class MongoDocumentStrategyEngine:
    """Decide whether MongoDB-bound fields should be embedded or referenced."""

    EMBED = "embed"
    REFERENCE = "reference"

    def generate_strategy(
        self,
        *,
        entity_name: str,
        entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        root_collection = self._collection_name(entity_name or "entity")
        documents: Dict[str, Dict[str, Any]] = {
            root_collection: self._new_document(root_collection)
        }
        container_map: Dict[Optional[str], str] = {None: root_collection, "": root_collection}
        strategy_entries: List[Dict[str, Any]] = []
        rules = {self.EMBED: [], self.REFERENCE: []}

        for entry in sorted(entries, key=lambda e: e.get("nesting_level", 0)):
            pipeline = (entry.get("pipeline") or "").lower()
            if pipeline != "mongo":
                continue

            decision = self._decide(entry)
            parent_collection = self._resolve_container(container_map, entry.get("parent"))

            if decision["decision"] == self.REFERENCE:
                collection = self._collection_name(entry.get("field_path", "collection"))
                documents.setdefault(collection, self._new_document(collection))
                documents[parent_collection]["references"].append(
                    {
                        "field_path": entry.get("field_path"),
                        "collection": collection,
                        "reason": decision["reason"],
                    }
                )
                documents[collection]["sources"].add(entry.get("field_path"))
                documents[collection]["reasons"].add(decision["reason"])
                container_map[entry.get("field_path")] = collection
                rules[self.REFERENCE].append(entry.get("field_path"))
            else:
                documents[parent_collection]["embedded_fields"].append(
                    {
                        "field_path": entry.get("field_path"),
                        "classification": entry.get("classification"),
                        "reason": decision["reason"],
                    }
                )
                documents[parent_collection]["sources"].add(entry.get("field_path"))
                documents[parent_collection]["reasons"].add(decision["reason"])
                container_map[entry.get("field_path")] = parent_collection
                rules[self.EMBED].append(entry.get("field_path"))

            strategy_entries.append(
                {
                    "field_path": entry.get("field_path"),
                    "parent": entry.get("parent"),
                    "decision": decision["decision"],
                    "reason": decision["reason"],
                    "target_collection": container_map.get(entry.get("field_path"), parent_collection),
                    "parent_collection": parent_collection,
                }
            )

        return {
            "root_collection": root_collection,
            "documents": [self._materialize(doc) for doc in documents.values()],
            "entries": strategy_entries,
            "rules": rules,
        }

    # ------------------------------------------------------------------
    # Decision heuristics
    # ------------------------------------------------------------------
    def _decide(self, entry: Dict[str, Any]) -> Dict[str, str]:
        classification = (entry.get("classification") or "").lower()
        pattern = (entry.get("pattern") or "").lower()
        depth = int(entry.get("nesting_level", 0) or 0)
        flags = [flag.lower() for flag in entry.get("flags", [])]

        # Default to embedding for tightly coupled data
        decision = self.EMBED
        reason = "tightly_coupled"

        if "deep_nesting" in flags or depth >= 3:
            decision = self.REFERENCE
            reason = "deep_nesting"
        elif classification in {"repeating_entity"} or "array of objects" in pattern:
            decision = self.REFERENCE
            reason = "array_of_objects"
        elif classification in {"nested_object"}:
            if depth > 1:
                decision = self.REFERENCE
                reason = "nested_object_large"
            else:
                reason = "nested_object_coupled"
        elif classification in {"embedded_list"}:
            if depth <= 1:
                reason = "small_array"
            else:
                decision = self.REFERENCE
                reason = "large_array"
        elif pattern in {"array of primitives", "array_of_primitives"}:
            reason = "small_array_primitives" if depth <= 1 else "large_array"
            if depth > 1:
                decision = self.REFERENCE
        elif classification in {"attribute", "simple_field"}:
            reason = "attribute"

        return {"decision": decision, "reason": reason}

    # ------------------------------------------------------------------
    # Helper utilities
    # ------------------------------------------------------------------
    def _collection_name(self, value: str) -> str:
        cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", value or "collection")
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        return cleaned.lower() or "collection"

    def _resolve_container(self, mapping: Dict[Optional[str], str], parent_path: Optional[str]) -> str:
        if parent_path in mapping:
            return mapping[parent_path]
        if not parent_path:
            return mapping.get(None)
        segments = parent_path.split(".")
        while segments:
            candidate = ".".join(segments)
            if candidate in mapping:
                return mapping[candidate]
            segments.pop()
        return mapping.get(None)

    def _new_document(self, collection: str) -> Dict[str, Any]:
        return {
            "collection": collection,
            "embedded_fields": [],
            "references": [],
            "sources": set(),
            "reasons": set(),
        }

    def _materialize(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "collection": doc["collection"],
            "embedded_fields": doc["embedded_fields"],
            "references": doc["references"],
            "sources": sorted(doc["sources"]),
            "reasons": sorted(doc["reasons"]),
        }


__all__ = ["MongoDocumentStrategyEngine"]
