"""Step 4 data classification engine for routing fields to storage pipelines."""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


class DataClassificationEngine:
    """Decide whether each schema field belongs in SQL, MongoDB, or Buffer."""

    PIPELINE_SQL = "sql"
    PIPELINE_MONGO = "mongo"
    PIPELINE_BUFFER = "buffer"

    def __init__(self, *, deep_mongo_depth: int = 3) -> None:
        self.deep_mongo_depth = deep_mongo_depth

    def classify_entries(
        self,
        entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Return entries augmented with pipeline decisions plus summary stats."""

        classified: List[Dict[str, Any]] = []
        pipeline_counts = {
            self.PIPELINE_SQL: 0,
            self.PIPELINE_MONGO: 0,
            self.PIPELINE_BUFFER: 0,
        }
        reason_index: Dict[str, List[str]] = {}

        for entry in entries:
            decision, reason, confidence = self._decide_pipeline(entry)
            enriched = dict(entry)
            enriched["pipeline"] = decision
            enriched["pipeline_reason"] = reason
            enriched["pipeline_confidence"] = confidence
            classified.append(enriched)

            pipeline_counts[decision] += 1
            reason_index.setdefault(reason, []).append(entry.get("field_path", ""))

        summary = {
            "pipelines": pipeline_counts,
            "reasons": reason_index,
        }
        return {"entries": classified, "summary": summary}


    def _decide_pipeline(self, entry: Dict[str, Any]) -> Tuple[str, str, float]:
        classification = (entry.get("classification") or "").lower()
        pattern = (entry.get("pattern") or "").lower()
        data_type = (entry.get("data_type") or "").lower()
        depth = int(entry.get("nesting_level", 0) or 0)
        flags = entry.get("flags", []) or []

        # Priority 1: Deep nesting automatically goes to MongoDB
        if "deep_nesting" in flags or depth >= self.deep_mongo_depth:
            return self.PIPELINE_MONGO, "deep_nesting", 0.95

        # Priority 2: Arrays / embedded content that suits document stores
        if pattern in {"array of primitives", "array_of_primitives"}:
            return self.PIPELINE_MONGO, "embedded_array", 0.8
        if pattern in {"array of objects", "array_of_objects"} and depth >= 2:
            return self.PIPELINE_MONGO, "deep_array_of_objects", 0.85
        if classification in {"embedded_list", "array"}:
            return self.PIPELINE_MONGO, "flexible_array", 0.8

        # Priority 3: Structured entities / attributes -> SQL
        if classification in {"root_object"}:
            return self.PIPELINE_SQL, "root_entity", 0.9
        if classification in {"repeating_entity"}:
            return self.PIPELINE_SQL, "repeating_entity", 0.92
        if classification in {"nested_object"} and depth <= 2:
            return self.PIPELINE_SQL, "nested_relation", 0.85
        if classification in {"simple_field"}:
            return self.PIPELINE_SQL, "structured_field", 0.88
        if classification in {"attribute"}:
            return self.PIPELINE_SQL, "child_attribute", 0.8

        # Priority 4: Large documents or flexible schemas -> MongoDB
        if pattern in {"nested object", "nested_object"} and depth > 2:
            return self.PIPELINE_MONGO, "deep_nested_object", 0.88

        # Priority 5: Unknown or insufficient information -> Buffer
        if not classification or classification == "unknown":
            return self.PIPELINE_BUFFER, "insufficient_structure", 0.4
        if data_type in {"null", ""}:
            return self.PIPELINE_BUFFER, "null_type", 0.35

        return self.PIPELINE_SQL, "default_structured", 0.7


__all__ = ["DataClassificationEngine"]
