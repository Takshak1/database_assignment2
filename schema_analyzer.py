"""JSON structure analyzer for detecting schema patterns."""

from __future__ import annotations

from collections import Counter
from typing import Any, Dict, List, Tuple


class JSONStructureAnalyzer:
	"""Analyze JSON-style schema definitions for structural patterns."""

	PRIMITIVE_TYPES = {"string", "integer", "number", "boolean"}
	COMPLEX_TYPES = {"object", "array"}
	KNOWN_TYPE_TOKENS = PRIMITIVE_TYPES | {"object", "array", "null"}
	DEEP_NEST_THRESHOLD = 3  # depth >= 3 -> mongo candidate
	PATTERN_MEANINGS = {
		"nested object": "possible new table",
		"array of objects": "repeating entity",
		"array of primitives": "embed",
		"deep nesting": "candidate for Mongo",
	}
	CLASSIFICATION_LABELS = {
		"simple_field": "simple field",
		"attribute": "attribute",
		"nested_object": "nested object",
		"root_object": "root object",
		"repeating_entity": "repeating entity",
		"embedded_list": "embedded list",
		"array": "array",
		"unknown": "review",
	}

	def analyze(self, schema: Dict[str, Any], *, already_prepared: bool = False) -> Dict[str, Any]:
		working_schema = schema if already_prepared else self._prepare_schema(schema)
		entries: List[Dict[str, Any]] = []

		if not working_schema:
			return {"entries": [], "summary": {"total_fields": 0}, "readable": []}

		self._walk_schema(
			schema_fragment=working_schema,
			path_prefix="",
			depth=0,
			parent_path=None,
			parent_classification=None,
			entries=entries,
		)
		summary = self._build_summary(entries)
		readable = [self.format_entry(entry) for entry in entries]
		return {"entries": entries, "summary": summary, "readable": readable}

	def prepare_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
		"""Public helper to normalize raw JSON samples into schema definitions."""
		return self._prepare_schema(schema)

	def format_entry(self, entry: Dict[str, Any]) -> str:
		label = entry.get("meaning") or self.CLASSIFICATION_LABELS.get(entry.get("classification", ""), entry.get("classification", ""))
		pipeline = entry.get("pipeline")
		if pipeline:
			return f"{entry.get('field_path', '')} -> {label} [{pipeline.upper()}]"
		return f"{entry.get('field_path', '')} -> {label}"

	# ------------------------------------------------------------------
	# Traversal helpers
	# ------------------------------------------------------------------
	def _normalize_root(self, schema: Dict[str, Any]) -> Dict[str, Any]:
		if not isinstance(schema, dict):
			raise ValueError("Schema must be an object definition")
		if schema.get("type") == "object" and isinstance(schema.get("properties"), dict):
			return schema["properties"]
		return schema

	def _prepare_schema(self, schema: Dict[str, Any]) -> Dict[str, Any]:
		root = self._normalize_root(schema)
		if not isinstance(root, dict):
			return {}
		prepared: Dict[str, Any] = {}
		for field, definition in root.items():
			if self._looks_like_definition(definition):
				prepared[field] = self._normalize_definition(definition)
			else:
				prepared[field] = self._infer_definition_from_sample(definition)
		return prepared

	def _walk_schema(
		self,
		*,
		schema_fragment: Dict[str, Any],
		path_prefix: str,
		depth: int,
		parent_path: str | None,
		parent_classification: str | None,
		entries: List[Dict[str, Any]],
	) -> None:
		for field_name, definition in schema_fragment.items():
			normalized = self._normalize_definition(definition)
			field_path = field_name if not path_prefix else f"{path_prefix}.{field_name}"

			entry = self._classify_field(
				field_path=field_path,
				parent_path=parent_path,
				depth=depth,
				definition=normalized,
				parent_classification=parent_classification,
			)
			entries.append(entry)

			dtype = normalized.get("type", "object")
			if dtype == "object":
				nested = normalized.get("properties", {})
				if nested:
					self._walk_schema(
						schema_fragment=nested,
						path_prefix=field_path,
						depth=depth + 1,
						parent_path=field_path,
						parent_classification=entry["classification"],
						entries=entries,
					)
			elif dtype == "array":
				nested = self._extract_array_properties(normalized)
				if nested:
					self._walk_schema(
						schema_fragment=nested,
						path_prefix=field_path,
						depth=depth + 1,
						parent_path=field_path,
						parent_classification=entry["classification"],
						entries=entries,
					)

	# ------------------------------------------------------------------
	# Classification helpers
	# ------------------------------------------------------------------
	def _classify_field(
		self,
		*,
		field_path: str,
		parent_path: str | None,
		depth: int,
		definition: Dict[str, Any],
		parent_classification: str | None,
	) -> Dict[str, Any]:
		dtype = definition.get("type", "object")
		data_type = self._describe_type(definition)
		flags: List[str] = []
		notes: List[str] = []
		recommendation = "sql_column"
		pattern = "primitive"
		meaning_parts: List[str] = []
		pattern_label = "primitive"
		meaning_label = ""

		if dtype == "object":
			classification = "nested_object" if depth > 0 else "root_object"
			pattern = "nested_object"
			pattern_label = "nested object" if depth > 0 else "root object"
			recommendation = "new_table" if depth > 0 else "root"
			if depth > 0:
				notes.append("Nested object - possible table")
			meaning_label = self.PATTERN_MEANINGS.get("nested object") if depth > 0 else "root object"
		elif dtype == "array":
			inner_type, description = self._describe_array(definition)
			pattern = description
			pattern_label = "array of objects" if inner_type == "object" else (
				"array of primitives" if inner_type in self.PRIMITIVE_TYPES else description.replace("_", " ")
			)
			if inner_type == "object":
				classification = "repeating_entity"
				recommendation = "new_table"
				notes.append("Array of objects - repeating entity")
				meaning_label = self.PATTERN_MEANINGS.get("array of objects", "repeating entity")
			elif inner_type in self.PRIMITIVE_TYPES:
				classification = "embedded_list"
				recommendation = "embed"
				notes.append("Array of primitives - embed inside parent")
				meaning_label = self.PATTERN_MEANINGS.get("array of primitives", "embed")
			else:
				classification = "array"
				recommendation = "review"
				meaning_label = "array"
		elif dtype in self.PRIMITIVE_TYPES:
			if depth == 0:
				classification = "simple_field"
				pattern = "simple_field"
				pattern_label = "simple field"
				recommendation = "sql_column"
				meaning_label = "simple field"
			else:
				classification = "attribute"
				pattern = "nested_attribute"
				pattern_label = "nested attribute"
				recommendation = "child_column"
				if parent_path:
					notes.append(f"Attribute of {parent_path}")
				meaning_label = "attribute"
		else:
			classification = "unknown"
			recommendation = "review"
			pattern = dtype
			pattern_label = dtype.replace("_", " ")
			meaning_label = "review"

		if depth >= self.DEEP_NEST_THRESHOLD:
			flags.append("deep_nesting")
			notes.append("Deep nesting - consider Mongo")
			recommendation = "mongo_candidate"
			meaning_parts.append(self.PATTERN_MEANINGS["deep nesting"])
			pattern_label = pattern_label or "deep nesting"

		base_meaning = meaning_label or self.CLASSIFICATION_LABELS.get(classification, classification)
		meaning_chain = [base_meaning]
		if meaning_parts:
			meaning_chain.extend(meaning_parts)
		meaning_display = " | ".join(filter(None, meaning_chain))

		entry = {
			"field_path": field_path,
			"parent": parent_path,
			"nesting_level": depth,
			"data_type": data_type,
			"classification": classification,
			"pattern": pattern_label or pattern,
			"recommendation": recommendation,
			"flags": flags,
			"notes": "; ".join(notes) if notes else "",
			"meaning": meaning_display,
		}

		return entry

	# ------------------------------------------------------------------
	# Utility helpers
	# ------------------------------------------------------------------
	def _normalize_definition(self, definition: Any) -> Dict[str, Any]:
		if definition is None:
			return {"type": "null"}
		if isinstance(definition, str):
			if definition.lower() in self.KNOWN_TYPE_TOKENS:
				return {"type": definition.lower()}
			return {"type": "string"}
		if isinstance(definition, bool):
			return {"type": "boolean"}
		if isinstance(definition, (int, float)):
			return {"type": "integer" if isinstance(definition, int) and not isinstance(definition, bool) else "number"}
		if isinstance(definition, list):
			return {"type": "array", "items": self._infer_array_items(definition)}
		if not isinstance(definition, dict):
			raise ValueError(f"Unsupported schema definition type: {type(definition)}")

		normalized = dict(definition)
		dtype = normalized.get("type")
		if isinstance(dtype, list) and dtype:
			normalized["type"] = dtype[0]
		if not normalized.get("type") and "properties" in normalized:
			normalized["type"] = "object"
		if not normalized.get("type") and "items" in normalized:
			normalized["type"] = "array"
		return normalized

	def _looks_like_definition(self, value: Any) -> bool:
		if isinstance(value, dict):
			return any(key in value for key in ("type", "properties", "items"))
		if isinstance(value, str):
			return value.lower() in self.KNOWN_TYPE_TOKENS
		return False

	def _infer_definition_from_sample(self, sample: Any) -> Dict[str, Any]:
		if isinstance(sample, dict):
			return {
				"type": "object",
				"properties": {key: self._infer_definition_from_sample(value) for key, value in sample.items()},
			}
		if isinstance(sample, list):
			return {"type": "array", "items": self._infer_array_items(sample)}
		return {"type": self._infer_primitive_type(sample)}

	def _infer_array_items(self, values: List[Any]) -> Dict[str, Any]:
		for value in values:
			if isinstance(value, dict):
				return {
					"type": "object",
					"properties": {
						key: self._infer_definition_from_sample(val)
						for key, val in value.items()
					},
				}
			if isinstance(value, list):
				return {"type": "array", "items": self._infer_array_items(value)}
			if value is not None:
				return {"type": self._infer_primitive_type(value)}
		return {"type": "string"}

	def _infer_primitive_type(self, value: Any) -> str:
		if value is None:
			return "null"
		if isinstance(value, bool):
			return "boolean"
		if isinstance(value, int) and not isinstance(value, bool):
			return "integer"
		if isinstance(value, float):
			return "number"
		return "string"

	def _extract_array_properties(self, definition: Dict[str, Any]) -> Dict[str, Any]:
		items = definition.get("items")
		if isinstance(items, dict) and (
			items.get("type") == "object" or isinstance(items.get("properties"), dict)
		):
			return items.get("properties", {})
		return {}

	def _describe_array(self, definition: Dict[str, Any]) -> Tuple[str, str]:
		items = definition.get("items")
		if isinstance(items, dict):
			itype = items.get("type")
			if not itype and isinstance(items.get("properties"), dict):
				itype = "object"
			if itype:
				descriptor = "array_of_objects" if itype == "object" else f"array_of_{itype}s"
				return itype, descriptor
		return "unknown", "array"

	def _describe_type(self, definition: Dict[str, Any]) -> str:
		dtype = definition.get("type", "object")
		if dtype == "array":
			inner, _ = self._describe_array(definition)
			return f"array<{inner}>"
		return dtype

	def _build_summary(self, entries: List[Dict[str, Any]]) -> Dict[str, Any]:
		if not entries:
			return {"total_fields": 0}

		class_counts = Counter(entry["classification"] for entry in entries)
		pattern_counts = Counter(entry["pattern"] for entry in entries)
		recommendation_counts = Counter(entry["recommendation"] for entry in entries)
		flag_counts = Counter(flag for entry in entries for flag in entry["flags"])
		meaning_counts = Counter(entry.get("meaning") for entry in entries if entry.get("meaning"))

		return {
			"total_fields": len(entries),
			"classifications": dict(class_counts),
			"patterns": dict(pattern_counts),
			"recommendations": dict(recommendation_counts),
			"flags": dict(flag_counts),
			"meanings": dict(meaning_counts),
		}


__all__ = ["JSONStructureAnalyzer"]
