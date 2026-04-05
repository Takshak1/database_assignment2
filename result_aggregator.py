"""Step 11: Result aggregation utilities for hybrid SQL + Mongo workloads."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from schema_registry import SchemaRegistry


class ResultAggregator:
    """Merge SQL row sets and Mongo documents into a unified JSON shape."""

    def __init__(self, *, registry: Optional[SchemaRegistry] = None) -> None:
        self.registry = registry or SchemaRegistry()

    def aggregate(
        self,
        schema_id: int,
        *,
        sql_rows: Optional[List[Dict[str, Any]]] = None,
        mongo_rows: Optional[List[Dict[str, Any]]] = None,
        merge_plan: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        schema = self.registry.get_schema(schema_id)
        storage_strategy = schema.get("storage_strategy") or {}
        sql_index = self._build_sql_alias_index(storage_strategy)
        merge_key_candidates = self._merge_key_candidates(merge_plan)

        base_index, order = self._build_sql_objects(sql_rows or [], sql_index, merge_key_candidates)
        results = self._merge_mongo_docs(
            base_index,
            order,
            mongo_rows or [],
            merge_key_candidates,
        )
        return results

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------
    def _build_sql_alias_index(self, storage_strategy: Dict[str, Any]) -> Dict[str, str]:
        index: Dict[str, str] = {}
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            decision = (mapping.get("decision") or "sql").lower()
            if decision != "sql":
                continue
            table = mapping.get("table")
            column = mapping.get("column")
            field_path = mapping.get("field_path")
            if not table or not column or not field_path:
                continue
            alias = f"{table}_{column}".lower()
            index[alias] = field_path
        return index

    def _build_sql_objects(
        self,
        rows: List[Dict[str, Any]],
        alias_index: Dict[str, str],
        merge_key_candidates: List[str],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[str]]:
        base: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for idx, row in enumerate(rows):
            logical = self._row_to_object(row, alias_index)
            merge_key = self._extract_merge_key(row, logical, merge_key_candidates)
            safe_key = self._safe_key(merge_key, fallback=f"sql_{idx}")
            if safe_key not in base:
                base[safe_key] = logical
                order.append(safe_key)
            else:
                self._merge_maps(base[safe_key], logical)
        return base, order

    def _row_to_object(self, row: Dict[str, Any], alias_index: Dict[str, str]) -> Dict[str, Any]:
        obj: Dict[str, Any] = {}
        for key, value in row.items():
            if value is None:
                continue
            path = alias_index.get(key.lower())
            if path:
                tokens = self._tokenize_path(path)
                self._assign_path_value(obj, tokens, value)
            else:
                obj[key] = value
        return obj

    def _tokenize_path(self, field_path: str) -> List[Tuple[str, bool]]:
        tokens: List[Tuple[str, bool]] = []
        for raw_token in field_path.split("."):
            if not raw_token:
                continue
            is_array = raw_token.endswith("[]")
            cleaned = raw_token[:-2] if is_array else raw_token
            tokens.append((cleaned, is_array))
        return tokens

    def _assign_path_value(
        self,
        target: Dict[str, Any],
        tokens: List[Tuple[str, bool]],
        value: Any,
    ) -> None:
        if not tokens:
            return
        current = target
        for key, is_array in tokens[:-1]:
            if is_array:
                bucket = current.setdefault(key, [])
                if not bucket or not isinstance(bucket[-1], dict):
                    bucket.append({})
                current = bucket[-1]
            else:
                next_target = current.get(key)
                if not isinstance(next_target, dict):
                    next_target = {}
                    current[key] = next_target
                current = next_target

        last_key, is_array = tokens[-1]
        if is_array:
            bucket = current.setdefault(last_key, [])
            if isinstance(value, list):
                bucket.extend(value)
            else:
                bucket.append(value)
        else:
            current[last_key] = value

    # ------------------------------------------------------------------
    # Merge-key helpers
    # ------------------------------------------------------------------
    def _merge_key_candidates(self, merge_plan: Optional[Dict[str, Any]]) -> List[str]:
        candidates: List[str] = []
        key = (merge_plan or {}).get("merge_key")
        if key:
            candidates.extend(self._expand_key_variants(key))
        for fallback in ("username", "user_id", "_id"):
            if fallback not in candidates:
                candidates.extend(self._expand_key_variants(fallback))
        seen = set()
        ordered: List[str] = []
        for candidate in candidates:
            normalized = candidate.lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            ordered.append(candidate)
        return ordered

    def _expand_key_variants(self, key: str) -> List[str]:
        key = key or ""
        if not key:
            return []
        segments = [segment for segment in key.replace("[", "").replace("]", "").split(".") if segment]
        if not segments:
            return []
        alias = "_".join(segments)
        simple = segments[-1]
        return [key, alias, simple]

    def _extract_merge_key(
        self,
        row: Dict[str, Any],
        logical: Dict[str, Any],
        candidates: List[str],
    ) -> Optional[Any]:
        lowered = {k.lower(): v for k, v in row.items()}
        for candidate in candidates:
            alias = candidate.lower()
            if alias in lowered and lowered[alias] is not None:
                return lowered[alias]
            if "." in candidate:
                value = self._resolve_path(logical, candidate)
                if value is not None:
                    return value
        return None

    def _resolve_path(self, document: Dict[str, Any], path: str) -> Any:
        current: Any = document
        for token in path.split('.'):
            if not isinstance(current, dict):
                return None
            current = current.get(token)
            if current is None:
                return None
        return current

    def _safe_key(self, value: Optional[Any], fallback: str) -> str:
        if value is None:
            return fallback
        if isinstance(value, (str, int, float)):
            return str(value)
        return fallback

    # ------------------------------------------------------------------
    # Mongo helpers
    # ------------------------------------------------------------------
    def _merge_mongo_docs(
        self,
        base_index: Dict[str, Dict[str, Any]],
        order: List[str],
        mongo_rows: List[Dict[str, Any]],
        merge_key_candidates: List[str],
    ) -> List[Dict[str, Any]]:
        for idx, doc in enumerate(mongo_rows):
            cleaned = self._sanitize_document(dict(doc))
            merge_key = self._extract_merge_key(cleaned, cleaned, merge_key_candidates)
            safe_key = self._safe_key(merge_key, fallback=f"mongo_{idx}")
            if safe_key not in base_index:
                base_index[safe_key] = {}
                order.append(safe_key)
            self._merge_maps(base_index[safe_key], cleaned)
        return [base_index[key] for key in order]

    def _sanitize_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        doc.pop("_collection", None)
        sanitized: Dict[str, Any] = {}
        for key, value in doc.items():
            if key == "_id":
                sanitized[key] = self._stringify_object_id(value)
                continue
            if value is None:
                continue
            sanitized[key] = self._sanitize_value(value)
        return sanitized

    def _sanitize_value(self, value: Any) -> Any:
        if isinstance(value, list):
            return [self._sanitize_value(item) for item in value]
        if isinstance(value, dict):
            return {k: self._sanitize_value(v) for k, v in value.items()}
        return self._stringify_object_id(value)

    def _stringify_object_id(self, value: Any) -> Any:
        if value.__class__.__name__ == "ObjectId": 
            return str(value)
        return value

    # ------------------------------------------------------------------
    # Generic merge utilities
    # ------------------------------------------------------------------
    def _merge_maps(self, base: Dict[str, Any], addition: Dict[str, Any]) -> None:
        for key, value in addition.items():
            if value is None:
                continue
            if key not in base:
                base[key] = value
                continue
            base[key] = self._merge_values(base[key], value)

    def _merge_values(self, current: Any, incoming: Any) -> Any:
        if isinstance(current, dict) and isinstance(incoming, dict):
            self._merge_maps(current, incoming)
            return current
        if isinstance(current, list) and isinstance(incoming, list):
            current.extend(incoming)
            return current
        if isinstance(current, list):
            current.append(incoming)
            return current
        if isinstance(incoming, list):
            return incoming
        return incoming


__all__ = ["ResultAggregator"]
