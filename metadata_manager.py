"""
Enhanced Metadata Management System
Provides comprehensive field metadata for robust data management and future project phases
"""

import json
import datetime
from typing import Dict, Any, List, Set, Optional
from collections import defaultdict
import statistics
import re

class MetadataManager:
    def __init__(self, metadata_file="metadata.json"):
        self.metadata_file = metadata_file
        self.field_metadata = {}
        self.load_metadata()
        
    def load_metadata(self):
        """Load existing enhanced metadata or create empty structure"""
        try:
            with open(self.metadata_file, 'r') as f:
                data = json.load(f)
                
                # Check if it's old simple metadata format or new enhanced format
                if isinstance(list(data.values())[0], str):
                    # Old simple format: {"field_name": "sql"|"mongo"}
                    print(f"Converting simple metadata to enhanced format...")
                    self._convert_simple_to_enhanced(data)
                else:
                    # New enhanced format
                    self.field_metadata = data
                    print(f"Loaded enhanced metadata for {len(self.field_metadata)} fields")
        except FileNotFoundError:
            self.field_metadata = {}
            print("No existing metadata found - will create new enhanced metadata")
    
    def _convert_simple_to_enhanced(self, simple_metadata):
        """Convert old simple metadata format to enhanced format"""
        current_time = datetime.datetime.now().isoformat()
        
        for field_name, placement in simple_metadata.items():
            self.field_metadata[field_name] = {
                "creation_timestamp": current_time,
                "field_name": field_name,
                "placement_decision": placement,
                "data_profile": {
                    "total_records": 0,
                    "frequency": 0.0,
                    "unique_count": 0,
                    "uniqueness_ratio": 0.0,
                    "is_unique_field": False,
                    "has_nested_data": False,
                    "composite_score": 0.0,
                    "stability_score": 0.0
                },
                "type_analysis": {
                    "detected_types": [],
                    "type_count": 0,
                    "has_type_ambiguity": False,
                    "ambiguity_score": 0.0,
                    "primary_type": "unknown",
                    "type_consistency": 1.0
                },
                "semantic_analysis": {
                    "detected_kind": "unknown",
                    "semantic_weight": 0.0,
                    "pattern_confidence": 0.0,
                    "is_identifier": False,
                    "is_measurement": False,
                    "is_categorical": False,
                    "data_classification": "internal",
                    "avg_length": 0,
                    "max_length": 0,
                    "is_long_text": False
                },
                "placement_reasoning": {
                    "reason": "legacy_decision",
                    "confidence": 1.0,
                    "decision_factors": {},
                    "override_applied": False,
                    "manual_review_needed": False
                },
                "drift_tracking": {
                    "drift_score": 0.0,
                    "should_quarantine": False,
                    "quarantine_reason": "none",
                    "drift_history": [],
                    "stability_trend": "stable"
                },
                "quality_metrics": {
                    "completeness": 1.0,
                    "consistency": 1.0,
                    "validity": 1.0,
                    "accuracy_estimate": 0.8,
                    "data_quality_score": 0.9
                },
                "usage_statistics": {
                    "access_frequency": "medium",
                    "criticality": "standard",
                    "business_importance": "medium",
                    "query_optimization_potential": "moderate",
                    "indexing_recommendation": {
                        "should_index": False,
                        "index_type": "none",
                        "reasoning": "Legacy field - needs analysis"
                    }
                },
                "schema_evolution": [],
                "business_context": {
                    "domain": "general",
                    "privacy_level": "standard",
                    "retention_policy": "indefinite",
                    "compliance_tags": []
                },
                "last_updated": current_time
            }
        
        print(f"Converted {len(simple_metadata)} simple metadata entries to enhanced format")
    
    def update_field_metadata(self, field_name: str, stats: Dict, placement_info: Dict, analyzer_stats: Dict):
        """
        Update comprehensive metadata for a field
        
        Args:
            field_name: Name of the field
            stats: Statistics from analyzer.get_stats()
            placement_info: Placement decision and reasoning
            analyzer_stats: Additional analyzer information
        """
        current_time = datetime.datetime.now().isoformat()
        
        # Initialize field metadata if not exists
        if field_name not in self.field_metadata:
            self.field_metadata[field_name] = {
                "creation_timestamp": current_time,
                "field_name": field_name,
                "placement_decision": None,
                "data_profile": {},
                "type_analysis": {},
                "semantic_analysis": {},
                "placement_reasoning": {},
                "drift_tracking": {},
                "quality_metrics": {},
                "usage_statistics": {},
                "schema_evolution": [],
                "business_context": {},
                "last_updated": current_time
            }
        
        metadata = self.field_metadata[field_name]
        metadata["last_updated"] = current_time
        
        # Update placement decision
        metadata["placement_decision"] = placement_info.get("decision", "unknown")
        
        # Enhanced data profile
        metadata["data_profile"] = {
            "total_records": stats.get("freq", 0) * analyzer_stats.get("total", 1),
            "frequency": stats.get("freq", 0),
            "unique_count": stats.get("unique_count", 0),
            "uniqueness_ratio": stats.get("uniqueness_ratio", 0.0),
            "is_unique_field": stats.get("is_unique_field", False),
            "has_nested_data": stats.get("nested", False),
            "composite_score": stats.get("composite_score", 0.0),
            "stability_score": stats.get("stability", 0.0)
        }
        
        # Comprehensive type analysis
        type_info = stats.get("types", set())
        metadata["type_analysis"] = {
            "detected_types": list(type_info),
            "type_count": len(type_info),
            "has_type_ambiguity": stats.get("has_type_ambiguity", False),
            "ambiguity_score": stats.get("ambiguity_info", {}).get("ambiguity_score", 0.0),
            "primary_type": self._determine_primary_type(type_info),
            "type_consistency": self._calculate_type_consistency(type_info)
        }
        
        # Enhanced semantic analysis
        semantic_info = stats.get("semantic_info", {})
        metadata["semantic_analysis"] = {
            "detected_kind": semantic_info.get("detected_kind", "unknown"),
            "semantic_weight": semantic_info.get("semantic_weight", 0.0),
            "pattern_confidence": semantic_info.get("pattern_confidence", 0.0),
            "is_identifier": self._is_identifier_field(field_name, semantic_info),
            "is_measurement": self._is_measurement_field(field_name, semantic_info),
            "is_categorical": self._is_categorical_field(stats),
            "data_classification": self._classify_data_sensitivity(field_name),
            "avg_length": semantic_info.get("avg_length", 0),
            "max_length": semantic_info.get("max_length", 0),
            "is_long_text": semantic_info.get("is_long_text", False)
        }
        
        # Detailed placement reasoning
        metadata["placement_reasoning"] = {
            "reason": placement_info.get("reason", "unknown"),
            "confidence": placement_info.get("confidence", 0.0),
            "decision_factors": placement_info.get("signals", {}),
            "override_applied": False,
            "manual_review_needed": self._needs_manual_review(stats, placement_info)
        }
        
        # Drift tracking information
        drift_info = stats.get("drift_analysis", {})
        metadata["drift_tracking"] = {
            "drift_score": drift_info.get("drift_score", 0.0),
            "should_quarantine": stats.get("should_quarantine", False),
            "quarantine_reason": stats.get("quarantine_reason", "none"),
            "drift_history": drift_info.get("drift_history", []),
            "stability_trend": self._analyze_stability_trend(stats)
        }
        
        # Quality metrics
        metadata["quality_metrics"] = {
            "completeness": self._calculate_completeness(stats),
            "consistency": self._calculate_consistency(stats),
            "validity": self._calculate_validity(stats, semantic_info),
            "accuracy_estimate": self._estimate_accuracy(field_name, stats),
            "data_quality_score": 0.0  # Will be calculated
        }
        
        # Calculate overall data quality score
        metadata["quality_metrics"]["data_quality_score"] = self._calculate_overall_quality_score(
            metadata["quality_metrics"]
        )
        
        # Usage statistics
        metadata["usage_statistics"] = {
            "access_frequency": "high" if stats.get("freq", 0) > 0.7 else "medium" if stats.get("freq", 0) > 0.3 else "low",
            "criticality": self._assess_field_criticality(field_name, stats),
            "business_importance": self._assess_business_importance(field_name),
            "query_optimization_potential": self._assess_query_optimization(stats),
            "indexing_recommendation": self._recommend_indexing(field_name, stats)
        }
        
        # Schema evolution tracking
        if self._schema_changed(field_name, stats):
            metadata["schema_evolution"].append({
                "timestamp": current_time,
                "change_type": "type_evolution",
                "old_types": metadata["type_analysis"].get("detected_types", []),
                "new_types": list(type_info),
                "impact_assessment": "medium"
            })
        
        # Business context (can be enhanced later)
        metadata["business_context"] = {
            "domain": self._infer_business_domain(field_name),
            "privacy_level": self._assess_privacy_level(field_name),
            "retention_policy": self._suggest_retention_policy(field_name),
            "compliance_tags": self._identify_compliance_requirements(field_name)
        }

        # Structural metadata for routing + schema decisions
        metadata["structural_profile"] = self._build_structural_profile(
            field_name,
            stats,
            metadata
        )
    
    def _determine_primary_type(self, types: Set) -> str:
        """Determine the primary/dominant type"""
        if not types:
            return "unknown"
        if len(types) == 1:
            return list(types)[0]
        # Logic to determine primary type when multiple exist
        type_priority = ['str', 'int', 'float', 'bool', 'dict', 'list']
        for t in type_priority:
            if t in types:
                return t
        return list(types)[0]
    
    def _calculate_type_consistency(self, types: Set) -> float:
        """Calculate type consistency score"""
        if not types:
            return 0.0
        return 1.0 - (len(types) - 1) * 0.2  # Penalize multiple types
    
    def _is_identifier_field(self, field_name: str, semantic_info: Dict) -> bool:
        """Check if field appears to be an identifier"""
        id_keywords = ['id', 'uuid', 'key', 'token', 'session']
        return any(keyword in field_name.lower() for keyword in id_keywords)
    
    def _is_measurement_field(self, field_name: str, semantic_info: Dict) -> bool:
        """Check if field appears to be a measurement"""
        measurement_keywords = ['temperature', 'pressure', 'speed', 'altitude', 'usage', 'rate', 'level']
        return any(keyword in field_name.lower() for keyword in measurement_keywords)
    
    def _is_categorical_field(self, stats: Dict) -> bool:
        """Check if field appears to be categorical"""
        uniqueness_ratio = stats.get("uniqueness_ratio", 1.0)
        return uniqueness_ratio < 0.1  # Less than 10% unique values suggests categorical
    
    def _classify_data_sensitivity(self, field_name: str) -> str:
        """Classify data sensitivity level"""
        sensitive_fields = ['email', 'phone', 'address', 'name', 'ssn', 'credit']
        public_fields = ['city', 'country', 'weather', 'timezone']
        
        field_lower = field_name.lower()
        if any(sensitive in field_lower for sensitive in sensitive_fields):
            return "sensitive"
        elif any(public in field_lower for public in public_fields):
            return "public"
        else:
            return "internal"
    
    def _needs_manual_review(self, stats: Dict, placement_info: Dict) -> bool:
        """Determine if field needs manual review"""
        return (
            stats.get("has_type_ambiguity", False) or
            placement_info.get("confidence", 1.0) < 0.7 or
            stats.get("should_quarantine", False)
        )
    
    def _analyze_stability_trend(self, stats: Dict) -> str:
        """Analyze stability trend"""
        stability = stats.get("stability", 1.0)
        if stability > 0.9:
            return "stable"
        elif stability > 0.7:
            return "mostly_stable"
        elif stability > 0.5:
            return "unstable"
        else:
            return "highly_unstable"
    
    def _calculate_completeness(self, stats: Dict) -> float:
        """Calculate data completeness score"""
        # Assume completeness based on frequency (could be enhanced)
        return min(1.0, stats.get("freq", 0.0) * 1.2)
    
    def _calculate_consistency(self, stats: Dict) -> float:
        """Calculate data consistency score"""
        return stats.get("stability", 0.0)
    
    def _calculate_validity(self, stats: Dict, semantic_info: Dict) -> float:
        """Calculate data validity score"""
        semantic_weight = semantic_info.get("semantic_weight", 0.0)
        type_consistency = self._calculate_type_consistency(stats.get("types", set()))
        return (semantic_weight + type_consistency) / 2.0
    
    def _estimate_accuracy(self, field_name: str, stats: Dict) -> float:
        """Estimate data accuracy"""
        # Basic heuristic - could be enhanced with validation rules
        base_accuracy = 0.8
        if stats.get("has_type_ambiguity", False):
            base_accuracy -= 0.2
        if stats.get("should_quarantine", False):
            base_accuracy -= 0.3
        return max(0.0, base_accuracy)
    
    def _calculate_overall_quality_score(self, quality_metrics: Dict) -> float:
        """Calculate overall data quality score"""
        weights = {
            "completeness": 0.3,
            "consistency": 0.25,
            "validity": 0.25,
            "accuracy_estimate": 0.2
        }
        
        total_score = sum(
            quality_metrics.get(metric, 0.0) * weight
            for metric, weight in weights.items()
        )
        return round(total_score, 3)
    
    def _assess_field_criticality(self, field_name: str, stats: Dict) -> str:
        """Assess field criticality for business operations"""
        critical_fields = ['id', 'user', 'timestamp', 'status', 'amount', 'payment']
        field_lower = field_name.lower()
        
        if any(critical in field_lower for critical in critical_fields):
            return "critical"
        elif stats.get("freq", 0.0) > 0.8:
            return "important"
        else:
            return "standard"
    
    def _assess_business_importance(self, field_name: str) -> str:
        """Assess business importance of field"""
        high_importance = ['revenue', 'customer', 'user', 'transaction', 'order']
        field_lower = field_name.lower()
        
        if any(important in field_lower for important in high_importance):
            return "high"
        else:
            return "medium"
    
    def _assess_query_optimization(self, stats: Dict) -> str:
        """Assess potential for query optimization"""
        if stats.get("is_unique_field", False):
            return "excellent"
        elif stats.get("uniqueness_ratio", 0.0) > 0.7:
            return "good"
        elif stats.get("freq", 0.0) > 0.8:
            return "moderate"
        else:
            return "low"
    
    def _recommend_indexing(self, field_name: str, stats: Dict) -> Dict:
        """Recommend indexing strategy"""
        recommendations = {
            "should_index": False,
            "index_type": "none",
            "reasoning": ""
        }
        
        if self._is_identifier_field(field_name, {}):
            recommendations.update({
                "should_index": True,
                "index_type": "primary_or_unique",
                "reasoning": "Identifier field - excellent for indexing"
            })
        elif stats.get("uniqueness_ratio", 0.0) > 0.7:
            recommendations.update({
                "should_index": True,
                "index_type": "standard",
                "reasoning": "High uniqueness - good index candidate"
            })
        elif stats.get("freq", 0.0) > 0.9:
            recommendations.update({
                "should_index": True,
                "index_type": "standard",
                "reasoning": "High frequency - likely queried often"
            })
        else:
            recommendations["reasoning"] = "Low indexing priority"
        
        return recommendations
    
    def _schema_changed(self, field_name: str, stats: Dict) -> bool:
        """Check if schema has changed for field"""
        if field_name not in self.field_metadata:
            return False
        
        old_types = set(self.field_metadata[field_name].get("type_analysis", {}).get("detected_types", []))
        new_types = stats.get("types", set())
        return old_types != new_types
    
    def _infer_business_domain(self, field_name: str) -> str:
        """Infer business domain of field"""
        domains = {
            "user_management": ['user', 'name', 'email', 'phone', 'profile'],
            "location": ['city', 'country', 'address', 'gps', 'timezone'],
            "device": ['device', 'os', 'version', 'battery', 'signal'],
            "analytics": ['timestamp', 'session', 'event', 'metric'],
            "health": ['heart_rate', 'steps', 'sleep', 'stress'],
            "commerce": ['purchase', 'payment', 'item', 'subscription']
        }
        
        field_lower = field_name.lower()
        for domain, keywords in domains.items():
            if any(keyword in field_lower for keyword in keywords):
                return domain
        
        return "general"
    
    def _assess_privacy_level(self, field_name: str) -> str:
        """Assess privacy level requirements"""
        pii_fields = ['email', 'phone', 'name', 'address']
        sensitive_fields = ['location', 'gps', 'health', 'biometric']
        
        field_lower = field_name.lower()
        if any(pii in field_lower for pii in pii_fields):
            return "pii"
        elif any(sensitive in field_lower for sensitive in sensitive_fields):
            return "sensitive"
        else:
            return "standard"
    
    def _suggest_retention_policy(self, field_name: str) -> str:
        """Suggest data retention policy"""
        privacy_level = self._assess_privacy_level(field_name)
        
        if privacy_level == "pii":
            return "7_years"
        elif privacy_level == "sensitive":
            return "3_years"
        else:
            return "indefinite"
    
    def _identify_compliance_requirements(self, field_name: str) -> List[str]:
        """Identify potential compliance requirements"""
        tags = []
        privacy_level = self._assess_privacy_level(field_name)
        
        if privacy_level == "pii":
            tags.extend(["GDPR", "CCPA"])
        
        if "health" in field_name.lower() or "medical" in field_name.lower():
            tags.append("HIPAA")
        
        if "payment" in field_name.lower() or "credit" in field_name.lower():
            tags.append("PCI_DSS")
        
        return tags

    def _build_structural_profile(self, field_name: str, stats: Dict, metadata: Dict) -> Dict[str, Any]:
        """Build structural metadata for routing, storage, and relationships."""
        placement = metadata.get("placement_decision", "unknown")
        primary_type = metadata.get("type_analysis", {}).get("primary_type", "unknown")
        nesting_level = self._derive_nesting_level(field_name, stats)
        parent_field = stats.get("parent_field") or self._derive_parent_field(field_name)
        storage_engine = self._infer_storage_engine(placement)
        destination = self._infer_storage_destination(field_name, parent_field, storage_engine, nesting_level)
        foreign_keys = self._infer_foreign_keys(field_name, parent_field, nesting_level)
        primary_key = self._infer_primary_key(field_name, parent_field, nesting_level)
        is_array = bool(
            stats.get("is_array")
            or primary_type.startswith("array")
            or "list" in primary_type
            or field_name.endswith("[]")
        )

        return {
            "field": field_name,
            "field_path": field_name,
            "canonical_name": self._to_identifier(field_name),
            "data_type": primary_type,
            "nest_level": nesting_level,
            "parent": parent_field,
            "storage": storage_engine.upper(),
            "storage_engine": storage_engine,
            "table_or_collection": destination,
            "foreign_key": foreign_keys[0]["field"] if foreign_keys else None,
            "key_relationships": {
                "primary_key": primary_key,
                "foreign_keys": foreign_keys
            },
            "is_array": is_array
        }

    def _derive_nesting_level(self, field_name: str, stats: Dict) -> int:
        if "nesting_level" in stats:
            return int(stats["nesting_level"])
        if "nest_level" in stats:
            return int(stats["nest_level"])
        clean = field_name.replace("[]", "")
        return max(0, clean.count('.'))

    def _derive_parent_field(self, field_name: str) -> Optional[str]:
        tokens = [self._clean_token(token) for token in field_name.replace("[]", "").split('.') if token]
        if len(tokens) > 1:
            return tokens[-2]
        return None

    def _clean_token(self, token: str) -> str:
        return token.replace("[]", "").strip()

    def _infer_storage_engine(self, placement_decision: str) -> str:
        mapping = {
            "sql": "sql",
            "mysql": "sql",
            "mongo": "mongo",
            "mongodb": "mongo",
            "buffer": "buffer"
        }
        return mapping.get((placement_decision or "").lower(), "unknown")

    def _infer_storage_destination(self, field_name: str, parent_field: str, storage_engine: str, nesting_level: int) -> str:
        if storage_engine == "sql":
            if nesting_level == 0:
                return "logs"
            anchor = parent_field or field_name.split('.')[0]
            return self._to_identifier(anchor)
        elif storage_engine == "mongo":
            anchor = field_name.split('.')[0]
            return self._to_identifier(anchor)
        elif storage_engine == "buffer":
            return "buffer_queue"
        return "unassigned"

    def _to_identifier(self, value: str) -> str:
        if not value:
            return "field"
        cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", value)
        cleaned = re.sub(r"_+", "_", cleaned).strip('_')
        return cleaned.lower() or "field"

    def _infer_primary_key(self, field_name: str, parent_field: str, nesting_level: int) -> Optional[str]:
        token = field_name.lower()
        if token in {"id", "_id"} or token.endswith("_id"):
            return field_name
        if nesting_level == 0 and parent_field:
            return f"{self._to_identifier(parent_field)}_id"
        return None

    def _infer_foreign_keys(self, field_name: str, parent_field: str, nesting_level: int) -> List[Dict[str, str]]:
        foreign_keys: List[Dict[str, str]] = []
        token = field_name.lower()
        if token.endswith("_id") and parent_field:
            foreign_keys.append({
                "field": field_name,
                "references": parent_field,
                "relationship": "identifier"
            })
            return foreign_keys

        candidate = self._find_related_identifier(parent_field)
        if candidate:
            foreign_keys.append({
                "field": candidate,
                "references": parent_field or "root",
                "relationship": "foreign_key"
            })
        return foreign_keys

    def _find_related_identifier(self, parent_field: str) -> Optional[str]:
        candidates = self._find_identifier_candidates()
        if not candidates:
            return None
        if parent_field:
            for _, name in candidates:
                if parent_field in name:
                    return name
        return candidates[0][1]

    def _find_identifier_candidates(self) -> List:
        candidates = []
        for name in self.field_metadata.keys():
            lowered = name.lower()
            if lowered.endswith("_id") or lowered in {"id", "_id"}:
                candidates.append((self._derive_nesting_level(name, {}), name))
        candidates.sort(key=lambda item: (item[0], item[1]))
        return candidates
    
    def save_metadata(self):
        """Save enhanced metadata to file"""
        try:
            with open(self.metadata_file, 'w') as f:
                json.dump(self.field_metadata, f, indent=2, default=str)
            print(f"Saved enhanced metadata for {len(self.field_metadata)} fields to {self.metadata_file}")
        except Exception as e:
            print(f"Error saving metadata: {e}")
    
    def get_simple_placement_decisions(self):
        """Get simple field -> placement mapping for backward compatibility"""
        return {
            field_name: field_data.get('placement_decision', 'mongo')
            for field_name, field_data in self.field_metadata.items()
        }
    
    def get_field_summary(self, field_name: str) -> Dict:
        """Get comprehensive summary for a specific field"""
        if field_name not in self.field_metadata:
            return {"error": "Field not found in metadata"}
        
        metadata = self.field_metadata[field_name]
        structural = metadata.get("structural_profile", {})
        return {
            "field_name": field_name,
            "placement": metadata["placement_decision"],
            "data_quality_score": metadata["quality_metrics"]["data_quality_score"],
            "type_stability": "stable" if len(metadata["type_analysis"]["detected_types"]) == 1 else "ambiguous",
            "business_criticality": metadata["usage_statistics"]["criticality"],
            "privacy_level": metadata["business_context"]["privacy_level"],
            "indexing_recommended": metadata["usage_statistics"]["indexing_recommendation"]["should_index"],
            "manual_review_needed": metadata["placement_reasoning"]["manual_review_needed"],
            "storage_engine": structural.get("storage_engine"),
            "table_or_collection": structural.get("table_or_collection"),
            "nest_level": structural.get("nest_level")
        }
    
    def get_quality_report(self) -> Dict:
        """Generate overall data quality report"""
        if not self.field_metadata:
            return {"error": "No metadata available"}
        
        quality_scores = [
            field["quality_metrics"]["data_quality_score"]
            for field in self.field_metadata.values()
            if "quality_metrics" in field
        ]
        
        return {
            "total_fields": len(self.field_metadata),
            "average_quality_score": statistics.mean(quality_scores) if quality_scores else 0.0,
            "fields_needing_review": sum(
                1 for field in self.field_metadata.values()
                if field.get("placement_reasoning", {}).get("manual_review_needed", False)
            ),
            "type_ambiguous_fields": sum(
                1 for field in self.field_metadata.values()
                if field.get("type_analysis", {}).get("has_type_ambiguity", False)
            ),
            "high_drift_fields": sum(
                1 for field in self.field_metadata.values()
                if field.get("drift_tracking", {}).get("should_quarantine", False)
            )
        }

    def get_structural_registry(self) -> List[Dict[str, Any]]:
        """Return a flattened view of structural metadata for every field."""
        registry: List[Dict[str, Any]] = []
        for entry in self.field_metadata.values():
            structural = entry.get("structural_profile")
            if structural:
                registry.append(structural)
        return registry
    
    def export_schema_recommendations(self) -> Dict:
        """Export schema and optimization recommendations"""
        mysql_fields = []
        mongodb_fields = []
        indexing_recommendations = []
        
        for field_name, metadata in self.field_metadata.items():
            placement = metadata.get("placement_decision", "unknown")
            
            if placement == "sql":
                mysql_fields.append({
                    "field": field_name,
                    "type": metadata["type_analysis"]["primary_type"],
                    "nullable": metadata["quality_metrics"]["completeness"] < 1.0,
                    "index_recommended": metadata["usage_statistics"]["indexing_recommendation"]["should_index"]
                })
            elif placement == "mongo":
                mongodb_fields.append({
                    "field": field_name,
                    "reason": metadata["placement_reasoning"]["reason"],
                    "type_ambiguity": metadata["type_analysis"]["has_type_ambiguity"]
                })
            
            if metadata["usage_statistics"]["indexing_recommendation"]["should_index"]:
                indexing_recommendations.append({
                    "field": field_name,
                    "database": placement,
                    "index_type": metadata["usage_statistics"]["indexing_recommendation"]["index_type"],
                    "reasoning": metadata["usage_statistics"]["indexing_recommendation"]["reasoning"]
                })
        
        return {
            "mysql_schema": mysql_fields,
            "mongodb_collections": mongodb_fields,
            "indexing_recommendations": indexing_recommendations,
            "generated_at": datetime.datetime.now().isoformat()
        }