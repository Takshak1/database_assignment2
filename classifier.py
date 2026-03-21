import re
from datetime import datetime

def detect_value_types(field_name, values_sample):
    
    analysis = {
        "semantic_type": "unknown",
        "sql_preference": 0.5,  # 0.0 = MongoDB preferred, 1.0 = SQL preferred
        "patterns": [],
        "indexable": False,
        "relational": False
    }
    
    sample_values = list(values_sample)[:20]  
    
    email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    email_matches = sum(1 for v in sample_values if email_pattern.match(str(v)))
    
    ip_pattern = re.compile(r'^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$')
    ip_matches = sum(1 for v in sample_values if ip_pattern.match(str(v)))
    
    url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$', re.IGNORECASE)
    url_matches = sum(1 for v in sample_values if url_pattern.match(str(v)))
    
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    uuid_matches = sum(1 for v in sample_values if uuid_pattern.match(str(v)))
    
    timestamp_keywords = ['time', 'date', 'created', 'updated', 'stamp', 'at', 'when']
    is_timestamp_field = any(keyword in field_name.lower() for keyword in timestamp_keywords)
    
    id_keywords = ['id', 'key', 'ref', 'pk', 'fk']
    is_id_field = any(keyword in field_name.lower() for keyword in id_keywords)
    
    geo_keywords = ['lat', 'lon', 'gps', 'coord', 'city', 'country', 'zip', 'postal']
    is_geo_field = any(keyword in field_name.lower() for keyword in geo_keywords)
    
    total_samples = len(sample_values)
    if total_samples == 0:
        return analysis
    
    if email_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "email",
            "sql_preference": 0.9,
            "patterns": ["email_format"],
            "indexable": True,
            "relational": True
        })
    
    elif ip_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "ip_address",
            "sql_preference": 0.9,
            "patterns": ["ipv4_format"],
            "indexable": True,
            "relational": True
        })
    
    elif url_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "url",
            "sql_preference": 0.2,
            "patterns": ["url_format"],
            "indexable": False,
            "relational": False
        })
    
    elif uuid_matches / total_samples > 0.8:
        analysis.update({
            "semantic_type": "uuid",
            "sql_preference": 0.95,
            "patterns": ["uuid_format"],
            "indexable": True,
            "relational": True
        })
    
    elif is_timestamp_field:
        analysis.update({
            "semantic_type": "timestamp",
            "sql_preference": 0.85,
            "patterns": ["temporal_data"],
            "indexable": True,
            "relational": True
        })
    
    elif is_id_field:
        analysis.update({
            "semantic_type": "identifier",
            "sql_preference": 0.9,
            "patterns": ["identifier"],
            "indexable": True,
            "relational": True
        })
    
    elif is_geo_field:
        analysis.update({
            "semantic_type": "geographic",
            "sql_preference": 0.7,
            "patterns": ["geographic_data"],
            "indexable": True,
            "relational": True
        })
    
    numeric_count = sum(1 for v in sample_values if str(v).replace('.', '').replace('-', '').isdigit())
    if numeric_count / total_samples > 0.9:
        analysis.update({
            "semantic_type": "numeric",
            "sql_preference": 0.8,
            "patterns": ["numeric_data"],
            "indexable": True,
            "relational": True
        })
    
    return analysis

def classify(stats):

    decisions = {}
    classification_reasons = {}
    
    THRESHOLDS = {
        "very_high_freq": 0.9,     
        "high_freq": 0.7,          
        "medium_freq": 0.5,        
        "very_unique": 0.95,        
        "low_freq": 0.3,           
        "unique": 0.8,              
        "semi_unique": 0.6,         
        "common": 0.3               
    }
    
    for field, s in stats.items():
        freq = s["freq"]
        uniqueness = s.get("uniqueness_ratio", 0)
        types_count = len(s["types"])
        is_nested = s["nested"]
        unique_values = s.get("unique", set())
        
        semantic_analysis = detect_value_types(field, unique_values)
        
        decision = "mongo"  
        reason = "default"
        
        if s.get("has_type_ambiguity", False):
            decision = "mongo"
            reason = "type_ambiguity_detected"
            
        elif is_nested:
            decision = "mongo"
            reason = "nested_structure"
        
        elif semantic_analysis["sql_preference"] >= 0.9:
            decision = "sql"
            reason = f"semantic_{semantic_analysis['semantic_type']}"
        
        elif (uniqueness >= THRESHOLDS["very_unique"] and 
              freq >= THRESHOLDS["high_freq"] and 
              types_count == 1):
            decision = "sql"
            reason = "primary_key_candidate"
        
        elif (uniqueness >= THRESHOLDS["unique"] and 
              freq >= THRESHOLDS["medium_freq"] and 
              types_count == 1 and 
              semantic_analysis["relational"]):
            decision = "sql"
            reason = "foreign_key_candidate"
        
        elif (uniqueness >= THRESHOLDS["semi_unique"] and 
              freq >= THRESHOLDS["very_high_freq"] and 
              types_count == 1):
            decision = "sql"
            reason = "indexed_lookup"
        
        elif (uniqueness <= THRESHOLDS["common"] and 
              freq >= THRESHOLDS["high_freq"] and 
              types_count == 1 and 
              semantic_analysis["indexable"]):
            decision = "sql"
            reason = "category_indexed"
        
        elif (freq >= THRESHOLDS["medium_freq"] and 
              types_count == 1 and 
              not is_nested and 
              semantic_analysis["sql_preference"] >= 0.6):
            decision = "sql"
            reason = "structured_consistent"
        
        else:
            decision = "mongo"
            reason = "flexible_schema"
        
        decisions[field] = decision
        classification_reasons[field] = {
            "decision": decision,
            "reason": reason,
            "semantic_type": semantic_analysis["semantic_type"],
            "sql_preference": semantic_analysis["sql_preference"],
            "patterns": semantic_analysis["patterns"],
            "freq": freq,
            "uniqueness": uniqueness,
            "types_count": types_count
        }
    
    return decisions, classification_reasons

def get_classification_summary(classification_reasons):

    summary = {
        "total_fields": len(classification_reasons),
        "sql_fields": 0,
        "mongo_fields": 0,
        "by_reason": {},
        "by_semantic_type": {},
        "high_confidence_sql": [],
        "semantic_patterns": []
    }
    
    for field, info in classification_reasons.items():
        if info["decision"] == "sql":
            summary["sql_fields"] += 1
        else:
            summary["mongo_fields"] += 1
        
        reason = info["reason"]
        if reason not in summary["by_reason"]:
            summary["by_reason"][reason] = []
        summary["by_reason"][reason].append(field)
        
        semantic_type = info["semantic_type"]
        if semantic_type not in summary["by_semantic_type"]:
            summary["by_semantic_type"][semantic_type] = []
        summary["by_semantic_type"][semantic_type].append(field)
        
        if info["decision"] == "sql" and info["sql_preference"] >= 0.8:
            summary["high_confidence_sql"].append({
                "field": field,
                "semantic_type": semantic_type,
                "confidence": info["sql_preference"],
                "reason": reason
            })
        
        for pattern in info["patterns"]:
            if pattern not in summary["semantic_patterns"]:
                summary["semantic_patterns"].append(pattern)
    
    return summary


def classify_with_placement_heuristics(stats):

    decisions = {}
    placement_reasons = {}
    
    THRESHOLDS = {
        'sql_freq_min': 0.60,
        'sql_stability_min': 0.80,
        'semi_unique_min': 0.70,
        'semi_unique_freq_min': 0.50,
        'composite_score_threshold': 0.65,
        'long_text_threshold': 120
    }
    
    for field_name, s in stats.items():
        freq = s['freq']
        types_count = s['types_count']
        uniqueness_ratio = s['uniqueness_ratio']
        stability = s['stability']
        nested = s['nested']
        has_type_ambiguity = s['has_type_ambiguity']
        semantic_info = s['semantic_info']
        composite_score = s['composite_score']
        
        should_quarantine = s.get('should_quarantine', False)
        quarantine_reason = s.get('quarantine_reason', 'none')
        drift_analysis = s.get('drift_analysis', {})
        drift_score = drift_analysis.get('drift_score', 0.0)
        
        detected_kind = semantic_info['detected_kind']
        semantic_weight = semantic_info['semantic_weight']
        is_long_text = semantic_info['is_long_text']
        
        decision = "mongo"  
        reason = "default"
        confidence = 0.5
        
        if should_quarantine:
            decision = "mongo"
            reason = f"drift_quarantine_{quarantine_reason}"
            confidence = max(0.1, 0.9 - drift_score)  
            
        elif has_type_ambiguity:
            decision = "mongo"
            reason = "type_ambiguity_detected"
            confidence = 0.9 - (drift_score * 0.2) 
            
        elif nested:
            decision = "mongo" 
            reason = "nested_structure"
            confidence = 1.0
        elif is_long_text:
            decision = "mongo"
            reason = "long_text"
            confidence = 0.85
        elif detected_kind == 'json-like':
            decision = "mongo"
            reason = "json_like_structure"
            confidence = 0.9
            
        elif (not nested and 
              freq >= THRESHOLDS['sql_freq_min'] and 
              types_count == 1 and 
              stability >= THRESHOLDS['sql_stability_min'] and
              detected_kind in {'timestamp', 'ip', 'email', 'uuid', 'username'}):
            decision = "sql"
            reason = "sql_strong_candidate"
            confidence = 0.9
            
        elif (not nested and
              freq >= THRESHOLDS['sql_freq_min'] and
              types_count == 1 and
              stability >= THRESHOLDS['sql_stability_min'] and
              detected_kind == 'categorical'):
            decision = "sql"
            reason = "categorical_low_cardinality"
            confidence = 0.8
            
        elif (uniqueness_ratio >= THRESHOLDS['semi_unique_min'] and
              freq >= THRESHOLDS['semi_unique_freq_min'] and
              types_count == 1):
            decision = "sql"
            reason = "semi_unique_field"
            confidence = 0.75
            
        elif composite_score >= THRESHOLDS['composite_score_threshold']:
            decision = "sql"
            reason = "composite_score_threshold"
            confidence = min(0.9, composite_score)
            
        else:
            decision = "mongo"
            reason = "flexible_schema_default"
            confidence = 0.6
        
        decisions[field_name] = decision
        placement_reasons[field_name] = {
            'decision': decision,
            'reason': reason,
            'confidence': confidence,
            'signals': {
                'freq': freq,
                'uniqueness_ratio': uniqueness_ratio,
                'stability': stability,
                'semantic_type': detected_kind,
                'composite_score': composite_score,
                'types_count': types_count,
                'semantic_weight': semantic_weight,
                'drift_score': drift_score,
                'quarantine_reason': quarantine_reason if should_quarantine else None
            }
        }
    
    return decisions, placement_reasons


def get_placement_summary(placement_reasons):

    summary = {
        'total_fields': len(placement_reasons),
        'sql_decisions': 0,
        'mongo_decisions': 0,
        'high_confidence_sql': [],
        'placement_breakdown': {},
        'semantic_distribution': {},
        'score_distribution': {'high': 0, 'medium': 0, 'low': 0}
    }
    
    for field, info in placement_reasons.items():
        decision = info['decision']
        reason = info['reason']
        confidence = info['confidence']
        signals = info['signals']
        
        if decision == 'sql':
            summary['sql_decisions'] += 1
        else:
            summary['mongo_decisions'] += 1
            
        if reason not in summary['placement_breakdown']:
            summary['placement_breakdown'][reason] = []
        summary['placement_breakdown'][reason].append(field)
        
        semantic_type = signals['semantic_type']
        if semantic_type not in summary['semantic_distribution']:
            summary['semantic_distribution'][semantic_type] = {'sql': 0, 'mongo': 0}
        summary['semantic_distribution'][semantic_type][decision] += 1
        
        score = signals['composite_score']
        if score >= 0.8:
            summary['score_distribution']['high'] += 1
        elif score >= 0.5:
            summary['score_distribution']['medium'] += 1
        else:
            summary['score_distribution']['low'] += 1
            
        if decision == 'sql' and confidence >= 0.8:
            summary['high_confidence_sql'].append({
                'field': field,
                'confidence': confidence,
                'reason': reason,
                'score': score,
                'semantic_type': semantic_type
            })
    
    return summary

