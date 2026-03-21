import json
from metadata_manager import MetadataManager
import sys
from collections import Counter

def main():
    metadata_mgr = MetadataManager()
    
    if not metadata_mgr.field_metadata:
        print("No enhanced metadata found. Run the main pipeline first.")
        return
    
    print("=" * 80)
    print("                 ENHANCED METADATA ANALYSIS REPORT")
    print("=" * 80)
    
    # Overall Statistics
    quality_report = metadata_mgr.get_quality_report()
    print(f"\nOVERALL STATISTICS:")
    print(f"  Total Fields: {quality_report['total_fields']}")
    print(f"  Average Quality Score: {quality_report['average_quality_score']:.3f}")
    print(f"  Fields Needing Review: {quality_report['fields_needing_review']}")
    print(f"  Type Ambiguous Fields: {quality_report['type_ambiguous_fields']}")
    print(f"  High Drift Fields: {quality_report['high_drift_fields']}")
    
    # Placement Distribution
    placements = [field['placement_decision'] for field in metadata_mgr.field_metadata.values()]
    placement_counter = Counter(placements)
    print(f"\nPLACEMENT DISTRIBUTION:")
    for placement, count in placement_counter.items():
        percentage = (count / len(placements)) * 100
        print(f"  {placement.upper()}: {count} fields ({percentage:.1f}%)")
    
    # Business Domain Analysis
    domains = [field['business_context']['domain'] for field in metadata_mgr.field_metadata.values()]
    domain_counter = Counter(domains)
    print(f"\nBUSINESS DOMAIN DISTRIBUTION:")
    for domain, count in domain_counter.most_common():
        percentage = (count / len(domains)) * 100
        print(f"  {domain}: {count} fields ({percentage:.1f}%)")
    
    # Privacy Analysis
    privacy_levels = [field['business_context']['privacy_level'] for field in metadata_mgr.field_metadata.values()]
    privacy_counter = Counter(privacy_levels)
    print(f"\nPRIVACY LEVEL DISTRIBUTION:")
    for level, count in privacy_counter.items():
        percentage = (count / len(privacy_levels)) * 100
        print(f"  {level.upper()}: {count} fields ({percentage:.1f}%)")
    
    # Data Quality Analysis
    print(f"\nDATA QUALITY ANALYSIS:")
    quality_scores = [field['quality_metrics']['data_quality_score'] for field in metadata_mgr.field_metadata.values()]
    
    high_quality = len([score for score in quality_scores if score >= 0.8])
    medium_quality = len([score for score in quality_scores if 0.5 <= score < 0.8])
    low_quality = len([score for score in quality_scores if score < 0.5])
    
    print(f"  High Quality (>=0.8): {high_quality} fields")
    print(f"  Medium Quality (0.5-0.8): {medium_quality} fields")
    print(f"  Low Quality (<0.5): {low_quality} fields")
    
    # Indexing Recommendations
    indexing_recommended = [
        field for field in metadata_mgr.field_metadata.values()
        if field['usage_statistics']['indexing_recommendation']['should_index']
    ]
    print(f"\nINDEXING RECOMMENDATIONS:")
    print(f"  Fields recommended for indexing: {len(indexing_recommended)}")
    
    # Critical Fields Analysis
    critical_fields = [
        field for field in metadata_mgr.field_metadata.values()
        if field['usage_statistics']['criticality'] == 'critical'
    ]
    print(f"\nCRITICAL FIELDS ANALYSIS:")
    print(f"  Critical fields identified: {len(critical_fields)}")
    
    # Type Ambiguity Details
    ambiguous_fields = [
        field for field in metadata_mgr.field_metadata.values()
        if field['type_analysis']['has_type_ambiguity']
    ]
    print(f"\nTYPE AMBIGUITY DETAILS:")
    print(f"  Fields with type ambiguity: {len(ambiguous_fields)}")
    if ambiguous_fields:
        print("  Examples:")
        for field in ambiguous_fields[:5]:  
            field_name = field['field_name']
            types = field['type_analysis']['detected_types']
            ambiguity_score = field['type_analysis']['ambiguity_score']
            print(f"    {field_name}: {types} (score: {ambiguity_score:.3f})")
    
    # Manual Review Required
    review_fields = [
        field for field in metadata_mgr.field_metadata.values()
        if field['placement_reasoning']['manual_review_needed']
    ]
    print(f"\nMANUAL REVIEW REQUIRED:")
    print(f"  Fields needing manual review: {len(review_fields)}")
    if review_fields:
        print("  Fields:")
        for field in review_fields:
            field_name = field['field_name']
            reason = field['placement_reasoning']['reason']
            confidence = field['placement_reasoning']['confidence']
            print(f"    {field_name}: {reason} (confidence: {confidence:.3f})")
    
    # Compliance Requirements
    compliance_fields = {}
    for field in metadata_mgr.field_metadata.values():
        tags = field['business_context']['compliance_tags']
        for tag in tags:
            if tag not in compliance_fields:
                compliance_fields[tag] = []
            compliance_fields[tag].append(field['field_name'])
    
    if compliance_fields:
        print(f"\nCOMPLIANCE REQUIREMENTS:")
        for compliance, fields in compliance_fields.items():
            print(f"  {compliance}: {len(fields)} fields")
            print(f"    Examples: {', '.join(fields[:3])}")
            if len(fields) > 3:
                print(f"    ... and {len(fields) - 3} more")
    
    schema_recs = metadata_mgr.export_schema_recommendations()
    print(f"\nSCHEMA RECOMMENDATIONS SUMMARY:")
    print(f"  MySQL fields: {len(schema_recs['mysql_schema'])}")
    print(f"  MongoDB fields: {len(schema_recs['mongodb_collections'])}")
    print(f"  Indexing recommendations: {len(schema_recs['indexing_recommendations'])}")
    
    print("=" * 80)

def export_detailed_report():
    metadata_mgr = MetadataManager()
    
    if not metadata_mgr.field_metadata:
        print("No enhanced metadata found.")
        return
    
    report = {
        "summary": metadata_mgr.get_quality_report(),
        "schema_recommendations": metadata_mgr.export_schema_recommendations(),
        "field_details": {}
    }
    
    for field_name, field_data in metadata_mgr.field_metadata.items():
        report["field_details"][field_name] = metadata_mgr.get_field_summary(field_name)
    
    with open("detailed_metadata_report.json", "w") as f:
        json.dump(report, f, indent=2, default=str)
    
    print("Detailed report exported to detailed_metadata_report.json")
    print("Note: Enhanced metadata is now stored in metadata.json")

def show_field_detail(field_name):
    metadata_mgr = MetadataManager()
    
    if field_name not in metadata_mgr.field_metadata:
        print(f"Field '{field_name}' not found in metadata.")
        available_fields = list(metadata_mgr.field_metadata.keys())[:10]
        print(f"Available fields (first 10): {available_fields}")
        return
    
    field_data = metadata_mgr.field_metadata[field_name]
    
    print("=" * 60)
    print(f"           DETAILED FIELD ANALYSIS: {field_name}")
    print("=" * 60)
    
    print(f"\nBASIC INFO:")
    print(f"  Field Name: {field_data['field_name']}")
    print(f"  Placement Decision: {field_data['placement_decision']}")
    print(f"  Created: {field_data['creation_timestamp']}")
    print(f"  Last Updated: {field_data['last_updated']}")
    
    print(f"\nDATA PROFILE:")
    profile = field_data['data_profile']
    print(f"  Total Records: {profile['total_records']}")
    print(f"  Frequency: {profile['frequency']:.3f}")
    print(f"  Unique Count: {profile['unique_count']}")
    print(f"  Uniqueness Ratio: {profile['uniqueness_ratio']:.3f}")
    print(f"  Has Nested Data: {profile['has_nested_data']}")
    print(f"  Composite Score: {profile['composite_score']:.3f}")
    print(f"  Stability Score: {profile['stability_score']:.3f}")
    
    print(f"\nTYPE ANALYSIS:")
    type_info = field_data['type_analysis']
    print(f"  Detected Types: {type_info['detected_types']}")
    print(f"  Has Type Ambiguity: {type_info['has_type_ambiguity']}")
    print(f"  Ambiguity Score: {type_info['ambiguity_score']:.3f}")
    print(f"  Primary Type: {type_info['primary_type']}")
    
    print(f"\nSEMANTIC ANALYSIS:")
    semantic = field_data['semantic_analysis']
    print(f"  Detected Kind: {semantic['detected_kind']}")
    print(f"  Is Identifier: {semantic['is_identifier']}")
    print(f"  Is Measurement: {semantic['is_measurement']}")
    print(f"  Is Categorical: {semantic['is_categorical']}")
    print(f"  Data Classification: {semantic['data_classification']}")
    
    print(f"\nPLACEMENT REASONING:")
    reasoning = field_data['placement_reasoning']
    print(f"  Reason: {reasoning['reason']}")
    print(f"  Confidence: {reasoning['confidence']:.3f}")
    print(f"  Manual Review Needed: {reasoning['manual_review_needed']}")
    
    print(f"\nQUALITY METRICS:")
    quality = field_data['quality_metrics']
    print(f"  Overall Quality Score: {quality['data_quality_score']:.3f}")
    print(f"  Completeness: {quality['completeness']:.3f}")
    print(f"  Consistency: {quality['consistency']:.3f}")
    print(f"  Validity: {quality['validity']:.3f}")
    print(f"  Accuracy Estimate: {quality['accuracy_estimate']:.3f}")
    
    print(f"\nBUSINESS CONTEXT:")
    business = field_data['business_context']
    print(f"  Domain: {business['domain']}")
    print(f"  Privacy Level: {business['privacy_level']}")
    print(f"  Compliance Tags: {business['compliance_tags']}")
    print(f"  Retention Policy: {business['retention_policy']}")
    
    print("=" * 60)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    elif len(sys.argv) == 2:
        if sys.argv[1] == "export":
            export_detailed_report()
        else:
            show_field_detail(sys.argv[1])
    else:
        print("Usage:")
        print("  python analyze_metadata.py              # Show overview")
        print("  python analyze_metadata.py export       # Export detailed report")
        print("  python analyze_metadata.py <field_name> # Show field details")