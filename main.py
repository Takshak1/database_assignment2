from ingestion import stream_records
from normalize import normalize_record
from analyzer import Analyzer
from classifier import classify_with_placement_heuristics, get_placement_summary
from storage_manager import StorageManager
from metadata_manager import MetadataManager
import json

print("=" * 80)
print("           ADAPTIVE INGESTION & HYBRID BACKEND PLACEMENT")
print("=" * 80)

analyzer = Analyzer()
storage = StorageManager()
metadata_mgr = MetadataManager()  

print(f"Enhanced metadata system initialized with {len(metadata_mgr.field_metadata)} detailed field profiles")

metadata = metadata_mgr.get_simple_placement_decisions()
print(f"Extracted {len(metadata)} placement decisions for pipeline compatibility")

print("\n" + "-" * 40)
print("CONNECTING TO BACKENDS")
print("-" * 40)
if not storage.connect():
    print("Database connection failed. Exiting.")
    exit(1)

if metadata:
    storage.initialize_schema(metadata)

print("\n" + "-" * 40)
print("PROCESSING RECORDS")
print("-" * 40)
stats_counter = {'total': 0, 'sql_stored': 0, 'mongo_stored': 0}

try:
    for i, record in enumerate(stream_records(batch_size=10, delay=1)):
        stats_counter['total'] += 1
        
        analyzer.update(record)
        stats = analyzer.get_stats()
        
        if len(stats) > 0:  
            current_decisions, placement_reasons = classify_with_placement_heuristics(stats)
            
            if i < 10 and 'detailed_placement' not in locals():
                detailed_placement = placement_reasons
            
            analyzer_total_stats = {"total": analyzer.total}
            for field_name, field_stats in stats.items():
                field_placement = placement_reasons.get(field_name, {})
                metadata_mgr.update_field_metadata(field_name, field_stats, field_placement, analyzer_total_stats)
        else:
            current_decisions = {}
            placement_reasons = {}
        
        for field, decision in current_decisions.items():
            if field not in metadata:
                metadata[field] = decision
        
        if (i + 1) % 10 == 0:
            metadata.update(metadata_mgr.get_simple_placement_decisions())
        
        if (i + 1) % 10 == 0:
            metadata_mgr.save_metadata()
        
        sql_id, mongo_id = storage.store_record(record, metadata)
        
        if sql_id:
            stats_counter['sql_stored'] += 1
        if mongo_id:
            stats_counter['mongo_stored'] += 1
        
        if (i + 1) % 10 == 0:
            counts = storage.get_stats()
            print(f"Processed: {i+1} | SQL: {counts['sql']} | MongoDB: {counts['mongo']}")
        
        if i < 3:
            sql_fields = [f for f, d in metadata.items() if d == 'sql']
            mongo_fields = [f for f, d in metadata.items() if d == 'mongo']
            print(f"\nRecord #{i+1}:")
            print(f"  SQL fields: {sql_fields}")
            print(f"  Mongo fields: {mongo_fields}")
            print(f"  IDs: SQL={sql_id}, Mongo={mongo_id}")
        
        if i >= 49:
            print(f"Processed {i+1} records, stopping.")
            break

except KeyboardInterrupt:
    print("\nInterrupted by user")

finally:
    metadata_mgr.save_metadata()
    
    final_counts = storage.get_stats()
    print("\n" + "=" * 80)
    print("                              FINAL SUMMARY")
    print("=" * 80)
    print(f"Records processed:     {stats_counter['total']:>8}")
    print(f"SQL records stored:    {final_counts['sql']:>8}")
    print(f"MongoDB docs stored:   {final_counts['mongo']:>8}")
    print(f"Enhanced metadata saved:      {'metadata.json':>8}")
    print("=" * 80)
    
    ambiguity_report = analyzer.get_normalization_report()
    print("\n" + "=" * 80)
    print("                        TYPE AMBIGUITY ANALYSIS")
    print("=" * 80)
    print(f"Total fields processed:          {ambiguity_report['total_fields']:>8}")
    print(f"Fields with type ambiguity:      {ambiguity_report['fields_with_type_ambiguity']:>8}")
    print(f"Clean fields (single type):      {len(ambiguity_report['clean_fields']):>8}")
    
    if ambiguity_report["ambiguous_fields"]:
        print(f"\nTYPE AMBIGUOUS FIELDS (routed to MongoDB):")
        for field_name, ambiguity_info in ambiguity_report["ambiguous_fields"].items():
            types_str = ", ".join(ambiguity_info["types_detected"])
            print(f"  '{field_name}' has mixed types: [{types_str}]")
    
    if ambiguity_report["clean_fields"]:
        print(f"\nCLEAN FIELDS (suitable for MySQL):")
        clean_count = 0
        for field_name, field_info in ambiguity_report["clean_fields"].items():
            if clean_count < 5:  
                print(f"  '{field_name}': {field_info['type']} ({field_info['count']} records)")
                clean_count += 1
        if len(ambiguity_report["clean_fields"]) > 5:
            print(f"  ... and {len(ambiguity_report['clean_fields']) - 5} more clean fields")
    
    if not ambiguity_report["ambiguous_fields"]:
        print("\nNo type ambiguities detected - all fields have consistent types")
    
    uniqueness_analysis = analyzer.analyze_field_uniqueness()
    print("\n" + "=" * 80)
    print("                      FIELD UNIQUENESS ANALYSIS")
    print("=" * 80)
    
    if uniqueness_analysis["unique_fields"]:
        print(f"\nUNIQUE FIELDS ({len(uniqueness_analysis['unique_fields'])}):")
        for field_info in uniqueness_analysis["unique_fields"]:
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
    
    if uniqueness_analysis["semi_unique_fields"]:
        print(f"\nSEMI-UNIQUE FIELDS ({len(uniqueness_analysis['semi_unique_fields'])}):")
        for field_info in uniqueness_analysis["semi_unique_fields"]:
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
    
    if uniqueness_analysis["common_fields"]:
        print(f"\nCOMMON FIELDS ({len(uniqueness_analysis['common_fields'])}):")
        for field_info in uniqueness_analysis["common_fields"][:5]:  
            print(f"  {field_info['field']}: {field_info['uniqueness_ratio']:.1%} unique "
                  f"({field_info['unique_values']}/{field_info['total_occurrences']} values)")
        if len(uniqueness_analysis["common_fields"]) > 5:
            print(f"    ... and {len(uniqueness_analysis['common_fields']) - 5} more")
    
    print("=" * 70)
    
    if 'detailed_placement' in locals():
        placement_summary = get_placement_summary(detailed_placement)
        print("\n" + "=" * 80)
        print("                   PLACEMENT HEURISTICS ANALYSIS")
        print("=" * 80)
        
        print(f"\nPLACEMENT OVERVIEW:")
        print(f"  Total fields analyzed:      {placement_summary['total_fields']:>8}")
        print(f"  SQL assignments:            {placement_summary['sql_decisions']:>8}")
        print(f"  MongoDB assignments:        {placement_summary['mongo_decisions']:>8}")
        
        print(f"\nCOMPOSITE SCORE DISTRIBUTION:")
        print(f"  High scores (>=0.8):        {placement_summary['score_distribution']['high']:>8}")
        print(f"  Medium scores (0.5-0.8):    {placement_summary['score_distribution']['medium']:>8}")
        print(f"  Low scores (<0.5):          {placement_summary['score_distribution']['low']:>8}")
        
        if placement_summary["high_confidence_sql"]:
            print(f"\nHIGH-CONFIDENCE SQL PLACEMENTS:")
            for item in placement_summary["high_confidence_sql"][:8]:  
                signals = detailed_placement[item['field']]['signals']
                print(f"  {item['field']}: {item['semantic_type']} "
                      f"(freq={signals['freq']:.2f}, stability={signals['stability']:.2f}, "
                      f"score={signals['composite_score']:.2f})")
        
        print(f"\nPLACEMENT REASONING BREAKDOWN:")
        for reason, fields in placement_summary['placement_breakdown'].items():
            if len(fields) <= 5:
                fields_str = ", ".join(fields)
            else:
                fields_str = ", ".join(fields[:5]) + f" + {len(fields)-5} more"
            print(f"  {reason}: {fields_str}")
        
        if placement_summary['semantic_distribution']:
            print(f"\nSEMANTIC TYPE DISTRIBUTION:")
            for sem_type, counts in placement_summary['semantic_distribution'].items():
                total = counts['sql'] + counts['mongo']
                print(f"  {sem_type}: {total} fields -> SQL: {counts['sql']}, MongoDB: {counts['mongo']}")
    
    print("=" * 80)
    
    drift_summary = analyzer.get_drift_summary()
    if drift_summary['total_fields_tracked'] > 0:
        print("\n" + "=" * 80)
        print("                    MIXED DATA HANDLING (TYPE DRIFT)")
        print("=" * 80)
        
        print(f"\nDRIFT OVERVIEW:")
        print(f"  Fields tracked for drift:   {drift_summary['total_fields_tracked']:>8}")
        print(f"  Quarantined fields:         {drift_summary['quarantined_fields']:>8}")
        print(f"  High drift fields:          {len(drift_summary['high_drift_fields']):>8}")
        print(f"  Stable fields:              {len(drift_summary['stable_fields']):>8}")
        
        if drift_summary['high_drift_fields']:
            print(f"\nHIGH DRIFT FIELDS (quarantined to MongoDB):")
            for field_info in drift_summary['high_drift_fields'][:5]:
                field = field_info['field']
                drift_score = field_info['drift_score']
                type_shares = field_info['type_shares']
                patterns = field_info['flip_patterns']
                
                types_str = ', '.join([f"{t}({s:.0%})" for t, s in type_shares.items()])
                print(f"  {field}: drift_score={drift_score:.2f}, types=[{types_str}]")
                
                if patterns:
                    print(f"    Patterns: {', '.join(patterns)}")
        
        if drift_summary['quarantine_list']:
            print(f"\nQUARANTINED FIELDS: {', '.join(drift_summary['quarantine_list'])}")
            print("    (These fields routed to MongoDB to prevent SQL schema conflicts)")
        
        if drift_summary['drift_patterns']:
            print(f"\nDETECTED FLIP PATTERNS:")
            for pattern, fields in drift_summary['drift_patterns'].items():
                fields_str = ', '.join(fields[:5])  
                if len(fields) > 5:
                    fields_str += f" + {len(fields)-5} more"
                print(f"  {pattern}: {fields_str}")
    
    quality_report = metadata_mgr.get_quality_report()
    print("\n" + "=" * 80)
    print("                     ENHANCED METADATA ANALYSIS")
    print("=" * 80)
    print(f"Total fields in metadata:        {quality_report['total_fields']:>8}")
    print(f"Average data quality score:      {quality_report['average_quality_score']:>8.3f}")
    print(f"Fields needing review:           {quality_report['fields_needing_review']:>8}")
    print(f"Type ambiguous fields:           {quality_report['type_ambiguous_fields']:>8}")
    print(f"High drift fields:               {quality_report['high_drift_fields']:>8}")
    
    print(f"\nSAMPLE FIELD PROFILES:")
    field_count = 0
    for field_name in metadata_mgr.field_metadata:
        if field_count < 5:  
            summary = metadata_mgr.get_field_summary(field_name)
            print(f"  {field_name}:")
            print(f"    Placement: {summary['placement']}")
            print(f"    Quality Score: {summary['data_quality_score']:.3f}")
            print(f"    Type Stability: {summary['type_stability']}")
            print(f"    Business Criticality: {summary['business_criticality']}")
            print(f"    Privacy Level: {summary['privacy_level']}")
            print(f"    Indexing Recommended: {summary['indexing_recommended']}")
            if summary['manual_review_needed']:
                print(f"       Manual Review Required")
            field_count += 1
        else:
            break
    
    schema_recommendations = metadata_mgr.export_schema_recommendations()
    print(f"\n" + "=" * 80)
    print("                      SCHEMA RECOMMENDATIONS")
    print("=" * 80)
    
    print(f"\nMYSQL SCHEMA RECOMMENDATIONS ({len(schema_recommendations['mysql_schema'])}):")
    for field in schema_recommendations['mysql_schema'][:10]:  
        nullable = "NULL" if field['nullable'] else "NOT NULL"
        index_note = " [INDEX]" if field['index_recommended'] else ""
        print(f"  {field['field']}: {field['type'].upper()} {nullable}{index_note}")
    
    print(f"\nMONGODB COLLECTIONS ({len(schema_recommendations['mongodb_collections'])}):")
    for field in schema_recommendations['mongodb_collections'][:10]:  
        reason_note = f" ({field['reason']})"
        ambiguity_note = " [TYPE AMBIGUOUS]" if field['type_ambiguity'] else ""
        print(f"  {field['field']}{reason_note}{ambiguity_note}")
    
    print(f"\nINDEXING RECOMMENDATIONS ({len(schema_recommendations['indexing_recommendations'])}):")
    for rec in schema_recommendations['indexing_recommendations']:
        print(f"  {rec['database'].upper()}: {rec['field']} -> {rec['index_type']}")
        print(f"    Reason: {rec['reasoning']}")
    
    print("=" * 80)
    
    if final_counts['sql'] > 0 and final_counts['mongo'] > 0:
        storage.demonstrate_bi_temporal_join()
    else:
        print("\nNo records processed - bi-temporal join demo requires data in both backends")
    
    storage.close()
    print("\nPipeline completed successfully")
