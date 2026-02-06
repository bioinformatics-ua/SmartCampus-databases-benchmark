#!/usr/bin/env python3

import argparse
import json
import statistics
from pathlib import Path
from typing import List, Dict, Any, Optional

def parse_benchmark_file(file_path: str) -> Dict[str, Any]:
    """Parse a single benchmark JSON file."""
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data

def calculate_ingestion_stats(benchmark_files: List[str]) -> Dict[str, Dict[str, float]]:
    """Calculate averaged ingestion statistics for each database type."""
    # Group files by dbType first
    grouped_data = {}
    
    for file_path in benchmark_files:
        data = parse_benchmark_file(file_path)
        db_type = data.get('dbType', Path(file_path).stem)
        ingestion_data = data.get('ingestion', [])
        
        if not ingestion_data:
            continue
        
        if db_type not in grouped_data:
            grouped_data[db_type] = []
        
        grouped_data[db_type].append(ingestion_data)
    
    # Calculate averaged statistics for each database type
    ingestion_stats = {}
    
    for db_type, ingestion_data_list in grouped_data.items():
        all_durations = []
        all_records_per_batch = []
        all_ingestion_rates = []
        all_total_records = []
        all_total_durations = []
        
        for ingestion_data in ingestion_data_list:
            durations = [entry['durationMs'] for entry in ingestion_data]
            records = [entry['nRecords'] for entry in ingestion_data]
            
            # Calculate records per batch (incremental)
            records_per_batch = []
            for i in range(len(records)):
                if i == 0:
                    records_per_batch.append(records[i])
                else:
                    records_per_batch.append(records[i] - records[i-1])
            
            # Calculate ingestion rate (records per second)
            ingestion_rates = []
            for duration, batch_records in zip(durations, records_per_batch):
                if duration > 0:
                    rate = (batch_records * 1000) / duration  # Convert ms to seconds
                    ingestion_rates.append(rate)
            
            # Collect data for averaging
            all_durations.extend([max(0, d) for d in durations])
            all_records_per_batch.extend([max(0, r) for r in records_per_batch])
            all_ingestion_rates.extend([max(0, r) for r in ingestion_rates])
            all_total_records.append(records[-1] if records else 0)
            all_total_durations.append(sum(durations))
        
        # Calculate averaged statistics
        ingestion_stats[db_type] = {
            'median_duration_ms': statistics.median(all_durations) if all_durations else 0,
            'median_records_per_batch': statistics.median(all_records_per_batch) if all_records_per_batch else 0,
            'median_ingestion_rate': statistics.median(all_ingestion_rates) if all_ingestion_rates else 0,
            'total_records': statistics.mean(all_total_records) if all_total_records else 0,
            'total_duration_ms': statistics.mean(all_total_durations) if all_total_durations else 0,
            'file_count': len(ingestion_data_list)
        }
    
    return ingestion_stats

def calculate_query_stats(benchmark_files: List[str]) -> Dict[int, Dict[str, Any]]:
    """Calculate averaged query statistics for each query ID and database type."""
    # Group data by dbType and queryId first
    grouped_data = {}
    
    for file_path in benchmark_files:
        data = parse_benchmark_file(file_path)
        db_type = data.get('dbType', Path(file_path).stem)
        queries = data.get('queries', [])
        
        if db_type not in grouped_data:
            grouped_data[db_type] = {}
        
        for query in queries:
            query_id = query['queryId']
            duration_ms = query['durationMs']
            description = query.get('description', f'Query {query_id}')
            
            if query_id not in grouped_data[db_type]:
                grouped_data[db_type][query_id] = {
                    'description': description,
                    'durations': []
                }
            
            grouped_data[db_type][query_id]['durations'].append(duration_ms)
    
    # Calculate averaged statistics
    query_stats = {}
    
    for db_type, db_queries in grouped_data.items():
        for query_id, query_info in db_queries.items():
            if query_id not in query_stats:
                query_stats[query_id] = {
                    'description': query_info['description'],
                    'databases': {}
                }
            
            # Calculate average duration, excluding failed queries (-1)
            successful_durations = [d for d in query_info['durations'] if d >= 0]
            
            if successful_durations:
                avg_duration = sum(successful_durations) / len(successful_durations)
                query_stats[query_id]['databases'][db_type] = avg_duration
            else:
                # If all queries failed, use -1 to indicate failure
                query_stats[query_id]['databases'][db_type] = -1
    
    return query_stats

def calculate_speedups(data: Dict[str, Any], baseline_db: str) -> Dict[str, float]:
    """Calculate speedups relative to baseline database."""
    speedups = {}
    
    if baseline_db not in data:
        return speedups
    
    baseline_value = data[baseline_db]
    
    # For speedup calculation, treat negative baseline as 0
    if baseline_value < 0:
        baseline_value = 0
    
    for db_type, value in data.items():
        if db_type != baseline_db:
            # Skip negative values - they will be shown as N/A
            if value < 0:
                continue
            elif value == 0:
                # Handle 0ms case - treat as extremely fast (use 0.1ms for calculation)
                if baseline_value > 0:
                    speedup = baseline_value / 0.1
                    speedups[db_type] = speedup
            elif value > 0 and baseline_value > 0:
                # Normal speedup calculation
                speedup = baseline_value / value
                speedups[db_type] = speedup
    
    return speedups

def generate_speedup_report(benchmark_files: List[str], output_file: str = "speedup_report.md"):
    """Generate a comprehensive speedup report in Markdown format."""
    
    # Calculate statistics
    ingestion_stats = calculate_ingestion_stats(benchmark_files)
    query_stats = calculate_query_stats(benchmark_files)
    
    # Determine baseline database - use the one with most completed queries and slowest ingestion
    baseline_db = None
    if ingestion_stats and query_stats:
        # Count completed queries for each database
        db_query_counts = {}
        for db in ingestion_stats.keys():
            completed_queries = sum(1 for qid, qdata in query_stats.items() 
                                  if db in qdata['databases'] and qdata['databases'][db] >= 0)
            db_query_counts[db] = completed_queries
        
        # Find databases with the most completed queries
        max_queries = max(db_query_counts.values())
        candidates = [db for db, count in db_query_counts.items() if count == max_queries]
        
        # Among those with most queries, pick the slowest for ingestion
        if candidates:
            baseline_db = max(candidates, key=lambda db: ingestion_stats[db]['median_duration_ms'])
        else:
            # Fallback to slowest ingestion if no queries
            baseline_db = max(ingestion_stats.keys(), 
                             key=lambda db: ingestion_stats[db]['median_duration_ms'])
    
    # Generate report content
    report_lines = []
    report_lines.append("# Database Performance Speedup Report (Averaged Results)")
    report_lines.append("")
    report_lines.append(f"**Baseline Database:** {baseline_db}")
    report_lines.append(f"**Analysis Method:** Results averaged across multiple benchmark runs per database type")
    report_lines.append("")
    
    # Ingestion Performance Section
    report_lines.append("## Ingestion Performance")
    report_lines.append("")
    
    if ingestion_stats:
        report_lines.append("### Ingestion Statistics (Averaged)")
        report_lines.append("")
        report_lines.append("| Database | Median Duration (ms) | Median Records/Batch | Median Rate (records/s) | Avg Total Records | Avg Total Duration (ms) | Files |")
        report_lines.append("|----------|-------------------|-------------------|---------------------|---------------|---------------------|-------|")
        
        for db_type, stats in ingestion_stats.items():
            report_lines.append(f"| {db_type} | {stats['median_duration_ms']:.1f} | {stats['median_records_per_batch']:,.0f} | {stats['median_ingestion_rate']:,.0f} | {stats['total_records']:,.0f} | {stats['total_duration_ms']:,.0f} | {stats['file_count']} |")
        
        report_lines.append("")
        
        # Calculate ingestion speedups
        duration_data = {db: stats['median_duration_ms'] for db, stats in ingestion_stats.items()}
        rate_data = {db: stats['median_ingestion_rate'] for db, stats in ingestion_stats.items()}
        
        duration_speedups = calculate_speedups(duration_data, baseline_db)
        
        if duration_speedups:
            report_lines.append("### Ingestion Speedups")
            report_lines.append("")
            report_lines.append("| Database | Duration Speedup | Rate Improvement |")
            report_lines.append("|----------|------------------|------------------|")
            
            # Add baseline first
            report_lines.append(f"| {baseline_db} | 1.00x | 1.00x |")
            
            for db_type, speedup in duration_speedups.items():
                rate_improvement = rate_data[db_type] / rate_data[baseline_db] if rate_data[baseline_db] > 0 else 0
                report_lines.append(f"| {db_type} | {speedup:.2f}x | {rate_improvement:.2f}x |")
            
            report_lines.append("")
    
    # Query Performance Section
    report_lines.append("## Query Performance")
    report_lines.append("")
    
    if query_stats:
        # Calculate median speedups across all queries
        # Include 0 for failed/incomplete queries to ensure fair comparison
        all_speedups = {db: [] for db in ingestion_stats.keys() if db != baseline_db}
        
        report_lines.append("### Query Execution Times (Averaged)")
        report_lines.append("")
        report_lines.append("| Query ID | Description | " + " | ".join(f"{db} (ms)" for db in sorted(ingestion_stats.keys())) + " |")
        report_lines.append("|----------|-------------|" + "|".join(["-" * 12 for _ in ingestion_stats.keys()]) + "|")
        
        for query_id in sorted(query_stats.keys()):
            query_data = query_stats[query_id]
            description = query_data['description']
            
            row = f"| {query_id} | {description} |"
            for db in sorted(ingestion_stats.keys()):
                duration = query_data['databases'].get(db, -1)
                if duration >= 0:
                    if duration >= 1000:
                        row += f" {duration/1000:.1f}s |"
                    else:
                        row += f" {duration:.1f}ms |"
                else:
                    row += " N/A |"
            
            report_lines.append(row)
        
        report_lines.append("")
        
        # Calculate and display speedups for each query
        report_lines.append("### Query Speedups")
        report_lines.append("")
        report_lines.append("| Query ID | Description | " + " | ".join(f"{db} Speedup" for db in sorted(ingestion_stats.keys())) + " |")
        report_lines.append("|----------|-------------|" + "|".join(["-" * 12 for db in ingestion_stats.keys()]) + "|")
        
        for query_id in sorted(query_stats.keys()):
            query_data = query_stats[query_id]
            description = query_data['description']
            
            speedups = calculate_speedups(query_data['databases'], baseline_db)
            
            row = f"| {query_id} | {description} |"
            for db in sorted(ingestion_stats.keys()):
                if db == baseline_db:
                    # Baseline is always 1.00x
                    if baseline_db in query_data['databases']:
                        row += " 1.00x |"
                    else:
                        row += " N/A |"
                else:
                    if db in query_data['databases'] and query_data['databases'][db] < 0:
                        row += " N/A |"
                        # Add 0 for failed queries in median calculation
                        all_speedups[db].append(0)
                    else:
                        speedup = speedups.get(db, 0)
                        if speedup > 0:
                            row += f" {speedup:.2f}x |"
                            all_speedups[db].append(speedup)
                        else:
                            row += " N/A |"
                            # Add 0 for queries that didn't complete
                            all_speedups[db].append(0)
            
            report_lines.append(row)
        
        report_lines.append("")
        
        # Median speedups summary
        report_lines.append("### Median Query Speedups")
        report_lines.append("")
        report_lines.append("| Database | Median Speedup | Min Speedup | Max Speedup | Queries Analyzed |")
        report_lines.append("|----------|-----------------|-------------|-------------|------------------|")
        
        # Add baseline first
        baseline_query_count = sum(1 for qid, qdata in query_stats.items() if baseline_db in qdata['databases'] and qdata['databases'][baseline_db] >= 0)
        report_lines.append(f"| {baseline_db} | 1.00x | 1.00x | 1.00x | {baseline_query_count} |")
        
        for db in sorted(all_speedups.keys()):
            speedups_list = all_speedups[db]
            if speedups_list:
                median_speedup = statistics.median(speedups_list)
                # For min/max, only consider successful queries (> 0)
                successful_speedups = [s for s in speedups_list if s > 0]
                if successful_speedups:
                    min_speedup = min(successful_speedups)
                    max_speedup = max(successful_speedups)
                else:
                    min_speedup = 0
                    max_speedup = 0
                successful_count = len(successful_speedups)
                report_lines.append(f"| {db} | {median_speedup:.2f}x | {min_speedup:.2f}x | {max_speedup:.2f}x | {successful_count} |")
        
        report_lines.append("")
    
    # Summary Section
    report_lines.append("## Summary")
    report_lines.append("")
    
    if duration_speedups:
        best_ingestion_db = max(duration_speedups.keys(), key=lambda db: duration_speedups[db])
        best_ingestion_speedup = duration_speedups[best_ingestion_db]
        report_lines.append(f"- **Best Ingestion Performance:** {best_ingestion_db} ({best_ingestion_speedup:.2f}x faster than {baseline_db})")
    
    if all_speedups:
        median_speedups_summary = {db: statistics.median(speedups) for db, speedups in all_speedups.items() if speedups}
        if median_speedups_summary:
            best_query_db = max(median_speedups_summary.keys(), key=lambda db: median_speedups_summary[db])
            best_query_speedup = median_speedups_summary[best_query_db]
            report_lines.append(f"- **Best Median Query Performance:** {best_query_db} ({best_query_speedup:.2f}x faster than {baseline_db})")
    
    report_lines.append("")
    report_lines.append("---")
    report_lines.append(f"*Report generated from {len(benchmark_files)} benchmark files, averaged by database type*")
    
    # Add file count summary
    if ingestion_stats:
        report_lines.append("")
        report_lines.append("**File Count Summary:**")
        for db_type, stats in ingestion_stats.items():
            report_lines.append(f"- {db_type}: {stats['file_count']} files")
    
    # Write report to file
    with open(output_file, 'w') as f:
        f.write('\n'.join(report_lines))
    
    print(f"Speedup report generated: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Generate speedup report from benchmark JSON files')
    parser.add_argument('files', nargs='+', help='JSON benchmark files to process')
    parser.add_argument('-o', '--output', default='speedup_report.md', help='Output markdown file (default: speedup_report.md)')
    
    args = parser.parse_args()
    
    # Validate input files
    valid_files = []
    for file_path in args.files:
        if Path(file_path).exists():
            valid_files.append(file_path)
        else:
            print(f"Warning: File not found: {file_path}")
    
    if not valid_files:
        print("Error: No valid benchmark files found")
        return 1
    
    print(f"Processing {len(valid_files)} benchmark files (averaging by dbType)...")
    generate_speedup_report(valid_files, args.output)
    
    return 0

if __name__ == '__main__':
    exit(main())