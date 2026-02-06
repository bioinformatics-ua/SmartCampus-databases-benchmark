#!/usr/bin/env python3

import json
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
from typing import List, Dict, Any
import os

def parse_benchmark_files(file_paths: List[str]) -> Dict[str, Any]:
    """Parse benchmark JSON files and organize data by query ID, averaging results by dbType."""
    # First, collect all data grouped by dbType and queryId
    grouped_data = {}
    
    for file_path in file_paths:
        with open(file_path, 'r') as f:
            benchmark_data = json.load(f)
        
        db_type = benchmark_data.get('dbType', Path(file_path).stem)
        queries = benchmark_data.get('queries', [])
        
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
    
    # Now calculate averages and organize by query ID
    data = {}
    
    for db_type, db_queries in grouped_data.items():
        for query_id, query_info in db_queries.items():
            if query_id not in data:
                data[query_id] = {
                    'description': query_info['description'],
                    'databases': {}
                }
            
            # Calculate average duration, excluding failed queries (-1)
            successful_durations = [d for d in query_info['durations'] if d >= 0]
            
            if successful_durations:
                avg_duration = sum(successful_durations) / len(successful_durations)
                data[query_id]['databases'][db_type] = avg_duration
            else:
                # If all queries failed, use -1 to indicate failure
                data[query_id]['databases'][db_type] = -1
    
    return data

def create_query_barplots(benchmark_files: List[str], output_dir: str = "query_plots"):
    """Create individual barplots for each query ID showing averaged time per database."""
    
    # Create output directory if it doesn't exist
    Path(output_dir).mkdir(exist_ok=True)
    
    # Parse all benchmark data (now returns averaged results)
    query_data = parse_benchmark_files(benchmark_files)
    
    # Count files per database type for display
    db_file_counts = {}
    for file_path in benchmark_files:
        with open(file_path, 'r') as f:
            benchmark_data = json.load(f)
        db_type = benchmark_data.get('dbType', Path(file_path).stem)
        db_file_counts[db_type] = db_file_counts.get(db_type, 0) + 1
    
    # Get all database types for consistent ordering
    all_databases = set()
    for query_id, data in query_data.items():
        all_databases.update(data['databases'].keys())
    all_databases = sorted(list(all_databases))
    
    # Create a plot for each query ID
    for query_id in sorted(query_data.keys()):
        data = query_data[query_id]
        description = data['description']
        
        # Prepare data for plotting
        databases = []
        durations = []
        colors = []
        
        color_map = {
            'postgres': '#336791',
            'questdb': '#FF6B35', 
            'timescaledb': '#FDB462',
            'influxdb': '#22577A',
            'clickhouse': '#FF9F1C',
            'cratedb': '#2E8B57'
        }
        
        for db in all_databases:
            if db in data['databases']:
                duration = data['databases'][db]
                # Skip queries with -1 duration (failed/timeout)
                if duration >= 0:
                    databases.append(db)
                    durations.append(duration)
                    colors.append(color_map.get(db, '#888888'))
        
        # Skip empty queries (all failed)
        if not databases:
            print(f"Skipping Query {query_id}: No valid data")
            continue
        
        # Create the plot
        plt.figure(figsize=(12, 6))
        bars = plt.bar(databases, durations, color=colors, alpha=0.8, edgecolor='black', linewidth=0.5)
        
        # Add value labels on bars
        for bar, duration in zip(bars, durations):
            height = bar.get_height()
            if duration >= 1000:
                label = f'{duration/1000:.1f}s'
            else:
                label = f'{duration:.1f}ms'
            plt.text(bar.get_x() + bar.get_width()/2., height,
                    label, ha='center', va='bottom', fontweight='bold')
        
        # Customize the plot
        plt.title(f'Query {query_id}: {description} (Averaged Results)', fontsize=14, fontweight='bold', pad=20)
        plt.xlabel('Database', fontsize=12, fontweight='bold')
        plt.ylabel('Average Execution Time (ms)', fontsize=12, fontweight='bold')
        plt.yscale('log' if any(d > 1000 for d in durations) else 'linear')
        plt.grid(True, alpha=0.3, axis='y')
        plt.tight_layout()
        
        # Save the plot
        output_file = f"{output_dir}/query_{query_id:02d}_comparison.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"Created plot for Query {query_id}: {output_file}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Create barplot comparisons for each query ID from benchmark JSON files')
    parser.add_argument('files', nargs='+', help='JSON benchmark files to process')
    parser.add_argument('-o', '--output', default='query_plots', help='Output directory for the plots (default: query_plots)')
    
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
    create_query_barplots(valid_files, args.output)
    print(f"All averaged query comparison plots created successfully in {args.output}/!")
    
    return 0

if __name__ == '__main__':
    exit(main())