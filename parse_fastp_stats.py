#!/usr/bin/env python3

import argparse
import json
import csv
import os
import glob
import sys

def parse_fastp_json(json_path):
    """Parse fastp JSON file and extract specified statistics."""
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        stats = {}
        
        # Before filtering stats
        bf = data['summary']['before_filtering']
        stats.update({
            'before_total_reads': bf['total_reads'],
            'before_total_bases': bf['total_bases'],
            'before_q20_bases': bf['q20_bases'],
            'before_q30_bases': bf['q30_bases'],
            'before_q20_rate': bf['q20_rate'],
            'before_q30_rate': bf['q30_rate'],
            'before_gc_content': bf['gc_content']
        })
        
        # After filtering stats
        af = data['summary']['after_filtering']
        stats.update({
            'after_total_reads': af['total_reads'],
            'after_total_bases': af['total_bases'],
            'after_q20_bases': af['q20_bases'],
            'after_q30_bases': af['q30_bases'],
            'after_q20_rate': af['q20_rate'],
            'after_q30_rate': af['q30_rate'],
            'after_gc_content': af['gc_content']
        })
        
        # Filtering result stats
        fr = data['filtering_result']
        stats.update({
            'passed_filter_reads': fr['passed_filter_reads'],
            'low_quality_reads': fr['low_quality_reads'],
            'too_many_N_reads': fr['too_many_N_reads'],
            'too_short_reads': fr['too_short_reads'],
            'too_long_reads': fr['too_long_reads']
        })
        
        # Duplication rate
        stats['duplication_rate'] = data['duplication']['rate']
        
        # Insert size peak
        stats['insert_size_peak'] = data['insert_size']['peak']
        
        return stats
        
    except (FileNotFoundError, KeyError, json.JSONDecodeError) as e:
        print(f"Error parsing {json_path}: {e}", file=sys.stderr)
        return None

def find_fastp_files(trimmed_data_paths):
    """Find all fastp JSON files in the specified trimmed_data directories."""
    fastp_files = []
    
    for trimmed_data_path in trimmed_data_paths:
        if not os.path.exists(trimmed_data_path):
            print(f"Warning: Path does not exist: {trimmed_data_path}", file=sys.stderr)
            continue
            
        # Look for subdirectories in trimmed_data
        subdirs = [d for d in os.listdir(trimmed_data_path) 
                  if os.path.isdir(os.path.join(trimmed_data_path, d))]
        
        for subdir in subdirs:
            sample_name = subdir
            json_pattern = os.path.join(trimmed_data_path, subdir, f"{sample_name}_fastp_report.json")
            
            # Use glob to handle any potential wildcards, though we expect exact match
            matching_files = glob.glob(json_pattern)
            
            if matching_files:
                fastp_files.append((sample_name, matching_files[0]))
            else:
                print(f"Warning: No fastp JSON found for sample {sample_name} in {json_pattern}", file=sys.stderr)
    
    return fastp_files

def main():
    parser = argparse.ArgumentParser(
        description="Parse fastp JSON reports and compile statistics into a CSV file"
    )
    parser.add_argument(
        '-i', '--input', 
        nargs='+', 
        required=True,
        help='One or more paths to trimmed_data/ directories'
    )
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Output CSV file path'
    )
    
    args = parser.parse_args()
    
    # Find all fastp JSON files
    fastp_files = find_fastp_files(args.input)
    
    if not fastp_files:
        print("Error: No fastp JSON files found in any of the specified directories", file=sys.stderr)
        sys.exit(1)
    
    print(f"Found {len(fastp_files)} fastp JSON files to process")
    
    # Define CSV header
    header = [
        'sample_name',
        'before_total_reads', 'before_total_bases', 'before_q20_bases', 'before_q30_bases',
        'before_q20_rate', 'before_q30_rate', 'before_gc_content',
        'after_total_reads', 'after_total_bases', 'after_q20_bases', 'after_q30_bases', 
        'after_q20_rate', 'after_q30_rate', 'after_gc_content',
        'passed_filter_reads', 'low_quality_reads', 'too_many_N_reads', 
        'too_short_reads', 'too_long_reads',
        'duplication_rate', 'insert_size_peak'
    ]
    
    # Process files and write CSV
    with open(args.output, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(header)
        
        processed_count = 0
        for sample_name, json_path in fastp_files:
            stats = parse_fastp_json(json_path)
            
            if stats is not None:
                row = [sample_name] + [stats.get(col, 'NA') for col in header[1:]]
                writer.writerow(row)
                processed_count += 1
                print(f"Processed: {sample_name}")
            else:
                print(f"Skipped: {sample_name} (parsing failed)")
    
    print(f"\nCompleted! Processed {processed_count}/{len(fastp_files)} samples")
    print(f"Output written to: {args.output}")

if __name__ == "__main__":
    main()
