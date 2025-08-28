#!/usr/bin/env python3
"""
CSV/TSV Combiner - A script to combine multiple CSV or TSV files into a single file.

Features:
- Preserves header structure from first CSV file
- Aligns data from subsequent files to match the first file's column order
- Adds empty values for missing columns with appropriate warnings

Usage: 
python merge_c-tsv_files.py -i input1.csv input2.csv input3.csv -o output.csv

Arguments:
-i, --input: List of input CSV files to combine (required)
-o, --output: Output CSV file path (required)
"""

import csv
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Set

def detect_delimiter(file_path: str, sample_size: int = 1024) -> str:
    """Detect whether file uses comma or tab delimiter."""
    with open(file_path, 'r', encoding='utf-8') as f:
        sample = f.read(sample_size)
    
    # Count occurrences of potential delimiters
    comma_count = sample.count(',')
    tab_count = sample.count('\t')
    
    # Return the more frequent delimiter
    return '\t' if tab_count > comma_count else ','

def get_headers(file_path: str, delimiter: str) -> List[str]:
    """Extract headers from a CSV/TSV file."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        headers = next(reader)
        return [header.strip() for header in headers]

def merge_files(input_files: List[str], output_file: str) -> None:
    """
    Merge multiple CSV/TSV files into one with unified headers.
    
    Args:
        input_files: List of input file paths
        output_file: Output file path
    """
    if not input_files:
        print("Error: No input files provided", file=sys.stderr)
        sys.exit(1)
    
    # Detect delimiter from first file
    delimiter = detect_delimiter(input_files[0])
    print(f"Detected delimiter: {'tab' if delimiter == chr(9) else 'comma'}", file=sys.stderr)
    
    # Collect all unique headers from all files
    all_headers: Set[str] = set()
    file_headers: Dict[str, List[str]] = {}
    
    for file_path in input_files:
        if not Path(file_path).exists():
            print(f"Error: File not found: {file_path}", file=sys.stderr)
            sys.exit(1)
        
        try:
            headers = get_headers(file_path, delimiter)
            file_headers[file_path] = headers
            all_headers.update(headers)
            print(f"File {file_path}: {len(headers)} columns", file=sys.stderr)
        except Exception as e:
            print(f"Error reading {file_path}: {e}", file=sys.stderr)
            sys.exit(1)
    
    # Create ordered list of all unique headers
    unified_headers = sorted(list(all_headers))
    print(f"Total unique columns: {len(unified_headers)}", file=sys.stderr)
    
    # Open output file
    with open(output_file, 'w', encoding='utf-8') as output_handle:
        writer = csv.writer(output_handle, delimiter=delimiter)
        
        # Write unified header
        writer.writerow(unified_headers)
        
        # Process each input file
        for file_path in input_files:
            headers = file_headers[file_path]
            
            # Create mapping from file columns to unified columns
            header_mapping = {header: unified_headers.index(header) for header in headers}
            
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                next(reader)  # Skip header row
                
                row_count = 0
                for row in reader:
                    # Create output row with correct column order (empty strings for missing columns)
                    output_row = [''] * len(unified_headers)
                    
                    # Fill in values according to header mapping
                    for i, value in enumerate(row):
                        if i < len(headers):  # Protect against malformed rows
                            unified_index = header_mapping[headers[i]]
                            output_row[unified_index] = value
                    
                    writer.writerow(output_row)
                    row_count += 1
                
                print(f"Processed {row_count} rows from {file_path}", file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(
        description='Merge multiple CSV/TSV files with potentially different column orders',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python merge_c-tsv_files.py --input file1.csv file2.csv file3.csv --output merged.csv
  python merge_c-tsv_files.py --input data/*.csv --output combined.csv
  python merge_c-tsv_files.py --input data/*.tsv --output output.tsv
        '''
    )
    
    parser.add_argument('--input', nargs='+', required=True, help='Input CSV/TSV files to merge')
    parser.add_argument('--output', required=True, help='Output file path')
    
    args = parser.parse_args()
    
    merge_files(args.input, args.output)
    print(f"Successfully merged {len(args.input)} files into {args.output}", file=sys.stderr)

if __name__ == '__main__':
    main()
