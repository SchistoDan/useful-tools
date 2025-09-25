#!/usr/bin/env python3
"""
Compare headers between two FASTA files and output filtered results.

This script takes two FASTA files as input, extracts and compares their headers,
then outputs a filtered list based on the specified criteria.

Usage:
    python compare_fasta.py -i file1.fasta file2.fasta -o output.txt --filter keep
    python compare_fasta.py -i file1.fasta file2.fasta -o output.txt --filter remove

Filter modes:
    - keep: Output headers that ARE present in both files (intersection)
    - remove: Output headers that are NOT in both files (present in only one file)

Output format:
    - One header per line in a .txt file
    - Headers are written without the '>' character
    - Results are sorted alphabetically for consistency
"""

import argparse
import sys
from pathlib import Path


def read_fasta_headers(fasta_file):
    headers = set()
    
    try:
        with open(fasta_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('>'):
                    # Remove '>' and add to set
                    header = line[1:]
                    headers.add(header)
    except FileNotFoundError:
        print(f"Error: File '{fasta_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading file '{fasta_file}': {e}", file=sys.stderr)
        sys.exit(1)
    
    return headers


def main():
    parser = argparse.ArgumentParser(
        description="Compare headers between two FASTA files and output filtered results."
    )
    
    parser.add_argument(
        '-i', '--input',
        nargs=2,
        required=True,
        metavar=('FILE1', 'FILE2'),
        help='Two FASTA files to compare'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Output text file'
    )
    
    parser.add_argument(
        '--filter',
        choices=['keep', 'remove'],
        required=True,
        help='Filter mode: "keep" for headers in both files, "remove" for headers not in both files'
    )
    
    args = parser.parse_args()
    
    # Read headers from both files
    print(f"Reading headers from {args.input[0]}...")
    headers1 = read_fasta_headers(args.input[0])
    
    print(f"Reading headers from {args.input[1]}...")
    headers2 = read_fasta_headers(args.input[1])
    
    # Determine which headers to output based on filter mode
    if args.filter == 'keep':
        # Headers that ARE in both files (intersection)
        output_headers = headers1.intersection(headers2)
        print(f"Found {len(output_headers)} headers present in both files.")
    else:  # args.filter == 'remove'
        # Headers that are NOT in both files (symmetric difference)
        output_headers = headers1.symmetric_difference(headers2)
        print(f"Found {len(output_headers)} headers present in only one file.")
    
    # Write output
    try:
        with open(args.output, 'w') as f:
            for header in sorted(output_headers):  # Sort for consistent output
                f.write(f"{header}\n")
        
        print(f"Results written to {args.output}")
        
    except Exception as e:
        print(f"Error writing to output file '{args.output}': {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
