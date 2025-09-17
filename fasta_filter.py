#!/usr/bin/env python3
"""
Script to filter a multi-FASTA file based on a list of headers to remove.
Creates two output files: one with remaining sequences and one with removed sequences.

Requires:
input_fasta: A multi-FASTA file with sequences to remove
headers_file: A txt file with 1 FASTA sequence header per line to find and remove in the input_fasta
-k/--keep: A multi-FASTA file = input_fasta - headers (and corresponding sequences) in headers_file
-r/--remove: A multi-FASTA file containing sequences removed from input_fasta
"""

import argparse
import sys
from pathlib import Path

def read_headers_to_remove(header_file):
    """Read headers from file and return as a set for fast lookup."""
    headers_to_remove = set()
    try:
        with open(header_file, 'r') as f:
            for line in f:
                header = line.strip()
                if header:
                    # Remove '>' if present, we'll handle that during comparison
                    if header.startswith('>'):
                        header = header[1:]
                    headers_to_remove.add(header)
        return headers_to_remove
    except FileNotFoundError:
        print(f"Error: Header file '{header_file}' not found.")
        sys.exit(1)

def filter_fasta(input_fasta, headers_to_remove, output_kept, output_removed):
    """Filter FASTA file based on headers to remove."""
    
    kept_count = 0
    removed_count = 0
    
    try:
        with open(input_fasta, 'r') as infile, \
             open(output_kept, 'w') as kept_file, \
             open(output_removed, 'w') as removed_file:
            
            current_header = ""
            current_sequence = []
            should_remove = False
            
            for line in infile:
                line = line.strip()
                
                if line.startswith('>'):
                    # Process previous sequence if any
                    if current_header:
                        sequence_text = '\n'.join(current_sequence)
                        if should_remove:
                            removed_file.write(f"{current_header}\n{sequence_text}\n")
                            removed_count += 1
                        else:
                            kept_file.write(f"{current_header}\n{sequence_text}\n")
                            kept_count += 1
                    
                    # Start new sequence
                    current_header = line
                    current_sequence = []
                    
                    # Check if this header should be removed
                    header_id = line[1:]  # Remove '>' character
                    should_remove = header_id in headers_to_remove
                    
                else:
                    # Add sequence line
                    if line:  # Skip empty lines
                        current_sequence.append(line)
            
            # Process the last sequence
            if current_header:
                sequence_text = '\n'.join(current_sequence)
                if should_remove:
                    removed_file.write(f"{current_header}\n{sequence_text}\n")
                    removed_count += 1
                else:
                    kept_file.write(f"{current_header}\n{sequence_text}\n")
                    kept_count += 1
                    
    except FileNotFoundError:
        print(f"Error: Input FASTA file '{input_fasta}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error processing files: {e}")
        sys.exit(1)
    
    return kept_count, removed_count

def main():
    parser = argparse.ArgumentParser(description='Filter multi-FASTA file based on header list')
    parser.add_argument('input_fasta', help='Input multi-FASTA file')
    parser.add_argument('headers_file', help='File containing headers to remove (one per line)')
    parser.add_argument('-k', '--kept', default='kept_sequences.fasta', 
                       help='Output file for kept sequences (default: kept_sequences.fasta)')
    parser.add_argument('-r', '--removed', default='removed_sequences.fasta',
                       help='Output file for removed sequences (default: removed_sequences.fasta)')
    
    args = parser.parse_args()
    
    # Read headers to remove
    print(f"Reading headers to remove from: {args.headers_file}")
    headers_to_remove = read_headers_to_remove(args.headers_file)
    print(f"Found {len(headers_to_remove)} headers to remove")
    
    # Filter FASTA file
    print(f"Processing FASTA file: {args.input_fasta}")
    kept_count, removed_count = filter_fasta(
        args.input_fasta, 
        headers_to_remove, 
        args.kept, 
        args.removed
    )
    
    print(f"\nResults:")
    print(f"  Sequences kept: {kept_count} -> {args.kept}")
    print(f"  Sequences removed: {removed_count} -> {args.removed}")
    print("Done!")

if __name__ == "__main__":
    main()
