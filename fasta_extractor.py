#!/usr/bin/env python3
"""
FASTA Sequence Filter

This script extracts sequences from a multi-FASTA file based on a list of IDs.
It creates a new FASTA file with the matched sequences and outputs a CSV log
of which IDs were found and which weren't.

Inputs:
    -i, --input: Path to the input multi-FASTA file
    -o, --output: Path to the output FASTA file where matched sequences will be saved
    -l, --log: Path to the output CSV log file for tracking found/not found IDs
    -id, --ids: Path to a text file containing IDs to search for (one per line)

Outputs:
    1. A FASTA file containing only the sequences that match the provided IDs
    2. A CSV file with two columns: "Found" and "Not Found"
       - "Found" column lists IDs that were found in the input FASTA file
       - "Not Found" column lists IDs that were not found

Usage:
    python fasta_filter.py -i input.fasta -o filtered.fasta -l results.csv -id ids.txt

    # If your IDs are in a file called 'sample_ids.txt':
    python fasta_filter.py -i sequences.fasta -o matched_sequences.fasta -l id_report.csv -id sample_ids.txt

Notes:
    - IDs are matched if they appear anywhere in the FASTA header (partial match)
    - FASTA headers should start with '>' as per standard FASTA format
    - The script preserves the entire original header in the output file
"""

import argparse
import csv
import re
import sys
from pathlib import Path


def parse_arguments():
    parser = argparse.ArgumentParser(description="Filter FASTA sequences by IDs")
    parser.add_argument("-i", "--input", required=True, help="Input FASTA file")
    parser.add_argument("-o", "--output", required=True, help="Output FASTA file")
    parser.add_argument("-l", "--log", required=True, help="Output CSV log file")
    parser.add_argument("-id", "--ids", required=True, help="File containing IDs to search for, one per line")
    return parser.parse_args()


def read_ids_from_file(ids_file):
    try:
        with open(ids_file, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error reading IDs file: {e}", file=sys.stderr)
        sys.exit(1)


def filter_fasta(input_file, output_file, ids_to_find):
    found_ids = set()
    current_id = None
    current_sequence = []
    write_current = False
    
    try:
        with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
            for line in infile:
                line = line.strip()
                
                # If this is a header line
                if line.startswith('>'):
                    # If we were collecting a sequence and it matched, write it out
                    if write_current and current_sequence:
                        outfile.write(f">{current_id}\n")
                        outfile.write("\n".join(current_sequence) + "\n")
                    
                    # Reset for the new sequence
                    current_sequence = []
                    write_current = False
                    current_id = line[1:]  # Remove the '>' character
                    
                    # Check if any of our IDs match this header
                    for id_to_find in ids_to_find:
                        if id_to_find in line:
                            found_ids.add(id_to_find)
                            write_current = True
                            break
                
                # If this is a sequence line and we're collecting this sequence
                elif current_id is not None:
                    current_sequence.append(line)
            
            # Don't forget to write the last sequence if it matched
            if write_current and current_sequence:
                outfile.write(f">{current_id}\n")
                outfile.write("\n".join(current_sequence) + "\n")
    
    except Exception as e:
        print(f"Error processing FASTA file: {e}", file=sys.stderr)
        sys.exit(1)
    
    # Determine which IDs were not found
    not_found_ids = set(ids_to_find) - found_ids
    
    return list(found_ids), list(not_found_ids)


def write_log_csv(log_file, found_ids, not_found_ids):
    try:
        with open(log_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Write header
            writer.writerow(["Found", "Not Found"])
            
            # Prepare the data as rows
            max_rows = max(len(found_ids), len(not_found_ids))
            for i in range(max_rows):
                found = found_ids[i] if i < len(found_ids) else ""
                not_found = not_found_ids[i] if i < len(not_found_ids) else ""
                writer.writerow([found, not_found])
    
    except Exception as e:
        print(f"Error writing log file: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    args = parse_arguments()
    
    # Check if input file exists
    if not Path(args.input).exists():
        print(f"Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # Check if IDs file exists
    if not Path(args.ids).exists():
        print(f"IDs file not found: {args.ids}", file=sys.stderr)
        sys.exit(1)
    
    # Read IDs
    ids_to_find = read_ids_from_file(args.ids)
    
    # Process FASTA file
    found_ids, not_found_ids = filter_fasta(args.input, args.output, ids_to_find)
    
    # Write log
    write_log_csv(args.log, found_ids, not_found_ids)
    
    # Report statistics
    print(f"Processing complete.")
    print(f"Found: {len(found_ids)} sequences")
    print(f"Not found: {len(not_found_ids)} IDs")
    print(f"Results written to {args.output}")
    print(f"Log written to {args.log}")


if __name__ == "__main__":
    main()
