#!/usr/bin/env python3
"""
NCBI Taxonomy File Splitter

This script takes a CSV, TSV, or XLSX file containing specimen information with NCBI 
taxonomy IDs and splits it into multiple files based on a specified taxonomic rank.

The input file should have columns including a 'taxid' column (or taxid in position 4).

For each taxid, the script queries the NCBI Taxonomy database to determine the 
taxonomic rank (default: phylum). It then creates separate output files for each 
unique value of that rank.

Output files are named: [input_file_name_without_ext]_[rank_value].[original_extension]

Usage:
    python split_by_rank.py --input your_input_file.csv --email your.email@example.com
    python split_by_rank.py --input data.tsv --email you@example.com --rank order
    python split_by_rank.py --input data.xlsx --email you@example.com -r class

Requirements:
    - biopython
    - openpyxl (for xlsx support)
"""

import argparse
import csv
import os
import time
from Bio import Entrez

def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Split CSV/TSV/XLSX file by taxonomic rank based on taxids.'
    )
    parser.add_argument('--input', '-i', required=True, help='Input file path (CSV, TSV, or XLSX)')
    parser.add_argument('--email', '-e', required=True, help='Email address for NCBI API access')
    parser.add_argument(
        '--rank', '-r', 
        default='phylum', 
        help='Taxonomic rank to split on (default: phylum). Case-insensitive.'
    )
    return parser.parse_args()


def detect_file_type(filepath):
    """Detect file type based on extension and content."""
    ext = os.path.splitext(filepath)[1].lower()
    
    if ext == '.xlsx':
        return 'xlsx', None
    
    # For text files, detect delimiter
    with open(filepath, 'r', newline='') as f:
        sample = f.read(8192)
        
    # Use csv.Sniffer to detect delimiter
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',\t')
        delimiter = dialect.delimiter
    except csv.Error:
        # Default to comma if detection fails
        delimiter = ','
    
    if delimiter == '\t' or ext == '.tsv':
        return 'tsv', '\t'
    else:
        return 'csv', ','


def read_input_file(filepath):
    """Read input file and return header and rows."""
    file_type, delimiter = detect_file_type(filepath)
    
    if file_type == 'xlsx':
        try:
            import openpyxl
        except ImportError:
            raise ImportError("openpyxl is required for XLSX support. Install with: pip install openpyxl")
        
        wb = openpyxl.load_workbook(filepath)
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        header = list(rows[0])
        data_rows = [list(row) for row in rows[1:]]
        return header, data_rows, file_type, delimiter
    
    else:
        with open(filepath, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader)
            data_rows = [row for row in reader]
        return header, data_rows, file_type, delimiter


def write_output_file(filepath, header, rows, file_type, delimiter):
    """Write output file in the same format as input."""
    if file_type == 'xlsx':
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(header)
        for row in rows:
            ws.append(row)
        wb.save(filepath)
    else:
        with open(filepath, 'w', newline='') as f:
            writer = csv.writer(f, delimiter=delimiter)
            writer.writerow(header)
            writer.writerows(rows)


def get_extension(file_type):
    """Return appropriate file extension for file type."""
    if file_type == 'xlsx':
        return '.xlsx'
    elif file_type == 'tsv':
        return '.tsv'
    else:
        return '.csv'


def get_rank_for_taxid(taxid, email, target_rank):
    """Retrieve taxonomic rank value for a given taxid using NCBI's API."""
    Entrez.email = email
    target_rank_lower = target_rank.lower()
    
    try:
        handle = Entrez.efetch(db="taxonomy", id=str(taxid), retmode="xml")
        records = Entrez.read(handle)
        handle.close()
        
        if not records or len(records) == 0:
            print(f"Warning: No taxonomy record found for taxid {taxid}")
            return "Unknown"
        
        # Find the target rank in the lineage
        lineage = records[0].get('LineageEx', [])
        for entry in lineage:
            if entry.get('Rank', '').lower() == target_rank_lower:
                return entry.get('ScientificName', 'Unknown')
        
        # Check if the taxon itself is at the target rank
        if records[0].get('Rank', '').lower() == target_rank_lower:
            return records[0].get('ScientificName', 'Unknown')
        
        print(f"Warning: No {target_rank} found in lineage for taxid {taxid}")
        return "Unknown"
    
    except Exception as e:
        print(f"Error retrieving taxonomy for {taxid}: {str(e)}")
        return "Unknown"


def main():
    args = parse_arguments()
    
    input_file = args.input
    email = args.email
    target_rank = args.rank.lower()
    
    # Get base filename for output
    input_basename = os.path.splitext(os.path.basename(input_file))[0]
    output_dir = os.path.dirname(input_file) or '.'
    
    # Read the input file
    print(f"Reading input file: {input_file}")
    header, data_rows, file_type, delimiter = read_input_file(input_file)
    print(f"Detected file type: {file_type}" + (f" (delimiter: {'tab' if delimiter == chr(9) else repr(delimiter)})" if delimiter else ""))
    print(f"Splitting by rank: {target_rank}")
    
    # Group rows by taxonomic rank
    rank_data = {}
    rank_mapping = {}  # Cache taxid to rank value mapping
    
    for row in data_rows:
        if len(row) < 4:
            print(f"Warning: Skipping invalid row: {row}")
            continue
        
        taxid = row[3]
        
        # Skip empty taxids
        if not taxid or str(taxid).strip() == '':
            print(f"Warning: Skipping row with empty taxid: {row[0] if row else 'unknown'}")
            continue
        
        # Get rank value, using cache if available
        if taxid in rank_mapping:
            rank_value = rank_mapping[taxid]
        else:
            rank_value = get_rank_for_taxid(taxid, email, target_rank)
            rank_mapping[taxid] = rank_value
            # Be nice to NCBI's API with a short delay
            time.sleep(0.35)
        
        # Add row to the appropriate rank group
        if rank_value not in rank_data:
            rank_data[rank_value] = []
        rank_data[rank_value].append(row)
        
        print(f"Processed taxid {taxid}: {row[0]} belongs to {target_rank} {rank_value}")
    
    # Write output files for each rank value
    extension = get_extension(file_type)
    for rank_value, rows in rank_data.items():
        # Create safe filename
        safe_rank_value = "".join([c if c.isalnum() else "_" for c in str(rank_value)])
        output_file = os.path.join(output_dir, f"{input_basename}_{safe_rank_value}{extension}")
        
        write_output_file(output_file, header, rows, file_type, delimiter)
        
        print(f"Created file {output_file} with {len(rows)} entries")
    
    print(f"\nDone! Created {len(rank_data)} files.")


if __name__ == "__main__":
    main()
