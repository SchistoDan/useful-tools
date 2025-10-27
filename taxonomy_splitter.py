#!/usr/bin/env python3
"""
NCBI Taxonomy CSV Splitter

This script takes a CSV file containing specimen information with NCBI taxonomy IDs
and splits it into multiple files based on the phylum of each specimen.

The input CSV should have the format:
ID,forward,reverse,taxid,type_status

For each taxid, the script queries the NCBI Taxonomy database to determine the phylum.
It then creates separate output CSV files for each phylum found.

Output files are named: [input_file_name_without_ext]_[phylum].csv

Usage:
    python taxonomy_splitter.py --input your_input_file.csv --email your.email@example.com

Requirements:
    - biopython
"""

import argparse
import csv
import os
import time
import requests
from urllib.parse import quote
from Bio import Entrez

def parse_arguments():
    parser = argparse.ArgumentParser(description='Split CSV file by phylum based on taxids.')
    parser.add_argument('--input', required=True, help='Input CSV file path')
    parser.add_argument('--email', required=True, help='Email address for NCBI API access')
    return parser.parse_args()

def get_phylum_for_taxid(taxid, email):
    """Retrieve phylum for a given taxid using NCBI's API."""
    Entrez.email = email
    
    try:
        # First get the lineage for this taxid
        handle = Entrez.efetch(db="taxonomy", id=str(taxid), retmode="xml")
        records = Entrez.read(handle)
        handle.close()
        
        if not records or len(records) == 0:
            print(f"Warning: No taxonomy record found for taxid {taxid}")
            return "Unknown"
        
        # Find the phylum level in the lineage
        lineage = records[0].get('LineageEx', [])
        for entry in lineage:
            if entry.get('Rank') == 'phylum':
                return entry.get('ScientificName', 'Unknown')
        
        # If no phylum found in lineage, check if the taxon itself is a phylum
        if records[0].get('Rank') == 'phylum':
            return records[0].get('ScientificName', 'Unknown')
        
        print(f"Warning: No phylum found in lineage for taxid {taxid}")
        return "Unknown"
    
    except Exception as e:
        print(f"Error retrieving taxonomy for {taxid}: {str(e)}")
        return "Unknown"

def main():
    args = parse_arguments()
    
    input_file = args.input
    email = args.email
    
    # Get base filename for output
    input_basename = os.path.splitext(os.path.basename(input_file))[0]
    output_dir = os.path.dirname(input_file) or '.'
    
    # Read the input CSV and group by phylum
    phylum_data = {}
    phylum_mapping = {}  # Cache taxid to phylum mapping to avoid repeated API calls
    
    with open(input_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)  # Get the header
        
        for row in reader:
            if len(row) < 5:
                print(f"Warning: Skipping invalid row: {row}")
                continue
                
            taxid = row[3]
            
            # Get phylum, using cache if available
            if taxid in phylum_mapping:
                phylum = phylum_mapping[taxid]
            else:
                phylum = get_phylum_for_taxid(taxid, email)
                phylum_mapping[taxid] = phylum
                # Be nice to NCBI's API with a short delay
                time.sleep(0.5)
            
            # Add row to the appropriate phylum group
            if phylum not in phylum_data:
                phylum_data[phylum] = [header]  # Initialise with header
            phylum_data[phylum].append(row)
            
            print(f"Processed taxid {taxid}: {row[0]} belongs to phylum {phylum}")
    
    # Write output files for each phylum
    for phylum, rows in phylum_data.items():
        # Create safe filename
        safe_phylum = "".join([c if c.isalnum() else "_" for c in phylum])
        output_file = os.path.join(output_dir, f"{input_basename}_{safe_phylum}.csv")
        
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerows(rows)
        
        print(f"Created file {output_file} with {len(rows)-1} entries")

if __name__ == "__main__":
    main()
