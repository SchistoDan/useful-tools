#!/usr/bin/env python3
"""
Parse BLAST results from multiple file formats into a single CSV with family names.

This script supports two BLAST result formats:
1. Custom TSV format (columns: ..., description, ..., identity_percent, ...)
2. Standard BLAST TSV format (outfmt 6 with stitle)

Features:
- Parses multiple file extensions (.tsv, .tsv.tsv, .txt, .out)
- Auto-detects format based on file content
- Extracts top 5 hits per sequence
- Queries ENA taxonomy API to get family names
- Outputs unified CSV format

Usage: python integrated_blast_parser.py <input_directory> [output.csv]
"""

import os
import sys
import csv
import glob
import re
import requests
import time
import json
from pathlib import Path

# Global cache for taxonomic lookups to avoid repeated API calls
TAXONOMY_CACHE = {}

def get_family_from_ena(species_name):
    """Query ENA taxonomy API to get family name for a species."""
    if not species_name or species_name in TAXONOMY_CACHE:
        return TAXONOMY_CACHE.get(species_name, "")
    
    try:
        # Clean up species name - take only first two words (genus species)
        words = species_name.strip().split()
        if len(words) >= 2:
            query_name = f"{words[0]} {words[1]}"
        else:
            query_name = species_name.strip()
        
        # Query ENA taxonomy API
        url = f"https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/{query_name}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                lineage = data[0].get('lineage', '')
                family = extract_family_from_lineage(lineage)
                TAXONOMY_CACHE[species_name] = family
                return family
        
        # If scientific name fails, try the any-name endpoint
        url = f"https://www.ebi.ac.uk/ena/taxonomy/rest/any-name/{query_name}"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                lineage = data[0].get('lineage', '')
                family = extract_family_from_lineage(lineage)
                TAXONOMY_CACHE[species_name] = family
                return family
                
    except Exception as e:
        print(f"Warning: Could not get taxonomy for '{species_name}': {e}", file=sys.stderr)
    
    # Cache empty result to avoid repeated failed queries
    TAXONOMY_CACHE[species_name] = ""
    return ""

def extract_family_from_lineage(lineage):
    """Extract family name from ENA lineage string using nomenclature conventions."""
    if not lineage:
        return ""
    
    taxa = [taxon.strip() for taxon in lineage.split(';') if taxon.strip()]
    
    # Standard family name endings by nomenclature code
    family_endings = ['idae', 'aceae', 'inae', 'oidae', 'ales']
    
    # Method 1: Look for taxa with standard family endings
    for taxon in taxa:
        for ending in family_endings:
            if taxon.lower().endswith(ending):
                return taxon
    
    # Method 2: Positional approach - family typically comes before genus
    if len(taxa) >= 2:
        high_level_taxa = {
            'eukaryota', 'metazoa', 'chordata', 'craniata', 'vertebrata', 
            'euteleostomi', 'mammalia', 'aves', 'reptilia', 'amphibia',
            'actinopterygii', 'arthropoda', 'mollusca', 'cnidaria',  
            'plantae', 'fungi', 'bacteria', 'archaea'
        }
        
        # Look for potential family 1-3 positions before genus (last taxon)
        for i in range(2, min(5, len(taxa))):
            potential_family = taxa[-i]
            if potential_family.lower() not in high_level_taxa:
                return potential_family
    
    return ""

# Functions from Script 1 (Custom TSV format)
def shorten_description(description):
    """Shorten description by keeping only the part before 'voucher' or 'isolate'."""
    desc_lower = description.lower()
    
    # Find the position of various terms
    terms = [' voucher', ' isolate', ' strain']
    cut_positions = [desc_lower.find(term) for term in terms if desc_lower.find(term) != -1]
    
    if cut_positions:
        cut_pos = min(cut_positions)
        shortened = description[:cut_pos].strip()
    else:
        # Try gene patterns
        gene_patterns = [' gene', ' cytochrome', ' COI', ' COX1', ' 16S', ' 18S']
        gene_positions = [desc_lower.find(pattern) for pattern in gene_patterns if desc_lower.find(pattern) != -1]
        
        if gene_positions:
            cut_pos = min(gene_positions)
            shortened = description[:cut_pos].strip()
        else:
            shortened = description.strip()
    
    return shortened

def extract_species_name(description):
    """Extract species name from description (first two words)."""
    if not description:
        return ""
    
    words = description.strip().split()
    if len(words) >= 2:
        return f"{words[0]} {words[1]}"
    elif len(words) == 1:
        return words[0]
    
    return ""

# Functions from Script 2 (BLAST TSV format)
def extract_species_from_stitle(stitle):
    """Extract species name from BLAST stitle field."""
    if not stitle:
        return ""
    
    try:
        # Split by '###' to separate accession from taxonomy
        if '###' in stitle:
            taxonomy_part = stitle.split('###')[1]
            taxa = taxonomy_part.split(';')
            
            if taxa:
                last_taxon = taxa[-1]
                species_with_taxid = last_taxon.split('_')
                if len(species_with_taxid) >= 2:
                    species_parts = []
                    for part in species_with_taxid:
                        if part.isdigit():
                            break
                        species_parts.append(part)
                    
                    if len(species_parts) >= 2:
                        return f"{species_parts[0]} {species_parts[1]}"
                    elif len(species_parts) == 1:
                        return species_parts[0]
        
        # Fallback: try to extract first two words
        words = stitle.split()
        if len(words) >= 2:
            if words[0][0].isupper() and words[1][0].islower():
                return f"{words[0]} {words[1]}"
    
    except Exception as e:
        print(f"Warning: Could not extract species from stitle '{stitle}': {e}", file=sys.stderr)
    
    return ""

def shorten_stitle(stitle):
    """Shorten the BLAST stitle field to make it more readable."""
    if not stitle:
        return ""
    
    try:
        if '###' in stitle:
            taxonomy_part = stitle.split('###')[1]
            taxa = taxonomy_part.split(';')
            
            if taxa:
                last_taxon = taxa[-1]
                species_with_taxid = last_taxon.split('_')
                if len(species_with_taxid) >= 2:
                    species_parts = []
                    for part in species_with_taxid:
                        if part.isdigit():
                            break
                        species_parts.append(part)
                    
                    if len(species_parts) >= 2:
                        return f"{species_parts[0]} {species_parts[1]}"
                    elif len(species_parts) == 1:
                        return species_parts[0]
        
        # Fallback: return first part before ###
        if '###' in stitle:
            return stitle.split('###')[0]
        
        return stitle
    
    except Exception as e:
        print(f"Warning: Could not shorten stitle '{stitle}': {e}", file=sys.stderr)
        return stitle

def detect_format(filepath):
    """
    Detect whether file is custom TSV format or standard BLAST TSV format.
    Returns 'custom' or 'blast'.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if not first_line:
                return 'blast'  # Default to blast format
            
            # Check if first line looks like a header
            if any(keyword in first_line.lower() for keyword in ['query', 'subject', 'identity', 'description']):
                return 'custom'
            
            # Check column count
            columns = first_line.split('\t')
            
            # Standard BLAST format has 12-13 columns, check for stitle format
            if len(columns) >= 12:
                # Look for BLAST stitle format in what should be the stitle column
                potential_stitle = columns[-1] if len(columns) == 13 else columns[12] if len(columns) > 12 else ""
                if '###' in potential_stitle or 'root_' in potential_stitle:
                    return 'blast'
            
            # If it has fewer columns or doesn't match BLAST format, assume custom
            return 'custom'
            
    except Exception as e:
        print(f"Warning: Could not detect format for {filepath}: {e}", file=sys.stderr)
        return 'blast'  # Default to blast format

def parse_custom_tsv(filepath):
    """Parse custom TSV format (Script 1 style)."""
    sequence_name = Path(filepath).name
    if sequence_name.endswith('.tsv.tsv'):
        sequence_name = sequence_name[:-8]
    elif sequence_name.endswith('.tsv'):
        sequence_name = sequence_name[:-4]
    
    hits_data = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Skip header line
            next(f)
            
            for i, line in enumerate(f):
                if i >= 5:
                    break
                    
                columns = line.strip().split('\t')
                if len(columns) >= 8:
                    full_description = columns[3]  # Description column
                    identity_pct = columns[7]      # Identities(%) column
                    
                    description = shorten_description(full_description)
                    species_name = extract_species_name(description)
                    family = get_family_from_ena(species_name)
                    
                    hits_data.append((description, identity_pct, family))
                    time.sleep(0.1)
                    
    except Exception as e:
        print(f"Warning: Could not parse {filepath}: {e}", file=sys.stderr)
    
    return sequence_name, hits_data

def parse_blast_tsv(filepath):
    """Parse standard BLAST TSV format (Script 2 style)."""
    sequence_name = Path(filepath).stem
    hits_data = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i >= 5:
                    break
                    
                columns = line.strip().split('\t')
                if len(columns) >= 13:
                    pident = columns[2]      # Percent identity
                    stitle = columns[12]     # Subject title
                    
                    description = shorten_stitle(stitle)
                    species_name = extract_species_from_stitle(stitle)
                    family = get_family_from_ena(species_name) if species_name else ""
                    
                    hits_data.append((description, pident, family))
                    
                    if species_name:
                        time.sleep(0.1)
                else:
                    print(f"Warning: Line {i+1} in {filepath} has only {len(columns)} columns, expected 13", file=sys.stderr)
                    
    except Exception as e:
        print(f"Warning: Could not parse {filepath}: {e}", file=sys.stderr)
    
    return sequence_name, hits_data

def find_supported_files(input_dir):
    """Find all supported file extensions."""
    extensions = ['*.tsv', '*.tsv.tsv', '*.txt', '*.out']
    all_files = []
    
    for extension in extensions:
        pattern = os.path.join(input_dir, extension)
        files = glob.glob(pattern)
        all_files.extend(files)
    
    return all_files

def main():
    if len(sys.argv) < 2:
        print("Usage: python integrated_blast_parser.py <input_directory> [output.csv]")
        print("Supports: .tsv, .tsv.tsv, .txt, .out files")
        print("Auto-detects custom TSV vs standard BLAST TSV format")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "blast_results_summary.csv"
    
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory")
        sys.exit(1)
    
    # Find all supported files
    files = find_supported_files(input_dir)
    
    if not files:
        print(f"No supported files found in {input_dir}")
        sys.exit(1)
    
    print(f"Found {len(files)} files")
    print("Auto-detecting file formats and starting family name lookups...")
    
    # Prepare CSV headers
    headers = ['sequence_name', 'detected_format']
    for i in range(1, 6):
        headers.extend([f'hit{i}_description', f'hit{i}_identity_percent', f'hit{i}_family'])
    
    # Process all files
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for filepath in sorted(files):
            # Detect format and parse accordingly
            format_type = detect_format(filepath)
            
            if format_type == 'custom':
                sequence_name, hits_data = parse_custom_tsv(filepath)
            else:
                sequence_name, hits_data = parse_blast_tsv(filepath)
            
            # Build row data
            row = [sequence_name, format_type]
            
            # Add hit data (up to 5 hits)
            for i in range(5):
                if i < len(hits_data):
                    description, identity_pct, family = hits_data[i]
                    row.extend([description, identity_pct, family])
                else:
                    row.extend(['', '', ''])
            
            writer.writerow(row)
            print(f"Processed: {sequence_name} (format: {format_type})")
            time.sleep(0.5)
    
    print(f"\nResults written to: {output_file}")
    print(f"Processed {len(files)} files")
    print(f"Cached {len(TAXONOMY_CACHE)} taxonomic lookups")

if __name__ == "__main__":
    main()
