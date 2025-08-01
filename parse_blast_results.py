#!/usr/bin/env python3
"""
Parse BLAST TSV results from multiple files into a single CSV with family names.

This script:
1. Parses TSV files from BLAST results
2. Extracts top 5 hits per sequence
3. Queries ENA taxonomy API to get family names
4. Outputs CSV with sequence name, hit descriptions, identity percentages, and families

Family Name Extraction:
- ENA's API returns taxonomic lineage as names only (no rank labels)
- Family identification uses established taxonomic nomenclature conventions:
  * Zoological families typically end in '-idae' (e.g., Canidae, Chordeumatidae)
  * Botanical families typically end in '-aceae' (e.g., Rosaceae, Asteraceae)
  * Positional inference when naming conventions don't apply

Usage: python parse_blast_results.py <input_directory> [output.csv]

Note: Requires internet connection for ENA taxonomy API calls.

BLAST TSV Format Expected:
qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle
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
    """
    Query ENA taxonomy API to get family name for a species.
    
    Note: ENA's API returns lineage as names only (no ranks), so we use
    established taxonomic nomenclature conventions to identify the family.
    
    Returns family name or empty string if not found.
    """
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
    """
    Extract family name from ENA lineage string using nomenclature conventions.
    
    ENA only provides lineage names without ranks, so we use established 
    taxonomic naming conventions:
    - Zoological families typically end in '-idae'
    - Botanical families typically end in '-aceae'  
    - Family usually appears before genus in lineage
    """
    if not lineage:
        return ""
    
    taxa = [taxon.strip() for taxon in lineage.split(';') if taxon.strip()]
    
    # Standard family name endings by nomenclature code
    family_endings = [
        'idae',    # Zoological families (e.g., Canidae, Chordeumatidae)
        'aceae',   # Botanical families (e.g., Rosaceae, Asteraceae) 
        'inae',    # Some subfamilies, but sometimes used for families
        'oidae',   # Some zoological superfamilies, but sometimes families
        'ales',    # Sometimes used for family-level groups
    ]
    
    # Method 1: Look for taxa with standard family endings
    for taxon in taxa:
        for ending in family_endings:
            if taxon.lower().endswith(ending):
                return taxon
    
    # Method 2: Positional approach - family typically comes before genus
    # In standard lineage: ...Kingdom; Phylum; Class; Order; Family; Genus;
    if len(taxa) >= 2:
        # Skip very high-level taxa that are clearly not families
        high_level_taxa = {
            'eukaryota', 'metazoa', 'chordata', 'craniata', 'vertebrata', 
            'euteleostomi', 'mammalia', 'aves', 'reptilia', 'amphibia',
            'actinopterygii', 'arthropoda', 'mollusca', 'cnidaria',  
            'plantae', 'fungi', 'bacteria', 'archaea'
        }
        
        # Look for potential family 1-3 positions before genus (last taxon)
        for i in range(2, min(5, len(taxa))):  # Check positions -2, -3, -4
            potential_family = taxa[-i]
            if potential_family.lower() not in high_level_taxa:
                return potential_family
    
    return ""

def extract_species_from_stitle(stitle):
    """
    Extract species name from BLAST stitle field.
    
    The stitle field contains taxonomic information in a special format like:
    'KU893267.1.<1.>629###root_1;Eukaryota_2759;...;Cocalus_menglaensis_1813799'
    
    Extract the species name from the end of the taxonomic path.
    """
    if not stitle:
        return ""
    
    try:
        # Split by '###' to separate accession from taxonomy
        if '###' in stitle:
            taxonomy_part = stitle.split('###')[1]
            
            # Split by ';' to get taxonomic levels
            taxa = taxonomy_part.split(';')
            
            # The last taxon should be the species (genus_species_taxid format)
            if taxa:
                last_taxon = taxa[-1]
                # Remove the taxid suffix (e.g., '_1813799')
                species_with_taxid = last_taxon.split('_')
                if len(species_with_taxid) >= 2:
                    # Reconstruct genus species, removing the numeric taxid at the end
                    # Handle cases like 'Cocalus_menglaensis_1813799' -> 'Cocalus menglaensis'
                    species_parts = []
                    for part in species_with_taxid:
                        if part.isdigit():  # Skip numeric taxid
                            break
                        species_parts.append(part)
                    
                    if len(species_parts) >= 2:
                        return f"{species_parts[0]} {species_parts[1]}"
                    elif len(species_parts) == 1:
                        return species_parts[0]
        
        # Fallback: try to extract first two words from the beginning of stitle
        words = stitle.split()
        if len(words) >= 2:
            # Check if first two words look like genus species
            if words[0][0].isupper() and words[1][0].islower():
                return f"{words[0]} {words[1]}"
    
    except Exception as e:
        print(f"Warning: Could not extract species from stitle '{stitle}': {e}", file=sys.stderr)
    
    return ""

def shorten_stitle(stitle):
    """
    Shorten the BLAST stitle field to make it more readable.
    Extract the species name from the taxonomic information.
    """
    if not stitle:
        return ""
    
    try:
        # Split by '###' to separate accession from taxonomy
        if '###' in stitle:
            taxonomy_part = stitle.split('###')[1]
            
            # Split by ';' to get taxonomic levels
            taxa = taxonomy_part.split(';')
            
            # The last taxon should be the species
            if taxa:
                last_taxon = taxa[-1]
                # Remove the taxid suffix and convert underscores to spaces
                species_with_taxid = last_taxon.split('_')
                if len(species_with_taxid) >= 2:
                    species_parts = []
                    for part in species_with_taxid:
                        if part.isdigit():  # Skip numeric taxid
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

def parse_tsv_file(filepath):
    """
    Parse a single TSV file and extract top 5 hits.
    Returns tuple: (sequence_name, hits_data)
    
    Expected BLAST TSV format:
    qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle
    """
    # Extract sequence name from filename (remove .tsv extension)
    sequence_name = Path(filepath).name
    if sequence_name.endswith('.tsv'):
        sequence_name = sequence_name[:-4]
    
    hits_data = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            # Process data lines (no header to skip)
            for i, line in enumerate(f):
                if i >= 5:  # Only take top 5 hits
                    break
                    
                columns = line.strip().split('\t')
                if len(columns) >= 13:  # Ensure we have all expected columns
                    # Extract relevant columns based on BLAST TSV format
                    pident = columns[2]      # Percent identity
                    stitle = columns[12]     # Subject title (description)
                    
                    # Shorten description for readability
                    description = shorten_stitle(stitle)
                    
                    # Extract species name for family lookup
                    species_name = extract_species_from_stitle(stitle)
                    family = get_family_from_ena(species_name) if species_name else ""
                    
                    hits_data.append((description, pident, family))
                    
                    # Small delay to be respectful to ENA API
                    if species_name:  # Only sleep if we made an API call
                        time.sleep(0.1)
                else:
                    print(f"Warning: Line {i+1} in {filepath} has only {len(columns)} columns, expected 13", file=sys.stderr)
                    
    except Exception as e:
        print(f"Warning: Could not parse {filepath}: {e}", file=sys.stderr)
        return sequence_name, []
    
    return sequence_name, hits_data

def main():
    if len(sys.argv) < 2:
        print("Usage: python parse_blast_results.py <input_directory> [output.csv]")
        sys.exit(1)
    
    input_dir = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else "blast_results_summary.csv"
    
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory")
        sys.exit(1)
    
    # Find all TSV files
    tsv_pattern = os.path.join(input_dir, "*.tsv")
    tsv_files = glob.glob(tsv_pattern)
    
    if not tsv_files:
        print(f"No TSV files found in {input_dir}")
        sys.exit(1)
    
    print(f"Found {len(tsv_files)} TSV files")
    print("Starting family name lookups via ENA taxonomy API...")
    
    # Prepare CSV headers
    headers = ['sequence_name']
    for i in range(1, 6):  # Hit 1 through Hit 5
        headers.extend([f'hit{i}_description', f'hit{i}_identity_percent', f'hit{i}_family'])
    
    # Process all files and write CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for tsv_file in sorted(tsv_files):
            sequence_name, hits_data = parse_tsv_file(tsv_file)
            
            # Build row data
            row = [sequence_name]
            
            # Add hit data (up to 5 hits)
            for i in range(5):
                if i < len(hits_data):
                    description, identity_pct, family = hits_data[i]
                    row.extend([description, identity_pct, family])
                else:
                    # Fill empty hits with blank values
                    row.extend(['', '', ''])
            
            writer.writerow(row)
            print(f"Processed: {sequence_name}")
            
            # Brief pause between files to be respectful to ENA API
            time.sleep(0.5)
    
    print(f"\nResults written to: {output_file}")
    print(f"Processed {len(tsv_files)} sequences")
    print(f"Cached {len(TAXONOMY_CACHE)} taxonomic lookups")

if __name__ == "__main__":
    main()
