#!/usr/bin/env python3
"""
Parse BLAST results from multiple file formats into a single CSV with taxonomic lineages.
NCBI-only version for better reliability.

This script supports two BLAST result formats:
1. Custom TSV format (columns: ..., description, ..., identity_percent, ...)
2. Standard BLAST TSV format (outfmt 6 with stitle)

Features:
- Parses multiple file extensions (.tsv, .txt, .out)
- Auto-detects format based on file content
- Extracts top 5 hits per sequence
- Queries NCBI taxonomy API to get full taxonomic lineages
- Optional input CSV to match process IDs with known taxonomy
- Outputs unified CSV format

Usage: python integrated_blast_parser.py --input_dir <directory> --output_csv <output.csv> [--taxonomy_csv <taxonomy.csv>]
"""

import os
import sys
import csv
import glob
import re
import requests
import time
import json
import argparse
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode

# Global cache for taxonomic lookups to avoid repeated API calls
TAXONOMY_CACHE = {}

def get_lineage_from_ncbi(species_name):
    """Query NCBI taxonomy API to get full taxonomic lineage for a species."""
    print(f"  🔍 NCBI: Searching for '{species_name}'", file=sys.stderr)
    
    try:
        # Clean up species name
        words = species_name.strip().split()
        if len(words) >= 2:
            query_name = f"{words[0]} {words[1]}"
        else:
            query_name = species_name.strip()
        
        print(f"  🔍 NCBI: Query term = '{query_name}'", file=sys.stderr)
        
        # Step 1: Search for taxonomy ID using esearch
        search_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
        search_params = {
            'db': 'taxonomy',
            'term': query_name,
            'retmode': 'json',
            'retmax': 1
        }
        
        print(f"  🌐 NCBI: Calling {search_url}?{urlencode(search_params)}", file=sys.stderr)
        search_response = requests.get(search_url, params=search_params, timeout=10)
        
        if search_response.status_code != 200:
            print(f"  ❌ NCBI: Search failed with status {search_response.status_code}", file=sys.stderr)
            return ""
        
        search_data = search_response.json()
        print(f"  📄 NCBI: Search response = {search_data}", file=sys.stderr)
        
        if not search_data.get('esearchresult', {}).get('idlist'):
            print(f"  ❌ NCBI: No taxonomy ID found for '{query_name}'", file=sys.stderr)
            return ""
        
        tax_id = search_data['esearchresult']['idlist'][0]
        print(f"  ✅ NCBI: Found taxonomy ID = {tax_id}", file=sys.stderr)
        
        # Step 2: Get full taxonomy lineage using efetch
        fetch_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
        fetch_params = {
            'db': 'taxonomy',
            'id': tax_id,
            'retmode': 'xml'
        }
        
        print(f"  🌐 NCBI: Fetching details for tax_id {tax_id}", file=sys.stderr)
        fetch_response = requests.get(fetch_url, params=fetch_params, timeout=10)
        
        if fetch_response.status_code != 200:
            print(f"  ❌ NCBI: Fetch failed with status {fetch_response.status_code}", file=sys.stderr)
            return ""
        
        # Parse XML to extract full lineage
        try:
            root = ET.fromstring(fetch_response.text)
            
            # Build lineage from all taxonomic ranks
            lineage_parts = []
            for taxon in root.findall('.//Taxon'):
                name_elem = taxon.find('ScientificName')
                rank_elem = taxon.find('Rank')
                
                if name_elem is not None and rank_elem is not None:
                    # Skip "no rank" entries that are just organizational
                    if rank_elem.text != 'no rank':
                        lineage_parts.append(name_elem.text)
            
            lineage = '; '.join(lineage_parts) if lineage_parts else ""
            print(f"  ✅ NCBI: Extracted lineage = '{lineage}'", file=sys.stderr)
            return lineage
            
        except ET.ParseError as e:
            print(f"  ❌ NCBI: XML parsing failed: {e}", file=sys.stderr)
            
    except Exception as e:
        print(f"  ❌ NCBI: Lookup failed for '{species_name}': {e}", file=sys.stderr)
    
    return ""

def get_lineage(species_name):
    """Get taxonomic lineage using NCBI only."""
    if not species_name:
        print(f"  ⚠️  Empty species name provided", file=sys.stderr)
        return ""
        
    if species_name in TAXONOMY_CACHE:
        cached_result = TAXONOMY_CACHE.get(species_name, "")
        print(f"  💾 Cache hit for '{species_name}' = '{cached_result}'", file=sys.stderr)
        return cached_result
    
    print(f"🔬 Starting NCBI taxonomy lookup for: '{species_name}'", file=sys.stderr)
    
    lineage = get_lineage_from_ncbi(species_name)
    if lineage:
        print(f"  ✅ NCBI lookup succeeded", file=sys.stderr)
        TAXONOMY_CACHE[species_name] = lineage
        return lineage
    else:
        print(f"  ❌ NCBI lookup failed", file=sys.stderr)
        TAXONOMY_CACHE[species_name] = ""
        return ""

def load_taxonomy_csv(csv_file):
    """Load taxonomy CSV and create lookup dictionary by Process ID."""
    taxonomy_lookup = {}
    
    if not csv_file or not os.path.exists(csv_file):
        return taxonomy_lookup
    
    try:
        with open(csv_file, 'r', encoding='utf-8') as f:
            # Try to detect if there's a header
            first_line = f.readline().strip()
            f.seek(0)
            
            reader = csv.reader(f)
            
            # Skip header if it looks like one
            if 'Process ID' in first_line or 'family' in first_line:
                next(reader)
            
            for row in reader:
                if len(row) >= 10:  # Ensure we have enough columns
                    process_id = row[1].strip()  # Process ID column
                    family = row[5].strip()      # family column
                    matched_rank = row[9].strip()  # matched_rank column
                    
                    taxonomy_lookup[process_id] = {
                        'family': family,
                        'matched_rank': matched_rank
                    }
                    
    except Exception as e:
        print(f"Warning: Could not load taxonomy CSV {csv_file}: {e}", file=sys.stderr)
    
    return taxonomy_lookup

def extract_process_id_from_filename(filename):
    """Extract process ID from filename (part before first underscore)."""
    try:
        # Get just the filename without path
        basename = os.path.basename(filename)
        
        # Remove file extensions
        if basename.endswith('.tsv.tsv'):
            basename = basename[:-8]
        elif basename.endswith(('.tsv', '.txt', '.out')):
            basename = basename[:-4]
        
        # Extract part before first underscore
        if '_' in basename:
            process_id = basename.split('_')[0]
            return process_id
        else:
            return basename
            
    except Exception as e:
        print(f"Warning: Could not extract process ID from {filename}: {e}", file=sys.stderr)
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
    """Extract species name from description - fixed version with debug."""
    print(f"    🔍 extract_species_name called with: '{description}'", file=sys.stderr)
    
    if not description:
        print(f"    🚫 Empty description", file=sys.stderr)
        return ""
    
    words = description.strip().split()
    if not words:
        print(f"    🚫 No words after splitting", file=sys.stderr)
        return ""
    
    print(f"    📝 Words to process: {words}", file=sys.stderr)
    
    result = ""
    
    # Skip leading numeric words and prefixes, find genus
    for i, word in enumerate(words):
        print(f"    📍 Processing word {i}: '{word}'", file=sys.stderr)
        
        # Skip numbers/accession IDs
        if word.isdigit() or re.match(r'^\d+[\d.-_]*$', word):
            print(f"      ⏭️  Skipping numeric: '{word}'", file=sys.stderr)
            continue
        
        # Skip prefixes (remove colons for comparison)
        clean_word = word.rstrip(':').upper()
        prefixes = ['UNVERIFIED_ORG', 'UNVERIFIED', 'PREDICTED', 'PROVISIONAL', 'CF', 'AFF']
        if clean_word in prefixes:
            print(f"      ⏭️  Skipping prefix: '{word}'", file=sys.stderr)
            continue
        
        # Check if this looks like a genus
        if (len(word) > 1 and 
            word[0].isupper() and 
            not word.isupper() and  # Not all caps
            not any(c.isdigit() for c in word)):
            
            print(f"      ✅ Found genus candidate: '{word}'", file=sys.stderr)
            genus = word.rstrip(':')
            
            # Check what comes next
            if i + 1 < len(words):
                next_word = words[i + 1]
                print(f"      🔍 Next word: '{next_word}'", file=sys.stderr)
                
                # Treat sp., aff., cf. as qualifiers - just return genus
                if next_word in ['sp.', 'aff.', 'cf.']:
                    result = genus
                    print(f"      ✅ Qualifier '{next_word}' found, returning genus: '{result}'", file=sys.stderr)
                    break
                elif (len(next_word) > 1 and
                      next_word[0].islower() and
                      not next_word.isupper() and
                      next_word not in ['strain', 'isolate', 'voucher', 'mitochondrion']):
                    result = f"{genus} {next_word}"
                    print(f"      ✅ Species found, returning: '{result}'", file=sys.stderr)
                    break
                else:
                    result = genus
                    print(f"      ✅ Just genus, returning: '{result}'", file=sys.stderr)
                    break
            else:
                result = genus
                print(f"      ✅ End of words, returning: '{result}'", file=sys.stderr)
                break
    
    print(f"    🎯 Final result: '{result}'", file=sys.stderr)
    return result

# Functions from Script 2 (BLAST TSV format)
def extract_species_from_stitle(stitle):
    """Extract species name from BLAST stitle field."""
    if not stitle:
        return ""
    
    try:
        # Handle special taxonomy format with ###
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
        
        # Handle standard BLAST stitle format
        # Look for genus species pattern at the beginning
        words = stitle.split()
        if len(words) >= 1:
            # Check if first word starts with capital letter (genus)
            if words[0][0].isupper() and len(words[0]) > 1:
                
                # For "Genus sp." or "Genus sp. CODE" patterns, return just the genus
                if len(words) > 1 and words[1] == 'sp.':
                    return words[0]  # Just return genus
                
                # For proper binomial nomenclature "Genus species"
                elif (len(words) >= 2 and words[1][0].islower() and 
                      words[1] not in ['strain', 'isolate', 'voucher']):
                    return f"{words[0]} {words[1]}"
                
                # Fallback: just return genus
                else:
                    return words[0]
        
        return ""
    
    except Exception as e:
        print(f"Warning: Could not extract species from stitle '{stitle}': {e}", file=sys.stderr)
        return ""

def shorten_stitle(stitle):
    """Shorten the BLAST stitle field to make it more readable."""
    if not stitle:
        return ""
    
    try:
        # Handle special taxonomy format with ###
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
            return stitle.split('###')[0]
        
        # Handle standard BLAST stitle format
        # Remove common suffixes that make descriptions too long
        desc_lower = stitle.lower()
        terms_to_cut = [' voucher', ' isolate', ' strain', ' gene,', ' cytochrome oxidase', ', mitochondrial', ', complete cds', ', partial cds']
        
        shortened = stitle
        for term in terms_to_cut:
            if term in desc_lower:
                cut_pos = desc_lower.find(term)
                shortened = stitle[:cut_pos].strip()
                break
        
        return shortened
        
    except Exception as e:
        print(f"Warning: Could not shorten stitle '{stitle}': {e}", file=sys.stderr)
        return stitle

def detect_format(filepath):
    """
    Simple format detection: Custom has header, BLAST doesn't.
    Returns 'custom' or 'blast'.
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            if not first_line:
                return 'blast'  # Default
            
            columns = first_line.split('\t')
            
            # Try to parse as BLAST data: check if column 2 (pident) is numeric
            if len(columns) >= 3:
                try:
                    float(columns[2])  # If this is numeric, it's likely BLAST pident
                    return 'blast'
                except ValueError:
                    # If column 2 isn't numeric, it's probably a header (custom format)
                    return 'custom'
            
            # Default to blast if we can't determine
            return 'blast'
            
    except Exception as e:
        print(f"Warning: Could not detect format for {filepath}: {e}", file=sys.stderr)
        return 'blast'

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
                    print(f"📝 Hit {i+1}: Full description = '{full_description}'", file=sys.stderr)
                    print(f"📝 Hit {i+1}: Shortened description = '{description}'", file=sys.stderr)
                    
                    species_name = extract_species_name(description)
                    print(f"🧬 Hit {i+1}: Extracted species name = '{species_name}'", file=sys.stderr)
                    
                    if species_name:
                        lineage = get_lineage(species_name)
                    else:
                        print(f"⚠️  Hit {i+1}: No species name extracted, skipping taxonomy lookup", file=sys.stderr)
                        lineage = ""
                    
                    hits_data.append((description, identity_pct, lineage))
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
            # Don't skip any lines - BLAST tabular output has no header by default
            for i, line in enumerate(f):
                if i >= 5:  # Only take top 5 hits
                    break
                    
                columns = line.strip().split('\t')
                if len(columns) >= 12:  # Standard BLAST has at least 12 columns
                    pident = columns[2]      # Percent identity (3rd column)
                    
                    # Handle both 12-column and 13-column formats
                    if len(columns) >= 13:
                        stitle = columns[12]     # Subject title (13th column if present)
                    else:
                        # If no stitle column, use subject ID
                        stitle = columns[1]      # Subject sequence ID
                    
                    # Extract species info from stitle
                    if len(columns) >= 13:
                        description = shorten_stitle(stitle)
                        print(f"📝 Hit {i+1}: Full stitle = '{stitle}'", file=sys.stderr)
                        print(f"📝 Hit {i+1}: Shortened description = '{description}'", file=sys.stderr)
                        
                        species_name = extract_species_from_stitle(stitle)
                        print(f"🧬 Hit {i+1}: Extracted species name = '{species_name}'", file=sys.stderr)
                    else:
                        # Fallback for 12-column format
                        description = stitle
                        species_name = ""
                        print(f"📝 Hit {i+1}: 12-column format, no stitle available", file=sys.stderr)
                    
                    if species_name:
                        lineage = get_lineage(species_name)
                    else:
                        print(f"⚠️  Hit {i+1}: No species name extracted, skipping taxonomy lookup", file=sys.stderr)
                        lineage = ""
                    
                    hits_data.append((description, pident, lineage))
                    
                    if species_name:
                        time.sleep(0.1)
                else:
                    print(f"Warning: Line {i+1} in {filepath} has only {len(columns)} columns, expected at least 12", file=sys.stderr)
                    
    except Exception as e:
        print(f"Warning: Could not parse {filepath}: {e}", file=sys.stderr)
    
    return sequence_name, hits_data

def find_supported_files(input_dir):
    """Find all supported file extensions."""
    extensions = ['*.tsv', '*.txt', '*.out']
    all_files = []
    
    for extension in extensions:
        pattern = os.path.join(input_dir, extension)
        files = glob.glob(pattern)
        all_files.extend(files)
    
    return all_files

def main():
    parser = argparse.ArgumentParser(
        description="Parse BLAST results from multiple file formats into a single CSV with taxonomic lineages (NCBI only)."
    )
    parser.add_argument('--input_dir', '-i', required=True,
                       help='Input directory containing BLAST result files')
    parser.add_argument('--output_csv', '-o', required=True,
                       help='Output CSV file path')
    parser.add_argument('--taxonomy_csv', '-t', 
                       help='Optional taxonomy CSV file for process ID matching')
    
    args = parser.parse_args()
    
    input_dir = args.input_dir
    output_file = args.output_csv
    taxonomy_file = args.taxonomy_csv
    
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory")
        sys.exit(1)
    
    # Load taxonomy lookup if provided
    taxonomy_lookup = load_taxonomy_csv(taxonomy_file)
    if taxonomy_file:
        print(f"Loaded taxonomy data for {len(taxonomy_lookup)} process IDs")
    
    # Find all supported files
    files = find_supported_files(input_dir)
    
    if not files:
        print(f"No supported files found in {input_dir}")
        sys.exit(1)
    
    print(f"Found {len(files)} files")
    print("Auto-detecting file formats and starting NCBI taxonomic lineage lookups...")
    
    # Prepare CSV headers
    headers = ['sequence_name', 'detected_format', 'process_id', 'taxonomy_family', 'taxonomy_matched_rank']
    for i in range(1, 6):
        headers.extend([f'hit{i}_description', f'hit{i}_identity_percent', f'hit{i}_lineage'])
    
    # Process all files
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)
        
        for filepath in sorted(files):
            print(f"\n📁 Processing file: {filepath}", file=sys.stderr)
            
            # Extract process ID from filename
            process_id = extract_process_id_from_filename(filepath)
            print(f"🆔 Process ID: {process_id}", file=sys.stderr)
            
            # Look up taxonomy info
            taxonomy_info = taxonomy_lookup.get(process_id, {'family': '', 'matched_rank': ''})
            print(f"📋 Taxonomy info: {taxonomy_info}", file=sys.stderr)
            
            # Detect format and parse accordingly
            format_type = detect_format(filepath)
            print(f"🔍 Detected format: {format_type}", file=sys.stderr)
            
            if format_type == 'custom':
                sequence_name, hits_data = parse_custom_tsv(filepath)
            else:
                sequence_name, hits_data = parse_blast_tsv(filepath)
            
            # Build row data
            row = [sequence_name, format_type, process_id, 
                   taxonomy_info['family'], taxonomy_info['matched_rank']]
            
            # Add hit data (up to 5 hits)
            for i in range(5):
                if i < len(hits_data):
                    description, identity_pct, lineage = hits_data[i]
                    row.extend([description, identity_pct, lineage])
                else:
                    row.extend(['', '', ''])
            
            writer.writerow(row)
            print(f"✅ Processed: {sequence_name} (format: {format_type}, process_id: {process_id})")
            time.sleep(0.5)
    
    print(f"\nResults written to: {output_file}")
    print(f"Processed {len(files)} files")
    print(f"Cached {len(TAXONOMY_CACHE)} taxonomic lineages")
    if taxonomy_file:
        print(f"Used taxonomy data from: {taxonomy_file}")

if __name__ == "__main__":
    main()
