#!/usr/bin/env python3
"""
Taxonomy ID Fetcher

This script processes a CSV file containing taxonomic information to fetch and append
taxonomic IDs (taxids) using the NCBI taxonomic database.

Usage:
    python taxid_fetcher.py <input_csv> <rankedlineage_path> <output_csv>

Arguments:
    input_csv: Path to the input CSV file containing taxonomy information
    rankedlineage_path: Path to the rankedlineage.dmp file for taxonomic resolution
    output_csv: Path where the output CSV with appended taxids will be saved

Dependencies:
    - pandas
    - concurrent.futures (for parallel processing)
"""

import sys
import os
import pandas as pd
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time


def load_rankedlineage(rankedlineage_path):
    """
    Load taxonomic data with comprehensive indexing at all taxonomic ranks
    
    Parameters:
    rankedlineage_path (str): Path to the rankedlineage.dmp file
    
    Returns:
    dict: Dictionary containing indexed taxonomic data at all ranks
    """
    print(f"Loading rankedlineage file from {rankedlineage_path}...")
    
    tax_data = {
        'by_scientific_name': {},
        'by_species': {},
        'by_genus': {},
        'by_family': {},
        'by_order': {},
        'by_class': {},
        'by_phylum': {}
    }
    
    line_count = 0
    species_indexed = 0
    
    with open(rankedlineage_path, 'r', encoding='utf-8', errors='ignore') as file:
        for line in file:
            parts = [p.strip() for p in line.strip().split('|')]
            if len(parts) < 10:
                continue
                
            taxid = parts[0]
            scientific_name = parts[1]
            species_field = parts[2]
            genus = parts[3]
            family = parts[4]
            order = parts[5]
            class_name = parts[6]
            phylum = parts[7]
            kingdom = parts[8]
            superkingdom = parts[9]
            
            # Extract species without duplicating genus
            species = None
            if species_field:
                # Remove genus if it's at the start of species field
                if genus and species_field.lower().startswith(genus.lower()):
                    species = species_field[len(genus):].strip()
                else:
                    species = species_field
            elif genus and scientific_name.startswith(genus):
                species = scientific_name[len(genus):].strip()
                
            # Validate species name
            if species and any(x in species.lower() for x in ['sp.', 'cf.', 'aff.', 'x ', 'subsp.', 'var.']):
                species = None
            
            lineage = {
                'taxid': taxid,
                'scientific_name': scientific_name,
                'species': species,
                'genus': genus,
                'family': family,
                'order': order,
                'class': class_name,
                'phylum': phylum,
                'kingdom': kingdom,
                'superkingdom': superkingdom
            }
            
            # Index by scientific name
            tax_data['by_scientific_name'][scientific_name.lower()] = lineage
            
            # Index by species if we have it - store without duplicating genus
            if species and genus:
                species_key = f"{genus.lower()} {species.lower()}"
                tax_data['by_species'][species_key] = lineage
                species_indexed += 1
            
            # Index by genus
            if genus:
                if genus.lower() not in tax_data['by_genus']:
                    tax_data['by_genus'][genus.lower()] = []
                tax_data['by_genus'][genus.lower()].append(lineage)
            
            # Index by family
            if family:
                if family.lower() not in tax_data['by_family']:
                    tax_data['by_family'][family.lower()] = []
                tax_data['by_family'][family.lower()].append(lineage)
            
            # Index by order
            if order:
                if order.lower() not in tax_data['by_order']:
                    tax_data['by_order'][order.lower()] = []
                tax_data['by_order'][order.lower()].append(lineage)
            
            # Index by class
            if class_name:
                if class_name.lower() not in tax_data['by_class']:
                    tax_data['by_class'][class_name.lower()] = []
                tax_data['by_class'][class_name.lower()].append(lineage)
            
            # Index by phylum
            if phylum:
                if phylum.lower() not in tax_data['by_phylum']:
                    tax_data['by_phylum'][phylum.lower()] = []
                tax_data['by_phylum'][phylum.lower()].append(lineage)
            
            line_count += 1
            if line_count % 500000 == 0:
                print(f"Processed {line_count} lines... ({species_indexed} species indexed)")
                print(f"Current index sizes:")
                print(f"  Scientific names: {len(tax_data['by_scientific_name'])}")
                print(f"  Species: {len(tax_data['by_species'])}")
                print(f"  Genera: {len(tax_data['by_genus'])}")
                print(f"  Families: {len(tax_data['by_family'])}")
                print(f"  Orders: {len(tax_data['by_order'])}")
                print(f"  Classes: {len(tax_data['by_class'])}")
                print(f"  Phyla: {len(tax_data['by_phylum'])}")
    
    print(f"\nFinished loading {line_count} taxonomic records.")
    print(f"Final index sizes:")
    print(f"  Scientific names: {len(tax_data['by_scientific_name'])}")
    print(f"  Species: {len(tax_data['by_species'])}")
    print(f"  Genera: {len(tax_data['by_genus'])}")
    print(f"  Families: {len(tax_data['by_family'])}")
    print(f"  Orders: {len(tax_data['by_order'])}")
    print(f"  Classes: {len(tax_data['by_class'])}")
    print(f"  Phyla: {len(tax_data['by_phylum'])}")
    
    return tax_data


def validate_against_higher_ranks(lineage, target_ranks, validation_level='family'):
    """
    Validate taxonomy with hierarchical fallback at different taxonomic levels.
    Returns true if a match is found at the specified level, without checking lower ranks.
    
    Parameters:
    lineage (dict): The lineage to validate
    target_ranks (dict): The target taxonomy to validate against
    validation_level (str): The taxonomic level to validate at ('family', 'order', 'class', or 'phylum')
    
    Returns:
    bool: True if validation passes at the specified level
    """
    # Define validation hierarchy (from lowest to highest rank)
    validation_ranks = {
        'family': ['family'],
        'order': ['order'],
        'class': ['class'],
        'phylum': ['phylum']
    }
    
    ranks_to_check = validation_ranks.get(validation_level, [])
    
    # Only check the specified rank, not lower ranks
    for rank in ranks_to_check:
        target_value = target_ranks.get(rank, '')
        lineage_value = lineage.get(rank, '')
        
        # Skip comparison if either value is empty
        if not target_value or not lineage_value:
            continue
            
        # Convert to strings and lowercase for comparison
        target_value = str(target_value).lower()
        lineage_value = str(lineage_value).lower()
        
        if lineage_value == target_value:
            return True
        
        return False
            
    return False


def resolve_taxid(phylum, class_name, order, family, genus, species, tax_data):
    """
    Resolve taxonomic ID with hierarchical fallback and validation.
    Tries to match at each rank level and validates against higher ranks in order.
    
    Parameters:
    phylum (str): Phylum name
    class_name (str): Class name
    order (str): Order name
    family (str): Family name
    genus (str): Genus name
    species (str): Species name
    tax_data (dict): The taxonomic data dictionary from load_rankedlineage
    
    Returns:
    tuple: (taxid, matched_rank, lineage_string, is_mismatch)
    """
    target_ranks = {
        'phylum': phylum,
        'class': class_name,
        'order': order,
        'family': family,
        'genus': genus,
        'species': species
    }
    
    validation_levels = ['family', 'order', 'class', 'phylum']
    
    # Try species-level match first
    if species and genus and isinstance(species, str) and isinstance(genus, str):
        try:
            # Remove genus from species if it's duplicated at the start
            species_name = species
            if species.lower().startswith(genus.lower()):
                species_name = species[len(genus):].strip()
                
            species_key = f"{genus.lower()} {species_name.lower()}"
            if species_key in tax_data['by_species']:
                lineage = tax_data['by_species'][species_key]
                for level in validation_levels:
                    if validate_against_higher_ranks(lineage, target_ranks, level):
                        return create_return_tuple(lineage, 'species', False)
        except (AttributeError, TypeError):
            # Handle potential errors with string operations
            pass
    
    # Try genus-level match
    if genus and isinstance(genus, str):
        try:
            if genus.lower() in tax_data['by_genus']:
                matches = tax_data['by_genus'][genus.lower()]
                for lineage in matches:
                    # For genus match, try validating at each level in order
                    for level in validation_levels:
                        if validate_against_higher_ranks(lineage, target_ranks, level):
                            return create_return_tuple(lineage, 'genus', False)
        except (AttributeError, TypeError):
            # Handle potential errors with string operations
            pass
    
    # Try matches at each higher rank
    for rank in validation_levels:
        rank_value = target_ranks.get(rank, '')
        if rank_value and isinstance(rank_value, str):
            try:
                if rank_value.lower() in tax_data[f'by_{rank}']:
                    matches = tax_data[f'by_{rank}'][rank_value.lower()]
                    # For each rank, only validate at that specific rank
                    for lineage in matches:
                        if validate_against_higher_ranks(lineage, target_ranks, rank):
                            return create_return_tuple(lineage, rank, False)
            except (AttributeError, TypeError):
                # Handle potential errors with string operations
                continue
    
    return None, 'unmatched', None, False


def create_return_tuple(lineage, matched_rank, is_mismatch):
    """Helper function to create consistent return values"""
    lineage_string = ";".join([
        f"{rank}:{lineage.get(rank, '')}" 
        for rank in ['superkingdom', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    ])
    return lineage['taxid'], matched_rank, lineage_string, is_mismatch


def process_row(row, tax_data):
    """
    Process a single row of taxonomy data to resolve taxid
    
    Parameters:
    row (dict): Row from the input CSV containing taxonomy information
    tax_data (dict): Taxonomic data for resolution
    
    Returns:
    dict: Original row with taxid information added
    """
    result = row.copy()
    
    # Extract taxonomy data from the row and handle NaN values
    phylum = str(row.get('phylum', '')) if pd.notna(row.get('phylum', '')) else ''
    class_name = str(row.get('class', '')) if pd.notna(row.get('class', '')) else ''
    order = str(row.get('order', '')) if pd.notna(row.get('order', '')) else ''
    family = str(row.get('family', '')) if pd.notna(row.get('family', '')) else ''
    genus = str(row.get('genus', '')) if pd.notna(row.get('genus', '')) else ''
    species = str(row.get('species', '')) if pd.notna(row.get('species', '')) else ''
    
    # Resolve taxonomic ID
    taxid, matched_rank, lineage, is_mismatch = resolve_taxid(
        phylum, class_name, order, family, genus, species, tax_data
    )
    
    # Add taxid information to the result
    result['taxid'] = taxid if taxid else 'not found'
    result['matched_rank'] = matched_rank
    result['lineage'] = lineage if lineage else ''
    result['lineage_mismatch'] = "Yes" if is_mismatch else "No"
    
    return result


def process_csv(input_csv, rankedlineage_path, output_csv):
    """
    Process the input CSV file to fetch taxids and generate the output CSV
    
    Parameters:
    input_csv (str): Path to the input CSV file
    rankedlineage_path (str): Path to the rankedlineage.dmp file
    output_csv (str): Path to save the output CSV file
    """
    print(f"Starting processing of {input_csv}")
    
    # Load the taxonomic data
    tax_data = load_rankedlineage(rankedlineage_path)
    
    # Read the input CSV file with proper handling of missing values
    df = pd.read_csv(input_csv, na_values=['', 'NA', 'N/A', 'nan', 'NaN'])
    
    # Print column names to help with debugging
    print(f"CSV columns found: {list(df.columns)}")
    
    # Check if expected taxonomy columns exist
    required_columns = ['phylum', 'class', 'order', 'family', 'genus', 'species']
    found_columns = [col.lower() for col in df.columns]
    
    missing_columns = [col for col in required_columns if col not in found_columns]
    if missing_columns:
        print(f"Warning: The following columns are missing or named differently: {missing_columns}")
        print("Will attempt to process with available columns")
    
    # Ensure lowercase column names for consistency
    df.columns = [col.lower() for col in df.columns]
    
    # Process each row to fetch taxids
    results = []
    total_rows = len(df)
    processed_rows = 0
    successful_rows = 0
    start_time = time.time()
    
    # Convert DataFrame to list of dictionaries for processing
    rows = df.to_dict('records')
    
    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_row = {executor.submit(process_row, row, tax_data): row for row in rows}
        
        for future in as_completed(future_to_row):
            try:
                result = future.result()
                results.append(result)
                processed_rows += 1
                successful_rows += 1
                
                # Update progress
                progress = (processed_rows / total_rows) * 100
                elapsed_time = time.time() - start_time
                remaining_time = (elapsed_time / processed_rows) * (total_rows - processed_rows) if processed_rows > 0 else 0
                
                print(f"\rProgress: {progress:.2f}% - Processed {processed_rows}/{total_rows} rows. "
                      f"Est. time remaining: {remaining_time:.2f} seconds", end="")
                
            except Exception as e:
                print(f"\nError processing row: {e}")
                # Still add the original row to results to maintain row count
                row_copy = rows[processed_rows].copy()
                row_copy['taxid'] = 'error'
                row_copy['matched_rank'] = 'error'
                row_copy['lineage'] = ''
                row_copy['lineage_mismatch'] = ''
                results.append(row_copy)
                processed_rows += 1
    
    # Convert results back to DataFrame and save to output CSV
    result_df = pd.DataFrame(results)
    result_df.to_csv(output_csv, index=False)
    
    print(f"\nOutput saved to {output_csv}")
    print(f"Total processing time: {time.time() - start_time:.2f} seconds")
    print(f"Successfully processed {successful_rows} out of {total_rows} rows")


if __name__ == "__main__":
    if len(sys.argv) == 4:
        input_csv = sys.argv[1]
        rankedlineage_path = sys.argv[2]
        output_csv = sys.argv[3]
        
        if not os.path.isfile(input_csv):
            print(f"Error: Input CSV file '{input_csv}' not found.")
            sys.exit(1)
            
        if not os.path.isfile(rankedlineage_path):
            print(f"Error: Rankedlineage file '{rankedlineage_path}' not found.")
            sys.exit(1)
            
        process_csv(input_csv, rankedlineage_path, output_csv)
    else:
        print("Usage: python taxonomy_id_fetcher.py <input_csv> <rankedlineage_path> <output_csv>")
        sys.exit(1)
