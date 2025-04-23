#!/usr/bin/env python3
"""
Taxonomy Resolver Script

This script processes taxonomic data from a CSV file and determines the taxid, matched_rank,
lineage, and lineage_mismatch using a taxonomy reference file (rankedlineage.dmp).

Usage:
    python taxonomy_resolver.py <input_csv_file> <rankedlineage_path> <output_csv_file>

Arguments:
    input_csv_file: CSV file containing taxonomic data
    rankedlineage_path: Path to the rankedlineage.dmp file
    output_csv_file: Path where the output CSV file will be saved

Dependencies:
    - pandas
    - time
"""

import sys
import os
import pandas as pd
import time
import csv


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
    Validate taxonomy against higher ranks.
    
    Parameters:
    lineage (dict): The lineage to validate
    target_ranks (dict): The target taxonomy to validate against
    validation_level (str): The taxonomic level to validate at
    
    Returns:
    bool: True if validation passes at the specified level
    """
    # Define validation hierarchy
    validation_ranks = {
        'family': ['family'],
        'order': ['order'],
        'class': ['class'],
        'phylum': ['phylum']
    }
    
    ranks_to_check = validation_ranks.get(validation_level, [])
    
    # Only check the specified rank
    for rank in ranks_to_check:
        if target_ranks.get(rank) and lineage.get(rank):
            target_value = target_ranks[rank].lower()
            lineage_value = lineage[rank].lower()
            
            if lineage_value == target_value:
                return True
            
            return False
            
    return False


def resolve_taxid(phylum, class_name, order, family, genus, species, tax_data):
    """
    Resolve taxonomic ID with hierarchical fallback and validation.
    Tries to match at each rank level and validates against higher ranks.
    
    Parameters:
    phylum (str): Phylum name
    class_name (str): Class name
    order (str): Order name
    family (str): Family name
    genus (str): Genus name
    species (str): Species name
    tax_data (dict): The taxonomic data dictionary
    
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
    if species and genus:
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
    
    # Try genus-level match
    if genus:
        if genus.lower() in tax_data['by_genus']:
            matches = tax_data['by_genus'][genus.lower()]
            for lineage in matches:
                # For genus match, try validating at each level in order
                for level in validation_levels:
                    if validate_against_higher_ranks(lineage, target_ranks, level):
                        return create_return_tuple(lineage, 'genus', False)
    
    # Try matches at each higher rank
    for rank in validation_levels:
        rank_value = target_ranks.get(rank)
        if rank_value:
            if rank_value.lower() in tax_data[f'by_{rank}']:
                matches = tax_data[f'by_{rank}'][rank_value.lower()]
                # For each rank, only validate at that specific rank
                for lineage in matches:
                    if validate_against_higher_ranks(lineage, target_ranks, rank):
                        return create_return_tuple(lineage, rank, False)
    
    return None, 'unmatched', None, False


def create_return_tuple(lineage, matched_rank, is_mismatch):
    """Helper function to create consistent return values"""
    lineage_string = ";".join([
        f"{rank}:{lineage.get(rank, '')}" 
        for rank in ['superkingdom', 'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    ])
    return lineage['taxid'], matched_rank, lineage_string, is_mismatch


def process_csv(input_file, rankedlineage_path, output_file):
    """
    Process the input CSV file and generate the output with taxid, matched_rank, lineage, and lineage_mismatch
    
    Parameters:
    input_file (str): Path to the input CSV file
    rankedlineage_path (str): Path to the rankedlineage.dmp file
    output_file (str): Path where the output CSV file will be saved
    """
    print(f"Processing CSV file: {input_file}")
    
    # Load taxonomic data
    tax_data = load_rankedlineage(rankedlineage_path)
    
    # Read input CSV
    df = pd.read_csv(input_file)
    
    # Create output dataframe with same columns plus new ones
    output_df = df.copy()
    output_df['taxid'] = None
    output_df['matched_rank'] = None
    output_df['lineage'] = None
    output_df['lineage_mismatch'] = None
    
    # Process each row
    total_rows = len(df)
    start_time = time.time()
    
    for index, row in df.iterrows():
        phylum = row['Phylum'] if 'Phylum' in row else None
        class_name = row['Class'] if 'Class' in row else None
        order = row['Order'] if 'Order' in row else None
        family = row['Family'] if 'Family' in row else None
        genus = row['Genus'] if 'Genus' in row else None
        species = row['Species'] if 'Species' in row else None
        
        # Fill missing values with 'not collected'
        phylum = 'not collected' if pd.isna(phylum) else phylum
        class_name = 'not collected' if pd.isna(class_name) else class_name
        order = 'not collected' if pd.isna(order) else order
        family = 'not collected' if pd.isna(family) else family
        genus = 'not collected' if pd.isna(genus) else genus
        species = 'not collected' if pd.isna(species) else species
        
        # Resolve taxonomic ID
        taxid, matched_rank, lineage, is_mismatch = resolve_taxid(
            phylum, class_name, order, family, genus, species, tax_data
        )
        
        # Update output dataframe
        output_df.at[index, 'taxid'] = 'not collected' if taxid is None else taxid
        output_df.at[index, 'matched_rank'] = matched_rank
        output_df.at[index, 'lineage'] = 'not collected' if lineage is None else lineage
        output_df.at[index, 'lineage_mismatch'] = "Yes" if is_mismatch else "No"
        
        # Print progress
        if (index + 1) % 10 == 0 or index == total_rows - 1:
            progress = ((index + 1) / total_rows) * 100
            elapsed_time = time.time() - start_time
            est_total_time = elapsed_time / ((index + 1) / total_rows)
            remaining_time = est_total_time - elapsed_time
            
            print(f"\rProgress: {progress:.2f}% - Processed {index + 1}/{total_rows} rows. " +
                  f"Est. time remaining: {remaining_time:.2f} seconds", end="")
    
    # Save to output file
    print(f"\nSaving results to {output_file}")
    output_df.to_csv(output_file, index=False)
    print(f"Processing completed in {time.time() - start_time:.2f} seconds")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python taxonomy_resolver.py <input_csv_file> <rankedlineage_path> <output_csv_file>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    rankedlineage_path = sys.argv[2]
    output_file = sys.argv[3]
    
    if not os.path.isfile(input_file):
        print(f"Error: Input file '{input_file}' does not exist.")
        sys.exit(1)
    
    if not os.path.isfile(rankedlineage_path):
        print(f"Error: Rankedlineage file '{rankedlineage_path}' does not exist.")
        sys.exit(1)
    
    process_csv(input_file, rankedlineage_path, output_file)
