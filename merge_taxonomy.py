#!/usr/bin/env python3
import pandas as pd
import argparse
import sys

def merge_taxonomy_data(file1_path, file2_path, output_path, encoding1=None, encoding2=None, use_identification=False):
    """
    Merge taxonomic data between File1 and File2 based on matching Process IDs.
    
    This function:
    1. Adds taxonomic data from File2 to File1 (Phylum, Class, Order, Family, Genus, Species)
    2. Also adds metadata (taxid, matched_rank, lineage, lineage_mismatch) from File2 to File1
    3. Adds metadata fields from File1 to File2 if File2 doesn't have these fields
    
    If use_identification=True, the 'Identification' column from File2 is used to populate
    the 'species' column in File1, rather than requiring separate taxonomy columns.
    
    Supports CSV, TSV, and XLSX files with auto-detection of delimiters.
    """
    
    def detect_delimiter(filepath, sample_size=8192):
        """
        Auto-detect delimiter by sampling the file content.
        Returns the most likely delimiter based on consistency across lines.
        """
        import csv
        
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            sample = f.read(sample_size)
        
        # Count occurrences of common delimiters
        delimiters = [',', '\t', ';', '|']
        lines = sample.strip().split('\n')[:10]  # Check first 10 lines
        
        if len(lines) < 2:
            return ','  # Default to comma if not enough lines
        
        best_delimiter = ','
        best_score = -1
        
        for delim in delimiters:
            counts = [line.count(delim) for line in lines]
            if counts[0] == 0:
                continue
            # Score based on consistency (all lines have same count) and count > 0
            if len(set(counts)) == 1 and counts[0] > 0:
                score = counts[0]  # Higher count = more likely correct
                if score > best_score:
                    best_score = score
                    best_delimiter = delim
        
        return best_delimiter
    
    def read_file(filepath, encoding=None):
        """
        Read a file (CSV, TSV, or XLSX) and return a DataFrame.
        Auto-detects format and delimiter.
        """
        filepath_lower = filepath.lower()
        
        # Handle Excel files
        if filepath_lower.endswith(('.xlsx', '.xls')):
            print(f"Reading as Excel file: {filepath}")
            return pd.read_excel(filepath)
        
        # Handle text files (CSV/TSV) with auto-detection
        encoding = encoding or 'utf-8'
        delimiter = detect_delimiter(filepath)
        delim_name = {',' : 'comma', '\t': 'tab', ';': 'semicolon', '|': 'pipe'}.get(delimiter, repr(delimiter))
        
        print(f"Reading {filepath}")
        print(f"  Detected delimiter: {delim_name}")
        print(f"  Encoding: {encoding}")
        
        return pd.read_csv(filepath, encoding=encoding, sep=delimiter)
    
    try:
        # Read files with auto-detection
        try:
            file1_df = read_file(file1_path, encoding1)
            print(f"  Loaded {len(file1_df)} rows from File1")
        except UnicodeDecodeError as e:
            print(f"Error reading File1: {str(e)}")
            print("Try specifying a different encoding with --encoding1")
            return []
        except Exception as e:
            print(f"Error reading File1: {str(e)}")
            return []
            
        try:
            file2_df = read_file(file2_path, encoding2)
            print(f"  Loaded {len(file2_df)} rows from File2")
        except UnicodeDecodeError as e:
            print(f"Error reading File2: {str(e)}")
            print("Try specifying a different encoding with --encoding2")
            return []
        except Exception as e:
            print(f"Error reading File2: {str(e)}")
            return []
        
        # Verify the required columns exist in File1
        required_columns_file1 = [
            'Process ID', 'phylum', 'class', 'order', 'family', 'genus', 
            'species', 'taxid', 'matched_rank', 'lineage', 'lineage_mismatch'
        ]
        missing_columns = [col for col in required_columns_file1 if col not in file1_df.columns]
        if missing_columns:
            raise ValueError(f"File1 is missing these required columns: {', '.join(missing_columns)}")
        
        # Verify the required columns exist in File2
        # Check if we need to use TAXON instead of Process ID
        if 'Process ID' not in file2_df.columns and 'TAXON' in file2_df.columns:
            print("Note: Using 'TAXON' column instead of 'Process ID' in File2")
            process_id_col = 'TAXON'
        else:
            process_id_col = 'Process ID'
        
        # Set up required columns and mapping based on mode
        if use_identification:
            # In identification mode, only require Process ID and Identification
            required_columns_file2 = [process_id_col, 'Identification']
            missing_columns = [col for col in required_columns_file2 if col not in file2_df.columns]
            if missing_columns:
                raise ValueError(f"File2 is missing these required columns: {', '.join(missing_columns)}")
            
            # Create a simple mapping: Identification -> species
            taxonomy_mapping = {'Identification': 'species'}
            print("Using 'Identification' column to populate 'species' field")
        else:
            # Default mode: require full taxonomy columns
            required_columns_file2 = [process_id_col, 'Phylum', 'Class', 'Order', 'Family', 'Genus', 'Species']
            missing_columns = [col for col in required_columns_file2 if col not in file2_df.columns]
            if missing_columns:
                raise ValueError(f"File2 is missing these required columns: {', '.join(missing_columns)}")
            
            # Create a mapping of taxonomic columns from File2 to File1
            taxonomy_mapping = {
                'Phylum': 'phylum',
                'Class': 'class',
                'Order': 'order',
                'Family': 'family',
                'Genus': 'genus',
                'Species': 'species'
            }
        
        # Create a mapping of metadata columns to update in File1
        metadata_mapping = {
            'taxid': 'taxid',
            'matched_rank': 'matched_rank',
            'lineage': 'lineage',
            'lineage_mismatch': 'lineage_mismatch'
        }
        
        # Check if File2 has the metadata columns
        metadata_in_file2 = {}
        for file1_col, file2_col in metadata_mapping.items():
            lowercase_cols = [col.lower() for col in file2_df.columns]
            if file2_col.lower() in lowercase_cols:
                # Find the actual column name with proper case
                idx = lowercase_cols.index(file2_col.lower())
                actual_col = file2_df.columns[idx]
                metadata_in_file2[file1_col] = actual_col
            elif file1_col.lower() in lowercase_cols:
                # Try the File1 column name in File2
                idx = lowercase_cols.index(file1_col.lower())
                actual_col = file2_df.columns[idx]
                metadata_in_file2[file1_col] = actual_col
        
        if metadata_in_file2:
            print(f"Found metadata columns in File2: {', '.join(metadata_in_file2.values())}")
        else:
            print("No metadata columns found in File2")
        
        # Track matched and unmatched Process IDs
        matched_ids = []
        unmatched_ids = []
        
        # Track species updates (only used in --use-identification mode)
        species_updates = []  # List of dicts: {'process_id': ..., 'old_species': ..., 'new_species': ...}
        
        # Create a copy of file1_df to modify
        result_df = file1_df.copy()
        
        # Process each row in File2 - update taxonomic data and metadata in File1
        for _, row in file2_df.iterrows():
            process_id = row[process_id_col]
            
            # Find matching rows in File1
            matching_rows = result_df['Process ID'] == process_id
            
            if matching_rows.any():
                # Update taxonomic data for matching rows
                for file2_col, file1_col in taxonomy_mapping.items():
                    new_value = row[file2_col]
                    
                    # In identification mode, only update species if values differ
                    if use_identification and file1_col == 'species':
                        # Get current species value(s) for matching rows
                        current_values = result_df.loc[matching_rows, file1_col].values
                        for idx, current_value in zip(result_df.loc[matching_rows].index, current_values):
                            # Normalise for comparison (handle NaN, whitespace)
                            current_str = str(current_value).strip() if pd.notna(current_value) else ''
                            new_str = str(new_value).strip() if pd.notna(new_value) else ''
                            
                            # Only use Identification if it contains multiple words (binomial/trinomial)
                            # Skip single-word identifications like "Arthropoda", "Insecta", etc.
                            if len(new_str.split()) < 2:
                                continue
                            
                            if current_str != new_str:
                                # Record the change
                                species_updates.append({
                                    'process_id': process_id,
                                    'old_species': current_value if pd.notna(current_value) else '',
                                    'new_species': new_value if pd.notna(new_value) else ''
                                })
                                # Apply the update
                                result_df.loc[idx, file1_col] = new_value
                    else:
                        # Default mode: always update
                        result_df.loc[matching_rows, file1_col] = new_value
                
                # Update metadata for matching rows if available in File2
                for file1_col, file2_col in metadata_in_file2.items():
                    result_df.loc[matching_rows, file1_col] = row[file2_col]
                
                matched_ids.append(process_id)
            else:
                unmatched_ids.append(process_id)
        
        # Create a modified copy of file2_df that includes metadata from file1_df
        # First, ensure all metadata columns exist in file2_df
        for metadata_col in metadata_mapping.values():
            if metadata_col not in file2_df.columns:
                file2_df[metadata_col] = None  # Initialise with None values
        
        # Process each row in File1 - update metadata in File2
        for _, row in file1_df.iterrows():
            process_id = row['Process ID']
            
            # Find matching rows in File2
            if process_id_col == 'TAXON':
                matching_rows = file2_df['TAXON'] == process_id
            else:
                matching_rows = file2_df['Process ID'] == process_id
            
            if matching_rows.any():
                # Update metadata for matching rows
                for file1_col, file2_col in metadata_mapping.items():
                    file2_df.loc[matching_rows, file2_col] = row[file1_col]
        
        # Save the merged data to the output file
        output_lower = output_path.lower()
        if output_lower.endswith(('.xlsx', '.xls')):
            result_df.to_excel(output_path, index=False)
        else:
            result_df.to_csv(output_path, index=False, encoding='utf-8')
        
        # Create a backup of the original File2 if adding metadata to it
        backup_file2 = False
        for file1_col in metadata_mapping.keys():
            if file1_col not in file2_df.columns:
                backup_file2 = True
                break
        
        if backup_file2:
            # Create a modified copy of file2_df that includes metadata from file1_df
            # First, ensure all metadata columns exist in file2_df
            for metadata_col in metadata_mapping.values():
                if metadata_col not in file2_df.columns:
                    file2_df[metadata_col] = None  # Initialize with None values
            
            # Process each row in File1 - update metadata in File2
            for _, row in file1_df.iterrows():
                process_id = row['Process ID']
                
                # Find matching rows in File2
                if process_id_col == 'TAXON':
                    matching_rows = file2_df['TAXON'] == process_id
                else:
                    matching_rows = file2_df['Process ID'] == process_id
                
                if matching_rows.any():
                    # Update metadata for matching rows
                    for file1_col, file2_col in metadata_mapping.items():
                        file2_df.loc[matching_rows, file2_col] = row[file1_col]
            
            # Save the updated File2
            if output_lower.endswith(('.xlsx', '.xls')):
                file2_output = output_path.replace('.xlsx', '_with_metadata.xlsx').replace('.xls', '_with_metadata.xls')
                file2_df.to_excel(file2_output, index=False)
            else:
                file2_output = output_path.replace('.csv', '_with_metadata.csv')
                file2_df.to_csv(file2_output, index=False, encoding='utf-8')
            print(f"Updated File2 saved to: {file2_output}")
        
        # Print log information
        print(f"Process completed successfully.")
        print(f"Updated File1 saved to: {output_path}")
        print(f"Matched {len(matched_ids)} Process IDs")
        if len(matched_ids) <= 10:
            print(f"Matched IDs: {', '.join(matched_ids)}")
        else:
            print(f"First 10 matched IDs: {', '.join(matched_ids[:10])}...")
        
        # Report species updates if in identification mode
        if use_identification and species_updates:
            print(f"\n--- Species Updates ({len(species_updates)} records) ---")
            for update in species_updates:
                print(f"  {update['process_id']}: '{update['old_species']}' -> '{update['new_species']}'")
            
            # Also save updates to a log file
            if output_lower.endswith(('.xlsx', '.xls')):
                updates_log_path = output_path.replace('.xlsx', '_species_updates.csv').replace('.xls', '_species_updates.csv')
            else:
                updates_log_path = output_path.replace('.csv', '_species_updates.csv')
            updates_df = pd.DataFrame(species_updates)
            updates_df.to_csv(updates_log_path, index=False, encoding='utf-8')
            print(f"\nSpecies updates log saved to: {updates_log_path}")
        elif use_identification:
            print("\nNo species values were updated (all Identification values matched existing species values).")
        
        return unmatched_ids
    
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Merge taxonomic data between two files (CSV, TSV, or XLSX) based on Process ID.')
    parser.add_argument('file1', help='Path to the first file (target file) - supports .csv, .tsv, .xlsx')
    parser.add_argument('file2', help='Path to the second file (source of taxonomic data) - supports .csv, .tsv, .xlsx')
    parser.add_argument('output', help='Path to save the merged output file (format determined by extension)')
    parser.add_argument('--encoding1', help='Explicitly specify encoding for file1 (default: utf-8)', default=None)
    parser.add_argument('--encoding2', help='Explicitly specify encoding for file2 (default: utf-8)', default=None)
    parser.add_argument('--use-identification', action='store_true',
                        help="Use 'Identification' column from File2 to populate 'species' in File1, "
                             "instead of requiring separate Phylum/Class/Order/Family/Genus/Species columns")
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    
    # Perform the merge
    unmatched_ids = merge_taxonomy_data(args.file1, args.file2, args.output, 
                                        args.encoding1, args.encoding2, 
                                        args.use_identification)
    
    # Report unmatched IDs
    if unmatched_ids:
        print(f"\nWARNING: {len(unmatched_ids)} Process IDs from File2 were not found in File1")
        if len(unmatched_ids) <= 10:
            print(f"Unmatched IDs: {', '.join(unmatched_ids)}")
        else:
            print(f"First 10 unmatched IDs: {', '.join(unmatched_ids[:10])}...")
    else:
        print("\nAll Process IDs from File2 were successfully matched in File1.")

if __name__ == "__main__":
    main()
