#!/usr/bin/env python3
"""
CSV Combiner - A script to combine multiple CSV files into a single file.

Features:
- Preserves header structure from first CSV file
- Aligns data from subsequent files to match the first file's column order
- Adds empty values for missing columns with appropriate warnings

Usage: 
python csv_combiner.py -i input1.csv input2.csv input3.csv -o output.csv

Arguments:
-i, --input: List of input CSV files to combine (required)
-o, --output: Output CSV file path (required)
"""

import argparse
import csv
import os
import sys

def combine_csv_files(input_files, output_file):
    if not input_files:
        print("Error: No input files provided.")
        return False
    
    # Check if all input files exist
    for file_path in input_files:
        if not os.path.exists(file_path):
            print(f"Error: Input file '{file_path}' does not exist.")
            return False
    
    try:
        first_file_header = None
        
        # Get header from first file
        with open(input_files[0], 'r', newline='') as infile:
            reader = csv.reader(infile)
            try:
                first_file_header = next(reader)
            except StopIteration:
                print(f"Error: First file '{input_files[0]}' is empty or has no header.")
                return False
        
        # Write to output file
        with open(output_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(first_file_header)
            
            # Process each input file
            for i, input_file in enumerate(input_files):
                with open(input_file, 'r', newline='') as infile:
                    reader = csv.reader(infile)
                    
                    # Get header row
                    try:
                        current_header = next(reader)
                    except StopIteration:
                        if i == 0:  # We already caught this error above
                            continue
                        print(f"Warning: File '{input_file}' is empty. Skipping.")
                        continue
                    
                    # Skip first file data since we already processed its header
                    if i == 0:
                        for row in reader:
                            writer.writerow(row)
                        continue
                    
                    # Create mapping from current file columns to first file columns
                    column_mapping = []
                    for target_col in first_file_header:
                        if target_col in current_header:
                            column_mapping.append(current_header.index(target_col))
                        else:
                            print(f"Warning: Column '{target_col}' not found in file '{input_file}'. Filling with empty values.")
                            column_mapping.append(None)
                    
                    # Check if any columns couldn't be mapped
                    if None in column_mapping:
                        print(f"Warning: Some columns in the first file were not found in '{input_file}'.")
                    
                    # Write data rows with mapped columns
                    for row in reader:
                        new_row = []
                        for col_idx in column_mapping:
                            if col_idx is not None and col_idx < len(row):
                                new_row.append(row[col_idx])
                            else:
                                new_row.append("")  # Empty value for missing columns
                        writer.writerow(new_row)
                        
        print(f"Successfully combined {len(input_files)} CSV files into '{output_file}'.")
        return True
        
    except Exception as e:
        print(f"Error: Failed to combine CSV files: {str(e)}")
        return False

def main():
    # Set up command line argument parser
    parser = argparse.ArgumentParser(description='Combine multiple CSV files into a single file.')
    parser.add_argument('-i', '--input', nargs='+', required=True, 
                        help='Input CSV files to combine')
    parser.add_argument('-o', '--output', required=True, 
                        help='Output CSV file path')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Combine CSV files
    success = combine_csv_files(args.input, args.output)
    
    # Exit with appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
