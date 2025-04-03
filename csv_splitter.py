#!/usr/bin/env python3
"""
CSV Splitter

This script splits a large CSV file into multiple smaller CSV files with a specified number of rows per file.
Each output file will contain the header row from the original CSV file followed by the specified number of data rows.
If the input CSV has fewer rows than specified for the final chunk, all remaining rows will be included in the last output file.

Usage:
    python csv_splitter.py <input_file> <rows_per_file>

Arguments:
    input_file    - Path to the input CSV file to be split
    rows_per_file - Number of rows each output file should contain (excluding header)

Example:
    python csv_splitter.py data.csv 1000

Output:
    This will create files like data_1.csv, data_2.csv, etc., each with 1000 rows (plus the header row).
"""

import csv
import os
import sys
import argparse


def split_csv(file_name, rows_per_file):
    if not os.path.isfile(file_name):
        print(f"Error: File '{file_name}' not found!")
        return 0
    
    try:
        with open(file_name, 'r', newline='') as input_csv:
            csv_reader = csv.reader(input_csv)
            header = next(csv_reader)  
            file_count = 1
            current_row = 0
            output_rows = []
            
            for row in csv_reader:
                output_rows.append(row)
                current_row += 1
                
                # Once we hit number of rows per file then write data to new csv
                if current_row == rows_per_file:
                    output_file_name = f"{os.path.splitext(file_name)[0]}_{file_count}.csv"
                    with open(output_file_name, 'w', newline='') as output_csv:
                        csv_writer = csv.writer(output_csv)
                        csv_writer.writerow(header)  
                        csv_writer.writerows(output_rows)  
                    print(f"Created {output_file_name} with {current_row} rows.")
                    
                    # Reset for next csv
                    file_count += 1
                    current_row = 0
                    output_rows = []
            
            # If there are any remaining rows then write to last csv
            if output_rows:
                output_file_name = f"{os.path.splitext(file_name)[0]}_{file_count}.csv"
                with open(output_file_name, 'w', newline='') as output_csv:
                    csv_writer = csv.writer(output_csv)
                    csv_writer.writerow(header)
                    csv_writer.writerows(output_rows)
                print(f"Created {output_file_name} with {len(output_rows)} rows (remaining rows).")
                file_count += 1
            
            return file_count - 1  # Return number of files created
            
    except Exception as e:
        print(f"Error processing CSV file: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser(
        description='Split a large CSV file into multiple smaller files with the specified number of rows.',
        formatter_class=argparse.RawTextHelpFormatter
    )
    
    parser.add_argument('input_file', help='Path to the input CSV file')
    parser.add_argument('rows_per_file', type=int, help='Number of rows per output file (excluding header)')
    
    args = parser.parse_args()
    
    if args.rows_per_file <= 0:
        print("Error: Number of rows per file must be greater than 0")
        sys.exit(1)
    
    files_created = split_csv(args.input_file, args.rows_per_file)
    
    if files_created > 0:
        print(f"Split complete. Created {files_created} CSV files.")
    else:
        print("Failed to split the CSV file.")
        sys.exit(1)


if __name__ == '__main__':
    main()
