import json
import os
import csv
import glob
import argparse

def extract_read_counts(directory_path='.', output_csv='read_counts.csv'):
    """
    Extract total_reads from JSON files and save to CSV.
    """
    
    # Find all JSON files in the directory and subdirectories recursively
    json_files = glob.glob(os.path.join(directory_path, '**', '*.json'), recursive=True)
    
    if not json_files:
        print(f"No JSON files found in {directory_path}")
        return
    
    results = []
    
    for json_file in json_files:
        try:
            # Load JSON data
            with open(json_file, 'r') as f:
                data = json.load(f)
            
            # Extract sample name from filename
            filename = os.path.basename(json_file)
            sample_name = filename.split('_')[0]  # Get first part before underscore
            
            # Extract read counts
            read1_total = data.get('read1_before_filtering', {}).get('total_reads', 0)
            read2_total = data.get('read2_before_filtering', {}).get('total_reads', 0)
            
            results.append([sample_name, read1_total, read2_total])
            print(f"Processed: {filename} -> {sample_name}, R1: {read1_total}, R2: {read2_total}")
            
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error processing {json_file}: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error with {json_file}: {e}")
            continue
    
    # Write results to CSV
    if results:
        with open(output_csv, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['sample', 'R1', 'R2'])  # Header
            writer.writerows(results)
        
        print(f"\nResults saved to {output_csv}")
        print(f"Processed {len(results)} files successfully")
    else:
        print("No valid data extracted from JSON files")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract read counts from JSON files and save to CSV')
    parser.add_argument('--input', '-i', 
                       default='.', 
                       help='Input directory containing JSON files (default: current directory)')
    parser.add_argument('--output', '-o', 
                       default='read_counts.csv', 
                       help='Output CSV filename (default: read_counts.csv)')
    
    args = parser.parse_args()
    
    # Run with command line arguments
    extract_read_counts(args.input, args.output)
