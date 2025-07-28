import pandas as pd
import argparse
import sys
from pathlib import Path

def load_process_ids(process_ids_input):
    """
    Load Process IDs from either a comma-separated string or a file.
    
    Args:
        process_ids_input (str): Either comma-separated Process IDs or path to file
    
    Returns:
        list: List of Process ID strings
    """
    # Check if input looks like a file path
    if ',' not in process_ids_input and Path(process_ids_input).exists():
        # Read from file
        try:
            with open(process_ids_input, 'r') as f:
                # Read all lines, strip whitespace, and filter out empty lines
                process_ids = [line.strip() for line in f.readlines() if line.strip()]
            print(f"Loaded {len(process_ids)} Process IDs from file: {process_ids_input}")
            return process_ids
        except Exception as e:
            print(f"Error reading Process IDs file: {e}")
            sys.exit(1)
    else:
        # Parse as comma-separated values
        process_ids = [pid.strip() for pid in process_ids_input.split(',') if pid.strip()]
        print(f"Using {len(process_ids)} Process IDs from command line")
        return process_ids

def analyze_taxonomic_diversity(process_ids, csv_file_path):
    """
    Analyze taxonomic diversity for specified Process IDs from a CSV file.
    
    Args:
        process_ids (list): List of Process ID strings to filter for
        csv_file_path (str): Path to the CSV file containing taxonomic data
    
    Returns:
        dict: Dictionary containing analysis results
    """
    
    # Read the CSV file
    try:
        df = pd.read_csv(csv_file_path)
        print(f"Successfully loaded CSV file with {len(df)} total records")
    except FileNotFoundError:
        print(f"Error: Could not find file '{csv_file_path}'")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    # Check if required columns exist
    required_columns = ['Process ID', 'phylum', 'class', 'order', 'family', 'genus', 'species']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        print(f"Available columns: {list(df.columns)}")
        sys.exit(1)
    
    # Filter dataframe for matching Process IDs
    filtered_df = df[df['Process ID'].isin(process_ids)]
    
    if filtered_df.empty:
        print("No matching Process IDs found in the CSV file.")
        print("Make sure your Process IDs match exactly with those in the CSV.")
        sys.exit(1)
    
    print(f"Found {len(filtered_df)} matching records")
    print()
    
    # Taxonomic levels to analyze
    taxonomic_levels = ['phylum', 'class', 'order', 'family', 'genus', 'species']
    
    results = {}
    
    # Analyze each taxonomic level
    for level in taxonomic_levels:
        # Remove NaN values and get unique values
        unique_taxa = filtered_df[level].dropna().unique()
        results[level] = {
            'count': len(unique_taxa),
            'taxa': sorted(unique_taxa.tolist())
        }
        
        print(f"{level.upper()}:")
        print(f"  Unique count: {len(unique_taxa)}")
        print(f"  Taxa: {', '.join(sorted(unique_taxa.tolist()))}")
        print()
    
    # Special analysis: families per class
    print("FAMILIES PER CLASS:")
    class_family_groups = filtered_df.groupby('class')['family'].apply(lambda x: x.dropna().unique()).to_dict()
    
    for class_name, families in sorted(class_family_groups.items()):
        if pd.notna(class_name):  # Skip NaN classes
            print(f"  {class_name}: {len(families)} families")
            print(f"    Families: {', '.join(sorted(families))}")
            print()
    
    # Special analysis: families per order
    print("FAMILIES PER ORDER:")
    order_family_groups = filtered_df.groupby('order')['family'].apply(lambda x: x.dropna().unique()).to_dict()
    
    for order, families in sorted(order_family_groups.items()):
        if pd.notna(order):  # Skip NaN orders
            print(f"  {order}: {len(families)} families")
            print(f"    Families: {', '.join(sorted(families))}")
            print()
    
    results['families_per_class'] = class_family_groups
    results['families_per_order'] = order_family_groups
    results['filtered_df'] = filtered_df  # Include for potential further analysis
    
    return results

def save_results_summary(results, process_ids, output_file):
    """Save analysis results to a text file."""
    try:
        with open(output_file, 'w') as f:
            f.write("Taxonomic Diversity Analysis Summary\n")
            f.write("=" * 40 + "\n")
            f.write(f"Process IDs analyzed ({len(process_ids)}): {', '.join(process_ids)}\n")
            f.write(f"Total matching records: {len(results['filtered_df'])}\n\n")
            
            taxonomic_levels = ['phylum', 'class', 'order', 'family', 'genus', 'species']
            
            for level in taxonomic_levels:
                f.write(f"{level.upper()}:\n")
                f.write(f"  Unique count: {results[level]['count']}\n")
                f.write(f"  Taxa: {', '.join(results[level]['taxa'])}\n\n")
            
            f.write("FAMILIES PER CLASS:\n")
            for class_name, families in sorted(results['families_per_class'].items()):
                if pd.notna(class_name):
                    f.write(f"  {class_name}: {len(families)} families\n")
                    f.write(f"    Families: {', '.join(sorted(families))}\n\n")
            
            f.write("FAMILIES PER ORDER:\n")
            for order, families in sorted(results['families_per_order'].items()):
                if pd.notna(order):
                    f.write(f"  {order}: {len(families)} families\n")
                    f.write(f"    Families: {', '.join(sorted(families))}\n\n")
        
        print(f"Summary saved to '{output_file}'")
    except Exception as e:
        print(f"Error saving summary: {e}")

def main():
    """Main function with command line argument parsing."""
    
    parser = argparse.ArgumentParser(
        description='Analyze taxonomic diversity for specified Process IDs from a CSV file.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using comma-separated Process IDs
  python script.py -c data.csv -p "POLNB003-13,POLNB007-13,POLNB008-13"
  
  # Using Process IDs from a file
  python script.py -c data.csv -p process_ids.txt
  
  # Save output to file
  python script.py -c data.csv -p "POLNB003-13,POLNB007-13" -o results.txt
        """
    )
    
    parser.add_argument(
        '-c', '--csv', 
        required=True,
        help='Path to the CSV file containing taxonomic data'
    )
    
    parser.add_argument(
        '-p', '--process-ids',
        required=True,
        help='Process IDs to analyze. Either comma-separated values or path to a text file with one Process ID per line'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Optional: Path to save analysis summary as a text file'
    )
    
    args = parser.parse_args()
    
    print("Taxonomic Diversity Analysis")
    print("=" * 40)
    
    # Load Process IDs
    process_ids = load_process_ids(args.process_ids)
    
    # Run analysis
    results = analyze_taxonomic_diversity(process_ids, args.csv)
    
    # Save results if output file specified
    if args.output:
        save_results_summary(results, process_ids, args.output)
    
    print("\nAnalysis completed successfully!")

if __name__ == "__main__":
    main()
