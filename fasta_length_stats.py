#!/usr/bin/env python3

import argparse
import os
import statistics
from pathlib import Path

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Analyse sequence lengths in a FASTA file.')
    parser.add_argument('-i', '--input', required=True, help='Input FASTA file')
    parser.add_argument('-o', '--output', help='Output file (default: {input_basename}_length.txt)')
    return parser.parse_args()

def read_fasta(fasta_file):
    """
    Read sequences from a FASTA file.
    """
    sequences = []
    current_header = ""
    current_seq = ""
    
    with open(fasta_file, 'r') as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if line.startswith('>'):
                if current_seq and current_header:
                    sequences.append((current_header, current_seq))
                current_header = line
                current_seq = ""
            else:
                current_seq += line
                
    # Add the last sequence if it exists
    if current_seq and current_header:
        sequences.append((current_header, current_seq))
        
    return sequences

def analyse_lengths(sequences):
    """
    Analyse sequence lengths.
    """
    if not sequences:
        return {
            "min": 0, 
            "max": 0, 
            "average": 0, 
            "count": 0,
            "min_header": "",
            "max_header": ""
        }
    
    # Create list of (header, length) tuples
    header_lengths = [(header, len(seq)) for header, seq in sequences]
    
    # Find min and max entries
    min_entry = min(header_lengths, key=lambda x: x[1])
    max_entry = max(header_lengths, key=lambda x: x[1])
    
    # Extract just the lengths for average calculation
    lengths = [length for _, length in header_lengths]
    
    return {
        "min": min_entry[1],
        "max": max_entry[1],
        "average": statistics.mean(lengths),
        "count": len(sequences),
        "min_header": min_entry[0],
        "max_header": max_entry[0]
    }

def main():
    """Main function to run the script."""
    args = parse_arguments()
    
    # Get the basename of the input file
    input_path = Path(args.input)
    basename = input_path.stem
    
    # Determine output file name
    if args.output:
        output_file = args.output
    else:
        output_file = f"{basename}_length.txt"
    
    # Read sequences from the FASTA file
    try:
        sequences = read_fasta(args.input)
    except FileNotFoundError:
        print(f"Error: Input file '{args.input}' not found.")
        return
    except Exception as e:
        print(f"Error reading input file: {e}")
        return
    
    # Analyse sequence lengths
    results = analyse_lengths(sequences)
    
    # Write results to the output file
    try:
        with open(output_file, 'w') as file:
            file.write(f"Filename: {os.path.basename(args.input)}\n")
            file.write(f"Total sequences: {results['count']}\n")
            file.write(f"Minimum length: {results['min']}\n")
            file.write(f"Minimum length sequence: {results['min_header']}\n")
            file.write(f"Maximum length: {results['max']}\n")
            file.write(f"Maximum length sequence: {results['max_header']}\n")
            file.write(f"Average length: {results['average']:.2f}\n")
        
        print(f"Results written to {output_file}")
    except Exception as e:
        print(f"Error writing to output file: {e}")

if __name__ == "__main__":
    main()
