#!/bin/bash
#SBATCH --job-name=fastp
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --partition=himem
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

# Usage: ./fastp_run.sh <input> <output_directory> [threads]
# 
# <input> can be either:
#   - A text file listing directories (one per line)
#   - A directory containing FASTQ files
#
# Example 1: ./fastp_universal_processor.sh dir_list.txt /path/to/output 8
# Example 2: ./fastp_universal_processor.sh /path/to/fastq_files /path/to/output 8

# Load conda env
source /mnt/apps/users/dparsons/conda/etc/profile.d/conda.sh
conda activate bgee_env

set -euo pipefail

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <input> <output_directory> [threads]"
    echo ""
    echo "<input> can be either:"
    echo "  - A text file listing directories (one per line)"
    echo "  - A directory containing FASTQ files"
    echo ""
    echo "Example 1: $0 dir_list.txt /path/to/output 8"
    echo "Example 2: $0 /path/to/fastq_files /path/to/output 8"
    exit 1
fi

INPUT="$1"
OUTPUT_DIR="$2"
THREADS="${3:-8}"

# Check if input exists
if [ ! -e "$INPUT" ]; then
    echo "Error: Input '$INPUT' not found!"
    exit 1
fi

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "Starting fastp processing..."
echo "Input: $INPUT"
echo "Output directory: $OUTPUT_DIR"
echo "Threads: $THREADS"

# Function to run fastp on a pair of files
run_fastp() {
    local ID="$1"
    local R1_FILE="$2"
    local R2_FILE="$3"
    
    echo "  Running fastp for sample $ID..."
    fastp -i "$R1_FILE" -I "$R2_FILE" \
        -o "$OUTPUT_DIR/${ID}.R1_trimmed.fastq.gz" \
        -O "$OUTPUT_DIR/${ID}.R2_trimmed.fastq.gz" \
        --adapter_sequence=AGATCGGAAGAGCACACGTCTGAACTCCAGTCA \
        --adapter_sequence_r2=AGATCGGAAGAGCGTCGTGTAGGGAAAGAGTGT \
        --dedup \
        --trim_poly_g \
        --thread "$THREADS" \
        --reads_to_process 0 \
        -h "$OUTPUT_DIR/${ID}.report.html" \
        -j "$OUTPUT_DIR/${ID}.json"
    
    if [ $? -eq 0 ]; then
        echo "  ✓ Successfully processed $ID"
    else
        echo "  ✗ Error processing $ID"
    fi
    
    echo "----------------------------------------"
}

# Detect input type and process accordingly
if [ -f "$INPUT" ]; then
    # Input is a file - treat as directory list
    echo "Detected: Directory list file"
    echo "----------------------------------------"
    
    while IFS= read -r sample_dir; do
        # Skip empty lines
        [ -z "$sample_dir" ] && continue
        
        # Check if directory exists
        if [ ! -d "$sample_dir" ]; then
            echo "Warning: Directory '$sample_dir' not found, skipping..."
            continue
        fi
        
        echo "Processing directory: $sample_dir"
        
        # Extract ID from directory name
        dir_basename=$(basename "$sample_dir")
        if [[ "$dir_basename" =~ ^Sample_YB-4226-(.+)$ ]]; then
            ID="${BASH_REMATCH[1]}"
            echo "  Extracted ID: $ID"
        else
            echo "  Warning: Could not extract ID from directory name '$dir_basename', skipping..."
            continue
        fi
        
        # Find R1 and R2 files (support both .fq.gz and .fastq.gz)
        R1_FILE=$(find "$sample_dir" -name "*R1*.fastq.gz" -o -name "*R1*.fq.gz" -type f | head -1)
        R2_FILE=$(find "$sample_dir" -name "*R2*.fastq.gz" -o -name "*R2*.fq.gz" -type f | head -1)
        
        # Check if both files were found
        if [ -z "$R1_FILE" ]; then
            echo "  Warning: No R1 file found in $sample_dir, skipping..."
            continue
        fi
        
        if [ -z "$R2_FILE" ]; then
            echo "  Warning: No R2 file found in $sample_dir, skipping..."
            continue
        fi
        
        echo "  R1 file: $R1_FILE"
        echo "  R2 file: $R2_FILE"
        
        run_fastp "$ID" "$R1_FILE" "$R2_FILE"
        
    done < "$INPUT"

elif [ -d "$INPUT" ]; then
    # Input is a directory - process flat structure
    echo "Detected: Flat directory structure"
    echo "----------------------------------------"
    
    # Track if we found any files
    FOUND_FILES=0
    
    # Try .fq.gz extension first
    for R1_FILE in "$INPUT"/*_R1.fq.gz; do
        [ -e "$R1_FILE" ] || break
        FOUND_FILES=1
        
        # Construct R2 filename
        R2_FILE="${R1_FILE/_R1.fq.gz/_R2.fq.gz}"
        
        # Check if R2 exists
        if [ ! -f "$R2_FILE" ]; then
            echo "Warning: R2 file not found for $R1_FILE, skipping..."
            continue
        fi
        
        # Extract sample ID
        basename_r1=$(basename "$R1_FILE")
        ID="${basename_r1/_R1.fq.gz/}"
        
        echo "Processing sample: $ID"
        echo "  R1 file: $R1_FILE"
        echo "  R2 file: $R2_FILE"
        
        run_fastp "$ID" "$R1_FILE" "$R2_FILE"
    done
    
    # Try .fastq.gz extension if no .fq.gz files found
    if [ $FOUND_FILES -eq 0 ]; then
        for R1_FILE in "$INPUT"/*_R1.fastq.gz; do
            [ -e "$R1_FILE" ] || break
            FOUND_FILES=1
            
            # Construct R2 filename
            R2_FILE="${R1_FILE/_R1.fastq.gz/_R2.fastq.gz}"
            
            # Check if R2 exists
            if [ ! -f "$R2_FILE" ]; then
                echo "Warning: R2 file not found for $R1_FILE, skipping..."
                continue
            fi
            
            # Extract sample ID
            basename_r1=$(basename "$R1_FILE")
            ID="${basename_r1/_R1.fastq.gz/}"
            
            echo "Processing sample: $ID"
            echo "  R1 file: $R1_FILE"
            echo "  R2 file: $R2_FILE"
            
            run_fastp "$ID" "$R1_FILE" "$R2_FILE"
        done
    fi
    
    if [ $FOUND_FILES -eq 0 ]; then
        echo "Error: No R1 FASTQ files (*_R1.fq.gz or *_R1.fastq.gz) found in $INPUT"
        exit 1
    fi

else
    echo "Error: Input must be either a file or a directory"
    exit 1
fi

echo "Batch processing complete!"
echo "Output files are in: $OUTPUT_DIR"
