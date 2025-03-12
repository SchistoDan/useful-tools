#!/bin/bash
#SBATCH --job-name=BLAST
#SBATCH --partition=medium
#SBATCH --output=%x.out
#SBATCH --error=%x.err
#SBATCH --mem=100G
#SBATCH --cpus-per-task=36

# Set path explicitly
export PATH="/mnt/apps/users/dparsons/conda/bin:$PATH"

# Source conda.sh
source /mnt/apps/users/dparsons/conda/etc/profile.d/conda.sh

# Activate env
conda activate CONDA_ENV



#!/bin/bash

# Script to run BLASTn on a directory of FASTA files or a single multi-FASTA file
# Output files are named according to sequence headers
# Added functionality: Skip processing if output file already exists

# ======= CONFIGURATION - MODIFY THESE VALUES =======
# Input: can be a directory containing FASTA files or a single multi-FASTA file
INPUT_PATH="path/to/fasta"

# Output directory where BLAST results will be saved
OUTPUT_DIR="path/to/output_dir"

# Path to the NCBI database directory
DB_DIR="path/to/ncbi_db"

# Name of the BLAST database to use (without file extensions), e.g. nt, nr
DB_NAME="db_prefix"

# Additional BLAST options
BLAST_OPTIONS="-evalue 5e-2 -max_target_seqs 50 -num_threads 36"
# ===================================================

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Check if blastn is available
if ! command -v blastn &> /dev/null; then
    echo "Error: blastn command not found. Please make sure BLAST+ is installed and in your PATH"
    exit 1
fi

# Construct full database path
DB_PATH="${DB_DIR}/${DB_NAME}"

# Check if database exists by looking for common database file extensions
if [ ! -e "${DB_PATH}.nhr" ] && [ ! -e "${DB_PATH}.00.nhr" ] && [ ! -e "${DB_PATH}.nal" ]; then
    echo "Error: BLAST database '${DB_NAME}' not found at $DB_DIR"
    echo "Available databases in $DB_DIR:"
    # List potential databases by looking for common BLAST database file extensions
    find "$DB_DIR" -name "*.nhr" -o -name "*.00.nhr" -o -name "*.nal" | sed 's/\.[0-9]*\.nhr$//' | sed 's/\.nhr$//' | sed 's/\.nal$//' | sort | uniq
    exit 1
fi

# Sanitise sequence headers for use as filenames
sanitize_header() {
    # Remove '>' character and replace spaces, slashes, and other problematic characters with underscores
    # Also limit length to avoid filename length issues
    echo "$1" | sed 's/^>//' | sed 's/[\/|:\*"<>? ]/_/g' | cut -c 1-100
}

# Function to process a single FASTA file
process_single_fasta() {
    local fasta_file="$1"
    local base_output="$2"
    
    echo "Processing: $(basename "$fasta_file")"
    
    # Get the sequence header (first line starting with '>')
    header=$(head -1 "$fasta_file")
    
    if [[ $header == ">"* ]]; then
        # Sanitize header for filename
        safe_header=$(sanitize_header "$header")
        output_file="${base_output}_${safe_header}.out"
        
        # Check if output file already exists
        if [ -e "$output_file" ]; then
            echo "  ? Skipping $header - output file already exists"
            return 0
        fi
        
        # Run BLASTn
        blastn -query "$fasta_file" \
               -db "$DB_PATH" \
               -out "$output_file" \
               -outfmt "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle" \
               $BLAST_OPTIONS
        
        # Check if BLASTn ran successfully
        if [ $? -eq 0 ]; then
            echo "  ? BLASTn completed for $header"
            return 0
        else
            echo "  ? Error running BLASTn on $header"
            return 1
        fi
    else
        echo "  ? Error: File doesn't appear to be in FASTA format (no '>' header found)"
        return 1
    fi
}

# Function to split multi-FASTA and process each sequence
process_multi_fasta() {
    local fasta_file="$1"
    local output_dir="$2"
    
    echo "Processing multi-FASTA file: $(basename "$fasta_file")"
    
    # Create temporary directory for split files
    temp_dir=$(mktemp -d)
    
    # Split the multi-FASTA file into individual sequences
    # This awk command writes each FASTA entry to a separate file
    awk '/^>/ {if (seq_count > 0) {close(outfile)}; outfile=sprintf("'"$temp_dir"'/seq_%d.fa", ++seq_count); print > outfile; next} {if (seq_count > 0) print >> outfile}' seq_count=0 "$fasta_file"
    
    # Count total number of split files
    total_files=$(ls "$temp_dir"/seq_*.fa | wc -l)
    echo "Found $total_files sequences in the multi-FASTA file"
    
    # Track skipped and processed counts
    skipped_count=0
    processed_count=0
    
    # Process each split file
    count=0
    for split_file in "$temp_dir"/seq_*.fa; do
        ((count++))
        
        # Get sequence header from the split file
        header=$(head -1 "$split_file")
        
        # Sanitize header for display and filename
        safe_header=$(sanitize_header "$header")
        output_file="${output_dir}/${safe_header}.out"
        
        echo "[$count/$total_files] Processing sequence: $header"
        
        # Check if output file already exists
        if [ -e "$output_file" ]; then
            echo "  ? Skipping - output file already exists"
            ((skipped_count++))
            continue
        fi
        
        # Run BLASTn on this sequence
        blastn -query "$split_file" \
               -db "$DB_PATH" \
               -out "$output_file" \
               -outfmt "6 qseqid sseqid pident length mismatch gapopen qstart qend sstart send evalue bitscore stitle" \
               $BLAST_OPTIONS
        
        # Check if BLASTn ran successfully
        if [ $? -eq 0 ]; then
            echo "  ? BLASTn completed for $header"
            ((processed_count++))
        else
            echo "  ? Error running BLASTn on $header"
        fi
    done
    
    # Clean up
    rm -rf "$temp_dir"
    
    echo "Completed multi-FASTA processing: $processed_count processed, $skipped_count skipped"
    return 0
}

# Check if input path exists
if [ ! -e "$INPUT_PATH" ]; then
    echo "Error: Input path $INPUT_PATH does not exist"
    exit 1
fi

# Display configuration
echo "===== BLAST Configuration ====="
echo "Database directory: $DB_DIR"
echo "Database name: $DB_NAME"
echo "Input: $INPUT_PATH"
echo "Output directory: $OUTPUT_DIR"
echo "BLAST options: $BLAST_OPTIONS"
echo "============================="

# Determine if input is a directory or a file
if [ -d "$INPUT_PATH" ]; then
    echo "Input is a directory. Processing all FASTA files..."
    
    # Count total number of FASTA files
    total_files=$(find "$INPUT_PATH" -maxdepth 1 \( -name "*.fasta" -o -name "*.fa" \) | wc -l)
    if [ "$total_files" -eq 0 ]; then
        echo "Error: No FASTA files found in $INPUT_PATH"
        exit 1
    fi

    echo "Found $total_files FASTA files to process"
    echo "Starting BLASTn searches..."

    # Track skipped and processed counts
    dir_skipped_count=0
    dir_processed_count=0
    
    # Process each FASTA file
    count=0
    for fasta_file in "$INPUT_PATH"/*.fa "$INPUT_PATH"/*.fasta; do
        # Skip if no files match pattern
        [ -e "$fasta_file" ] || continue
        
        # Get the filename without path and extension
        filename=$(basename "$fasta_file")
        base_name="${filename%.*}"
        
        # Increment counter and display progress
        ((count++))
        echo "[$count/$total_files] Processing file: $filename"
        
        # Check if it's a multi-FASTA file (contains multiple sequences)
        seq_count=$(grep -c "^>" "$fasta_file")
        
        if [ "$seq_count" -eq 0 ]; then
            echo "  ? Error: No FASTA sequences found in $fasta_file"
            continue
        elif [ "$seq_count" -eq 1 ]; then
            echo "  Single sequence FASTA file"
            # Get the sequence header
            header=$(head -1 "$fasta_file")
            safe_header=$(sanitize_header "$header")
            output_file="${OUTPUT_DIR}/${base_name}_${safe_header}.out"
            
            # Check if output file already exists
            if [ -e "$output_file" ]; then
                echo "  ? Skipping $filename - output file already exists"
                ((dir_skipped_count++))
            else
                process_single_fasta "$fasta_file" "$OUTPUT_DIR/${base_name}"
                ((dir_processed_count++))
            fi
        else
            echo "  Multi-FASTA file with $seq_count sequences"
            # Create a subdirectory for this file's results
            file_output_dir="$OUTPUT_DIR/${base_name}"
            mkdir -p "$file_output_dir"
            
            # Check if all output files exist (quick check for first and last sequence)
            first_header=$(head -1 "$fasta_file")
            safe_first_header=$(sanitize_header "$first_header")
            
            # If the directory exists and has files, assume some processing has been done
            # We'll let process_multi_fasta handle individual file skipping
            process_multi_fasta "$fasta_file" "$file_output_dir"
        fi
    done

    echo "Directory processing completed: $dir_processed_count files processed, $dir_skipped_count files skipped"

else
    # Input is a single file
    echo "Input is a single file..."
    
    # Check if it's a FASTA file
    if [[ "$INPUT_PATH" != *.fa && "$INPUT_PATH" != *.fasta ]]; then
        echo "Warning: Input file does not have .fa or .fasta extension. Continuing anyway..."
    fi
    
    # Get the filename without path and extension
    filename=$(basename "$INPUT_PATH")
    base_name="${filename%.*}"
    
    # Check if it's a multi-FASTA file (contains multiple sequences)
    seq_count=$(grep -c "^>" "$INPUT_PATH")
    
    if [ "$seq_count" -eq 0 ]; then
        echo "Error: No FASTA sequences found in $INPUT_PATH"
        exit 1
    elif [ "$seq_count" -eq 1 ]; then
        echo "Processing single sequence FASTA file: $filename"
        
        # Get the sequence header
        header=$(head -1 "$INPUT_PATH")
        safe_header=$(sanitize_header "$header")
        output_file="${OUTPUT_DIR}/${base_name}_${safe_header}.out"
        
        # Check if output file already exists
        if [ -e "$output_file" ]; then
            echo "  ? Skipping $filename - output file already exists"
        else
            process_single_fasta "$INPUT_PATH" "$OUTPUT_DIR/${base_name}"
        fi
    else
        echo "Processing multi-FASTA file: $filename with $seq_count sequences"
        process_multi_fasta "$INPUT_PATH" "$OUTPUT_DIR"
    fi
fi

echo "BLASTn searches completed. Results saved to $OUTPUT_DIR"
