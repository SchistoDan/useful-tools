#!/bin/bash
#SBATCH --job-name=mafft_align
#SBATCH --output=mafft_%j.out
#SBATCH --error=mafft_%j.err
#SBATCH --cpus-per-task=16
#SBATCH --mem=24G

# Usage: sbatch mafft_align.sh <input.fasta> <threads>
# Example: sbatch mafft_align.sh sequences.fasta 8

# Check if correct number of arguments provided
if [ "$#" -ne 2 ]; then
    echo "Usage: sbatch $0 <input.fasta> <threads>"
    exit 1
fi

INPUT_FASTA=$1
THREADS=$2

# Check if input file exists
if [ ! -f "$INPUT_FASTA" ]; then
    echo "Error: Input file $INPUT_FASTA not found"
    exit 1
fi

# Create output filename based on input
BASENAME=$(basename "$INPUT_FASTA" .fasta)
OUTPUT="${BASENAME}_aligned.fasta"

echo "Starting MAFFT alignment"
echo "Input file: $INPUT_FASTA"
echo "Threads: $THREADS"
echo "Output file: $OUTPUT"
echo "Job ID: $SLURM_JOB_ID"
echo "Started at: $(date)"

# Run MAFFT with auto algorithm selection and specified threads
mafft --auto --thread $THREADS --reorder --adjustdirectionaccurately --anysymbol --large --retree 2 "$INPUT_FASTA" > "$OUTPUT"

echo "Finished at: $(date)"
echo "Output saved to: $OUTPUT"
