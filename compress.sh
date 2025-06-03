#!/bin/bash
#SBATCH --job-name=pigz_compress-
#SBATCH --cpus-per-task=8
#SBATCH --mem=4G
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err


# Activate conda env
source ~/.bashrc  
conda activate dds

# Set input directory
INPUT_DIR=""

# Set output filename
OUTPUT_FILE="${INPUT_DIR##*/}.tar.gz"

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory $INPUT_DIR does not exist"
    exit 1
fi

echo "Starting compression of $INPUT_DIR"
echo "Output file: $OUTPUT_FILE"
echo "Using $SLURM_CPUS_PER_TASK CPU cores"

# Create tar archive and compress with pigz
tar --use-compress-program="pigz -p $SLURM_CPUS_PER_TASK" -cf "$OUTPUT_FILE" -C "$(dirname "$INPUT_DIR")" "$(basename "$INPUT_DIR")"

if [ $? -eq 0 ]; then
    echo "Compression completed successfully"
    echo "Output file size: $(du -h "$OUTPUT_FILE" | cut -f1)"
else
    echo "Compression failed"
    exit 1
fi
