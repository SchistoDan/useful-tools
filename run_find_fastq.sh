#!/bin/bash
#SBATCH --job-name=find_fastq
#SBATCH --output=find_fastq_%j.out
#SBATCH --error=find_fastq_%j.err
#SBATCH --mem=8G
#SBATCH --cpus-per-task=4
#SBATCH --partition=medium

# ============================================================================
# SLURM wrapper for find_fastq.py
# ============================================================================
#
# Usage:
#   sbatch run_find_fastq.sh <input_file> <search_dir> <output_dir>
#
# Example:
#   sbatch run_find_fastq.sh strings.txt /path/to/seq_data /path/to/output
#
# ============================================================================

set -euo pipefail

# Check arguments
if [ $# -ne 3 ]; then
    echo "Usage: sbatch run_find_fastq.sh <input_file> <search_dir> <output_dir>"
    echo ""
    echo "Arguments:"
    echo "  input_file  - Text file with one search string per line"
    echo "  search_dir  - Directory to search recursively for FASTQ files"
    echo "  output_dir  - Output directory for copied files"
    exit 1
fi

INPUT_FILE="$1"
SEARCH_DIR="$2"
OUTPUT_DIR="$3"

# Path to the Python script (adjust as needed)
SCRIPT="./find_fastq.py"

# Log job info
echo "=============================================="
echo "Job ID: ${SLURM_JOB_ID}"
echo "Job name: ${SLURM_JOB_NAME}"
echo "Node: ${SLURM_NODELIST}"
echo "Start time: $(date)"
echo "=============================================="
echo ""
echo "Input file: ${INPUT_FILE}"
echo "Search directory: ${SEARCH_DIR}"
echo "Output directory: ${OUTPUT_DIR}"
echo ""

# Check inputs exist
if [ ! -f "${INPUT_FILE}" ]; then
    echo "ERROR: Input file not found: ${INPUT_FILE}"
    exit 1
fi

if [ ! -d "${SEARCH_DIR}" ]; then
    echo "ERROR: Search directory not found: ${SEARCH_DIR}"
    exit 1
fi

if [ ! -f "${SCRIPT}" ]; then
    echo "ERROR: Python script not found: ${SCRIPT}"
    exit 1
fi

# Run the script
echo "Running find_fastq.py..."
echo ""

python3 "${SCRIPT}" \
    --in "${INPUT_FILE}" \
    --search "${SEARCH_DIR}" \
    --out "${OUTPUT_DIR}"

echo ""
echo "=============================================="
echo "End time: $(date)"
echo "=============================================="
