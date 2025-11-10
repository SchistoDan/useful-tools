# useful-tools
A collection of useful tools/scripts/commands, developed for a range of bioinformatics-related functions.


## Taxonomy resolvers
### taxid_fetcher.py
Fetches taxonomic ID (taxids) for N samples using taxonomic heirarchy information for each sample. Script looks for taxid at species-level before traversing up the taxonomic 'tree'.
```bash
python taxid_fetcher.py <input_csv> <rankedlineage_path> <output_csv>

input:
- CSV file containing '[sample] ID', 'phylum', 'class', 'order', 'family', 'genus', and 'species' fields (i.e. taxonomic hierarchy).
- Path to rankedlineage.dmp file (downloaded with the newest NCBI tax dump: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz).

output:
- CSV file with taxid, matched_rank (taxonomic rank taxid corresponds to), NCBI lineage for taxid, lineage_mismatch (yes=higher taxonomy of input taxonomic heirarchy does not match higher taxonomy of fetched lineage) appended.
```
## taxonomy_analyser.py
Determines the number of unique taxa at each rank (phylum, class, order, family, genus, species), the number of unique families per class, and the number of unique families per order. Requires a list of Process IDs (no header), and a CSV file containing the same Process IDs and heirarchical taxonomy information (i.e. columns for phylum, class, order, family, genus, species).
```bash
python taxonomy_analyser.py -c <input_csv>.csv -p <process_id_list>.txt <output_stats>.txt

input:
- CSV file containing '[Process] ID', 'phylum', 'class', 'order', 'family', 'genus', and 'species' fields (i.e. taxonomic hierarchy).
- TXT file containing list of Process IDs to search for in CSV file.

output:
- TXT file with taxonomy information
```
## taxonomy_splitter.py
This script takes a CSV file containing specimen information with NCBI taxonomic IDs and splits it into multiple files based on the phylum of each specimen.
For each taxid, the script queries the NCBI Taxonomy database to determine the phylum. It then creates separate output CSV files for each phylum found.
```bash
python taxonomy_splitter.py --input your_input_file.csv --email your.email@example.com

input CSV:
- ID,forward,reverse,taxid,type_status

output CSV(s):
- Named [input_file_name_without_ext]_[phylum].csv
```






## BLASTn searching
### blast.sh
Runs BLASTn on a directory of FASTA files or a single multi-FASTA file, where output files are named according to sequence headers in the FASTA file. The script can skip processing an input FASTA file or sequence if the output file already exists. Inputs, outputs and parameters must be changed within the script.
 ```bash
sbatch/srun blast.sh

input:
- Path to FASTA file/multi-FASTA file/directory containing N FASTA files.
- Path to the NCBI database directory (or FASTA file to create BLAST db from)
- Name of created BLAST db (if FASTA given)
- Name of the BLAST database to use (without file extensions), e.g. nt, nr
- Additional BLAST options, e.g. - BLAST_OPTIONS="-evalue 1e-5 -max_target_seqs 50 -num_threads 16"

output:
- Path to output directory where BLAST results will be saved
```

### ena-blast.sh
- Runs BLASTn on an input multi-fasta file by submitting sequences (in batches of 30 (maximum allowed)) via HTTP requests to EBI's remote servers, thereby outsourcing resources to EBI's side. Outputs a aingle TSV file per sequence in the multi-fasta.
- Clone [EBI's web services/api repo](https://github.com/ebi-jdispatcher/webservice-clients/tree/master), and run:
```
python ./python/ncbiblast.py \
  --multifasta \
  --email $EMAIL \
  --program blastn \
  --stype dna \
  --database em_all \
  --sequence $INPUT \
  --maxJobs 30 \
  --useSeqId \
  --exp 1e-5 \
  --outformat tsv \
  --verbose

# em_all = All nucleotide sequences on ENA.
# em_inv = Invert nucleotide sequences on ENA.
# --exp = E-value threshold (default 10)
```
### parse_blast_results.py
Parses the BLAST results genreated by `blast.sh` OR `ena-blast.sh` into a combined CSV file. `blast.sh` = standard output format 6. `ena-blast.sh` = custom output format from ENA. Organised the CSV file as follows for the first 5 hits:
sequence_name | hit1_description | hit1_identity_percent | hit1_family | hit2_description | hit2_identity_percent | hit2_family

```
python parse_blast_results.py <input_dir> ./output.csv

input_dir = Directory containing BLAST output .TSV/.TXT/.OUT files produced by blast.sh (flat structure)
output.csv = Combined BLASt results file for each processed TSV file
```


## File manipulation
## filer_mover.py
Moves files containing user-specified string in the filename in one directory to another.
 ```bash
python filer_mover.py <input_directory> <output_directory> <string_in_filename>

input:
- Directory to search for files to rename
- The string in the filename to search for (e.g. results, output, etc.)

output:
- Directory to move specified files to
```

## compress.sh
Gzip target directory using Pigz and packages it into a tarball (.tar.gz) file.

```bash
    sbatch compress.sh

 INPUT_DIR = Path to target directory to compress (set in script)
 OUTPUT_FILE = ${INPUT_DIR##*/}.tar.gz

```





## CSV/TSV manipulation
### merge_c-tsv_files.py
Combine multiple CSV/TSV files into a single file, preserving header structure from first input file (subsequent input file columns will be reordered to align with the first input files header order).

 ```bash
python merge_c-tsv_files.py --input file1.csv file2.csv file3.csv --output merged.csv
python merge_c-tsv_files.py --input data/*.csv --output combined.csv
python merge_c-tsv_files.py --input data/*.tsv --output output.tsv

input:
- CSV or TSV files to combine

output:
- CSV or TSV file path(s)
```
### csv_splitter.py
Splits a CSV file into multiple smaller CSV files with a specified number of rows per file. Each output file will contain the header row from the original CSV file followed by the specified number of data rows.
If the input CSV has fewer rows than specified for the final chunk, all remaining rows will be included in the last output file.

 ```bash
python csv_splitter.py input_file rows_per_file

input:
- Path to the input CSV file to be split

output:
-  Number of rows each output file should contain (excluding header)
```
### Remove Trailing Whitespace from a specified column in a CSV file
- Replace COLUMN_NAME with your column of interest, and replace input and output csv filenames.
```
python3 -c "
import csv, sys
reader = csv.DictReader(sys.stdin)
writer = csv.DictWriter(sys.stdout, reader.fieldnames)
writer.writeheader()
total_removed = 0
rows_modified = 0
for row in reader:
    original = row['COLUMN_NAME']
    trimmed = original.rstrip()
    removed = len(original) - len(trimmed)
    if removed > 0:
        total_removed += removed
        rows_modified += 1
    row['COLUMN_NAME'] = trimmed
    writer.writerow(row)
print(f'Removed {total_removed} trailing whitespace characters from {rows_modified} rows', file=sys.stderr)
" < input.csv > output.csv
```
### filter_csv.py
Filters CSV rows based on exact matches from a text file containing target values.
Script performs case-insensitive matching with robust whitespace and hidden character handling.
```
Usage:
    python filter_csv.py --csv <input_csv> --output <output_csv> --txt <filter_file> --filter <column_name>
Input:
    - CSV file with headers (any format)
    - Text file containing filter values (one per line)
    - Column name to filter on (e.g., "Process ID")
Output:
    - Filtered CSV file containing only rows where the specified column value matches 
      (case-insensitive, whitespace-normalized) any value in the filter text file
```



## FASTA file processing
### mafft_aligner.sh
A SLURM HPC-compatible bash script, submitted with sbatch, utilising the mafft aligner to create an MSA from an input multi-FASTA sequence. Requires mafft to be installed in an activated conda env.
```
sbatch mafft_align.sh <input.fasta> <threads>
```
### fasta_length_stats.py
Calculate the number of sequences, minimum length, maximum length, and average length of sequences present within an input multi-fasta, and output these statistics to a text file.

 ```bash
python fasta_length_stats.py -i <multi.fasta> -o <length_stats.txt>  

input:
- Multi-FASTA file

output:
- TXT file containing sequence length statistics.

Filename: multi.fasta
Total sequences: 18602
Minimum length: 51
Minimum length sequence: >shortest sequence header
Maximum length: 1105
Maximum length sequence: >longest sequence header
Average length: 188.94
```
### add_sequences.py
Add sequences from one FASTA file to multiple other FASTA files, such as might be necessary when adding several possible contaminant sequences into a FASTA file containing a reference sequence. The script reads all FASTA files in the specified target directory, appends sequences from a specified additions file to each of them, and saves the modified FASTA files to the specified output directory.
 ```bash
python add_sequences.py --target_dir [TARGET_DIR] --additions_file [ADDITIONS_FILE] [--output_dir OUTPUT_DIR]

input:
- TARGET_DIR - Directory containing the target FASTA files to which sequences will be added.
- ADDITIONS_FILE - Path to the FASTA file containing the sequences to be added to each target FASTA file.

output:
OUTPUT_DIR - Directory where the modified FASTA files will be saved. Defaults to 'output' if not provided.
```
### fasta_extractor.py
Extracts sequences from a multi-FASTA file based on a list of IDs. It creates a new FASTA file with the matched sequences and outputs a CSV log of which IDs were found and which weren't.

 ```bash
    python fasta_extractor.py -i input.fasta -o filtered.fasta -l results.csv -id ids.txt

input:
    -i, --input: Path to the input multi-FASTA file
    -o, --output: Path to the output FASTA file where matched sequences will be saved
    -l, --log: Path to the output CSV log file for tracking found/not found IDs
    -id, --ids: Path to a text file containing IDs to search for (one per line)

output:
    1. A FASTA file containing only the sequences that match the provided IDs
    2. A CSV file with two columns: "Found" and "Not Found"
       - "Found" column lists IDs that were found in the input FASTA file
       - "Not Found" column lists IDs that were not found

```
### extract_best_barcode.py
Extract the 'best' barcode consensus sequences from a fasta_compare CSV output and corresponding FASTA files. Written for BGE.
1. Filters fasta_compare CSV rows where 'best_sequence' = 'yes'
2. Optionally merges statistics data from a BGEE summary statistics CSV file
3. Extracts corresponding sequences from source FASTA files
4. Copies corresponding alignment FASTA files from mode-specific directories
5. Copies fastp-generated JSON files from subdirectories to a flattened structure
6. Outputs curated CSV file, extracted FASTA sequence file, compressed alignment FASTA files and JSON files, and a script log

 ```bash
 python extract_best_barcodes.py -i input.csv -o best_barcodes/ -f best_barcodes.fasta -a alignment_file/parent/directory -c BGEE_summary_stats.csv -j json_file/parent/directory

input:
   -i/--input: Input fasta_compare CSV file output
   -o/--out: Output directory for all generated files
   -f/--fasta: Output FASTA filename containing the 'best' sequences
   -a/--align: Parent directory containing mode-specific alignment subdirectories (optional)
   -c/--csv: BGEE summary stats CSV file to merge with fasta_compare CSV file output (optional)
   -j/--json: Parent directory containing subdirectories with JSON files (optional)

output:
   - {input_name}-best.csv: Filtered CSV with optional stats columns
   - {fasta_name}: Extracted sequences in FASTA format
   - alignment_files.tar.gz: Compressed alignment files (if -a provided)
   - fastp_json.tar.gz: Compressed JSON files (if -j provided)
   - {fasta_stem}.log: Complete operation log
```
### Determine number of valid (not empty) sequences in multi-FASTA file
Extracts sequences from a multi-FASTA file if they are not empty.
```
awk '
  /^>/ {                     # header line
    if (seqlen > 0) c++;     # finished a non-empty record
    seqlen = 0;              # reset length for next record
    next
  }
  { gsub(/[ \t\r\n]/, "");   # remove whitespace
    seqlen += length         # accumulate sequence length
  }
  END {
    if (seqlen > 0) c++;     # last record
    print c
  }
' [path/to/your.fasta]
```
### fasta_filter.py
Remove specified sequences from a multi-FASTA using a list of FASTA sequence headers
```
python fasta_filter.py input_fasta headers_file -k keep.fasta -r remove.fasta

Requires:
input_fasta: A multi-FASTA file with sequences to remove
headers_file: A txt file with 1 FASTA sequence header per line to find and remove in the input_fasta
-k/--keep: A multi-FASTA file = input_fasta - headers (and corresponding sequences) in headers_file
-r/--remove: A multi-FASTA file containing sequences removed from input_fasta
```
### compare_fasta.py
```
This script takes two FASTA files as input, extracts and compares their headers,
then outputs a filtered list based on the specified filtering criteria.

Usage:
    python compare_fasta.py -i file1.fasta file2.fasta -o output.txt --filter keep
    python compare_fasta.py -i file1.fasta file2.fasta -o output.txt --filter remove

Filter modes:
    - keep: Output headers that ARE present in both files (intersection)
    - remove: Output headers that are NOT in both files (present in only one file)

Output format:
    - One header per line in a .txt file
    - Headers are written without the '>' character
    - Results are sorted alphabetically for consistency
```



### Run Fatsp, and parse Fastp read QC statistics
## fastp_run.sh
Run fastp on a flat or nested directory structure of fastq.gz or fq.gz raw PE read files.
```
 Usage: sbatch fastp_run.sh <input> <output_directory> [threads]
 
 <input> can be either:
   - A text file listing directories (one per line)
   - A directory containing FASTQ files

Example 1: ./fastp_run.sh dir_list.txt /path/to/output 16
Example 2: ./fastp_run.sh /path/to/fastq_files /path/to/output 16
```

## parse_fastp_stats.py
Parse Fastp read QC metrics from JSON files output by Fastp into a single CSV file.

 ```bash
 python parse_fastp_stats.py -i path_1/trimmed_data/ path_2/trimmed_data/ path_3/trimmed_data/ -o output.csv

input:
   -i: Path to at least one 'trimmed_data' directory, each containing sample-specific subdirectories with a fastp JSON file in each subdirectory.

output:
   -o: Path to output CSV file
```

## extract_read_counts.py
Parse just R1, R2, and total raw read counts from Fastp JSON files into a single CSV file.

 ```bash
 python extract_read_counts.py -i path_1/trimmed_data/ -o output.csv

input:
   -i: Path to a directory containg >1 Fastp JSON files (flat structure) or >1 sub-directories containing >1 Fastp JSON file each.

output:
   -o: Path to output CSV file







# useful-commands

**General**
```bash
# Count files with specific extension in dir
	find /path/to/directory -type f -name "*.fasta" | wc -l

# Count subdirs in dir
	find /path/to/directory -mindepth 1 -type d | wc -l

# Show files with non-ASCII characters
	find /gpfs/nhmfsa/bulk/share/data/mbl/share/scratch/MGE/protein_references/benchmarking_data_570_refs-contam_refs_final14 -type f -print0 | LC_ALL=C grep -l '.' -z

# Remove any hidden files in dir
	find /gpfs/nhmfsa/bulk/share/data/mbl/share/scratch/MGE/cox1/benchmarking/contam/new_mge_patch-021224/mge_standard_r1_13_15_s50_100_benchmarking_contam -type f -name ".*" -delete
	
# tar and compress directory
	tar czf name_of_directory_to_tar.tar.gz name_of_directory_to_tar/

# untar and gunzip compressed file
	tar -xfv archive.tar -C /path/to/destination

# Change file encoding to utf-8
	
# List number of files in directory
	ls -1 | wc -l

# List number of files in dir and all subdirs that belong to memory
	find . -user dparsons -type f | wc -l
```

**SLURM**
```
# View job info
	sacct --format=JobID,JobName,MaxRSS%32,MaxVMSize%32,Elapsed,Partition,End,ExitCode
		+ 	--job [JobID] = to specify 1 job only

# View job resource usage
	sstat --format=JobID,AveCPU,AveRSS,AveVMSize -j [job_id]
	
# Monitor live cluster memory usage
	watch -n 1 'free -h'
```

**Conda**
```
# Create .yaml from conda env:
	conda env export > [environment].yaml
	
# Create .yaml from conda env without platform-specific build numbers (more portable):
	conda env export --no-builds > environment.yaml

# Create conda env from .yaml file:
	conda env create -f [environment].yaml

# Remove/delete conda env:
	conda env remove --name [env_name]
	
# Update all packages in env:
	conda update --all
	
# Update conda env with updated yaml (--prune removes uneeded dependencies)
	conda env update --name myenv --file environment.yaml --prune
```

**Screen**
```
# Start a new screen session
	screen

# Start a named screen session
	screen -S session_name

# List running screen sessions
	screen -ls

# Reattach to a session
	screen -r session_name   # if you know the name
	screen -r 1234          # if you know the process ID

# Detach from current session (without killing it)
# Press Ctrl+A, then press d

# Kill a screen session
	screen -X -S session_name quit
```

