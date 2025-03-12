# useful-tools
A collection of useful tools and script, developed for a range of bioinformatics-related functions.


## taxid_fetcher.py
Fetches taxonomic ID (taxids) for N samples using taxonomic heirarchy information for each sample. Script looks for taxid at species-level before traversing up the taxonomic 'tree'.
```bash
python taxid_fetcher.py <input_csv> <rankedlineage_path> <output_csv>

input:
- CSV file containing '[sample] ID', 'phylum', 'class', 'order', 'family', 'genus', and 'species' fields (i.e. taxonomic hierarchy).
- Path to rankedlineage.dmp file (downloaded with the newest NCBI tax dump: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz).

output:
- CSV file with taxid, matched_rank (taxonomic rank taxid corresponds to), NCBI lineage for taxid, lineage_mismatch (yes=higher taxonomy of input taxonomic heirarchy does not match higher taxonomy of fetched lineage) appended.
```




## add_sequences.py
Add sequences from one FASTA file to multiple other FASTA files, such as might be necessary when adding several possible contaminant sequences into a FASTA file containing a reference sequence. The script reads all FASTA files in the specified target directory, appends sequences from a specified additions file to each of them, and saves the modified FASTA files to the specified output directory.
 ```bash
python add_sequences.py --target_dir [TARGET_DIR] --additions_file [ADDITIONS_FILE] [--output_dir OUTPUT_DIR]

input:
- TARGET_DIR - Directory containing the target FASTA files to which sequences will be added.
- ADDITIONS_FILE - Path to the FASTA file containing the sequences to be added to each target FASTA file.

output:
OUTPUT_DIR - Directory where the modified FASTA files will be saved. Defaults to 'output' if not provided.
```






## blast.sh
Runs BLASTn on a directory of FASTA files or a single multi-FASTA file, where output files are named according to sequence headers in the FASTA file. The script can skip processing an input FASTA file or sequence if the output file already exists. Inputs, outputs and parameters must be changed in the script (i.e. they are hard-coded).
 ```bash
sbatch/srun blast.sh

input:
- Path to FASTA file/multi-FASTA file/directory containing N FASTA files.
- Path to the NCBI database directory
- Name of the BLAST database to use (without file extensions), e.g. nt, nr
- Additional BLAST options, e.g. - BLAST_OPTIONS="-evalue 5e-2 -max_target_seqs 50 -num_threads 36"

output:
- Path to output directory where BLAST results will be saved
```





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

