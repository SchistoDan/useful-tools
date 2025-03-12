# useful-tools
A collection of useful tools and script, developed for a range of bioinformatics-related functions.


## taxid_fetcher.py
Fetches taxonomic ID (taxids) for N samples using taxonomic heirarchy information for each sample. Script looks for taxid at species-level before traversing up the taxonomic 'tree'.
```bash
python taxid_fetcher.py <input_csv> <rankedlineage_path> <output_csv>

input:
- CSV file containing '[sample] ID', 'phylum', 'class', 'order', 'family', 'genus', and 'species' fields (i.e. taxonomic hierarchy)
- Path to rankedlineage.dmp file (downloaded with the newest NCBI tax dump: https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz)

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
