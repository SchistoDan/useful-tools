# useful-tools
Collection of useful tools and script, developed for a range of functions


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
