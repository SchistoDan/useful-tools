#!/usr/bin/env python3
"""
Extract best sequences from CSV metadata and corresponding FASTA files.

This script:
1. Filters CSV rows where 'best_sequence' = 'yes'
2. Optionally merges statistics data from a separate CSV file
3. Extracts corresponding sequences from FASTA files
4. Copies corresponding alignment files from mode-specific directories
5. Copies JSON files from subdirectories to a flattened structure
6. Outputs filtered CSV, extracted FASTA, alignment files, JSON files, and operation log

Features:
- Filters sequences based on 'best_sequence' column
- Merges stats data using search_id matching and mode detection
- Supports both merge_mode and concat_mode alignment file copying
- Handles fcleaner sequences with appropriate directory selection
- Extracts different stat columns based on fcleaner status
- Copies and compresses JSON files from subdirectories
- Compresses alignment and JSON files into separate tar.gz archives
- Comprehensive logging for debugging and error tracking

Arguments:
-i/--input: Input CSV file with sequence metadata
-f/--fasta: Output FASTA filename
-o/--out: Output directory for all generated files
-a/--align: Parent directory containing mode-specific alignment subdirectories (optional)
-c/--csv: Stats CSV file to merge with sequence metadata (optional)
-j/--json: Parent directory containing subdirectories with JSON files (optional)

Output files:
- {input_name}-best.csv: Filtered CSV with optional stats columns
- {fasta_name}: Extracted sequences in FASTA format
- alignment_files.tar.gz: Compressed alignment files (if -a provided)
- fastp_json.tar.gz: Compressed JSON files (if -j provided)
- {fasta_stem}.log: Complete operation log
"""

import argparse
import csv
import os
import sys
import shutil
import tarfile
from pathlib import Path
from typing import List, Dict, Tuple, Optional


class SequenceExtractor:
    """Handles extraction of best sequences from CSV and FASTA files."""
    
    def __init__(self, input_csv: str, output_dir: str, fasta_name: str, parent_dir: Optional[str] = None, stats_csv: Optional[str] = None, json_dir: Optional[str] = None):
        self.input_csv = Path(input_csv)
        self.output_dir = Path(output_dir)
        self.output_fasta = self.output_dir / fasta_name
        self.log_file = self.output_dir / f"{Path(fasta_name).stem}.log"
        self.csv_output = self.output_dir / f"{self.input_csv.stem}-best.csv"
        self.parent_dir = Path(parent_dir) if parent_dir else None
        self.stats_csv = Path(stats_csv) if stats_csv else None
        self.json_dir = Path(json_dir) if json_dir else None
        self.alignment_output_dir = self.output_dir / "alignment_files"
        self.json_output_dir = self.output_dir / "fastp_json"
        
        # Define alignment directories
        self.align_dirs = []
        if self.parent_dir:
            self.concat_align_dir = self.parent_dir / "concat_mode" / "alignment"
            self.merge_align_dir = self.parent_dir / "merge_mode" / "alignment"
            self.align_dirs = [self.concat_align_dir, self.merge_align_dir]
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize log
        self.log_messages = []
        self._log(f"Starting extraction process")
        self._log(f"Input CSV: {self.input_csv}")
        self._log(f"Output directory: {self.output_dir}")
        self._log(f"Output FASTA: {self.output_fasta}")
        self._log(f"Output CSV: {self.csv_output}")
        if self.parent_dir:
            self._log(f"Parent directory: {self.parent_dir}")
            self._log(f"Looking for alignment dirs:")
            for align_dir in self.align_dirs:
                self._log(f"  - {align_dir}")
            self._log(f"Alignment output: {self.alignment_output_dir}")
        if self.stats_csv:
            self._log(f"Stats CSV: {self.stats_csv}")
        if self.json_dir:
            self._log(f"JSON directory: {self.json_dir}")
            self._log(f"JSON output: {self.json_output_dir}")
        
        # Load stats data if provided
        self.stats_data = []
        if self.stats_csv:
            self.stats_data = self._load_stats_csv()
    
    def _log(self, message: str):
        """Add message to log."""
        self.log_messages.append(message)
        print(message)  # Also print to console
    
    def _write_log(self):
        """Write all log messages to file."""
        try:
            with open(self.log_file, 'w') as f:
                for message in self.log_messages:
                    f.write(f"{message}\n")
            self._log(f"Log written to: {self.log_file}")
        except Exception as e:
            print(f"ERROR: Could not write log file: {e}")
    
    def extract_best_sequences(self) -> bool:
        """Main extraction process."""
        try:
            # Step 1: Filter CSV for best sequences
            best_rows = self._filter_csv()
            if not best_rows:
                self._log("WARNING: No sequences with 'best_sequence' = 'yes' found")
                return False
            
            # Step 1.5: Add stats data to best rows if stats CSV provided
            if self.stats_csv and self.stats_data:
                best_rows = self._merge_stats_data(best_rows)
            
            # Write filtered CSV (after stats are potentially added)
            self._write_filtered_csv_with_stats(best_rows)
            
            # Step 2: Extract sequences from FASTA files
            extracted_count = self._extract_fasta_sequences(best_rows)
            
            # Step 3: Copy alignment files if parent directory provided
            if self.parent_dir:
                self._copy_alignment_files(best_rows)
            
            # Step 4: Copy JSON files if JSON directory provided
            if self.json_dir:
                self._copy_json_files()
            
            self._log(f"Extraction complete. {extracted_count} sequences extracted.")
            return extracted_count > 0
            
        except Exception as e:
            self._log(f"ERROR: Extraction failed: {e}")
            return False
        finally:
            self._write_log()
    
    def _filter_csv(self) -> List[Dict]:
        """Filter CSV for rows where best_sequence = 'yes'."""
        self._log("Filtering CSV for best sequences...")
        
        if not self.input_csv.exists():
            raise FileNotFoundError(f"Input CSV not found: {self.input_csv}")
        
        best_rows = []
        total_rows = 0
        
        try:
            with open(self.input_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                # Validate required columns
                required_cols = ['file', 'seq_id', 'best_sequence']
                missing_cols = [col for col in required_cols if col not in headers]
                if missing_cols:
                    raise ValueError(f"Missing required columns: {missing_cols}")
                
                for row in reader:
                    total_rows += 1
                    if row.get('best_sequence', '').strip().lower() == 'yes':
                        best_rows.append(row)
                
                self._log(f"Found {len(best_rows)} best sequences out of {total_rows} total rows")
                
                # Store headers for later use (will be updated if stats are added)
                self.original_headers = headers
                
        except Exception as e:
            raise Exception(f"Error reading CSV: {e}")
        
        return best_rows
    
    def _write_filtered_csv(self, rows: List[Dict], headers: List[str]):
        """Write filtered rows to output CSV (legacy method for compatibility)."""
        try:
            with open(self.csv_output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            self._log(f"Filtered CSV written to: {self.csv_output}")
        except Exception as e:
            self._log(f"ERROR: Could not write filtered CSV: {e}")
    
    def _write_filtered_csv_with_stats(self, rows: List[Dict]):
        """Write filtered rows to output CSV with potentially added stats columns."""
        if not rows:
            self._log("WARNING: No rows to write to CSV")
            return
        
        # Define the exact column order you want (complete list)
        desired_order = [
            'file', 'seq_id', 'process_id', 'parameters', 'n_reads_in', 'n_reads_skipped', 'n_reads_aligned', 
            'cov_min', 'cov_max', 'cov_avg', 'cov_med', 'cleaning_input_reads', 'cleaning_kept_reads', 
            'cleaning_removed_at', 'cleaning_removed_human', 'cleaning_removed_outlier', 
            'cleaning_ambig_bases', 'cleaning_cov_min', 'cleaning_cov_max', 'cleaning_cov_percent', 'cleaning_cov_avg'
            'leading_gaps', 'trailing_gaps', 'internal_gaps', 'ambiguous_bases', 'longest_stretch',
            'barcode_length', 'barcode_ambiguous_bases', 'barcode_longest_stretch', 'barcode_rank', 'full_rank',
            'best_sequence', 'selected_full_fasta', 'selected_barcode_fasta',
            
            
            
        ]
        
        # Get ALL possible column names from ALL rows
        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())
        
        # Start with desired order for columns that exist - ONLY include desired columns
        ordered_headers = []
        for col in desired_order:
            if col in all_columns:
                ordered_headers.append(col)
        
        # Don't add any remaining columns - stick strictly to desired order
        all_headers = ordered_headers
        
        # Ensure all rows have all columns (fill missing with empty string)
        # But exclude certain columns for fcleaner samples
        fcleaner_excluded_columns = ['n_reads_in', 'n_reads_skipped', 'n_reads_aligned', 'cov_min', 'cov_max', 'cov_avg', 'cov_med']
        
        normalized_rows = []
        for row in rows:
            normalized_row = {}
            
            # Check if this is a fcleaner sample
            is_fcleaner = '_fcleaner' in row.get('seq_id', '')
            
            for header in all_headers:
                # Skip fcleaner-excluded columns for fcleaner samples
                if is_fcleaner and header in fcleaner_excluded_columns:
                    normalized_row[header] = ''
                else:
                    normalized_row[header] = row.get(header, '')
            normalized_rows.append(normalized_row)
        
        try:
            with open(self.csv_output, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=all_headers)
                writer.writeheader()
                writer.writerows(normalized_rows)
            self._log(f"Filtered CSV written to: {self.csv_output}")
            self._log(f"CSV contains {len(all_headers)} columns and {len(normalized_rows)} rows")
        except Exception as e:
            self._log(f"ERROR: Could not write filtered CSV: {e}")
    
    def _extract_fasta_sequences(self, best_rows: List[Dict]) -> int:
        """Extract sequences from FASTA files."""
        self._log("Extracting sequences from FASTA files...")
        
        extracted_sequences = []
        
        for i, row in enumerate(best_rows, 1):
            fasta_path = row['file']
            seq_id = row['seq_id']
            
            self._log(f"Processing {i}/{len(best_rows)}: {seq_id}")
            
            sequence = self._find_sequence_in_fasta(fasta_path, seq_id)
            if sequence:
                extracted_sequences.append(sequence)
            else:
                self._log(f"WARNING: Sequence {seq_id} not found in {fasta_path}")
        
        # Write extracted sequences to output FASTA
        if extracted_sequences:
            self._write_fasta(extracted_sequences)
        
        return len(extracted_sequences)
    
    def _find_sequence_in_fasta(self, fasta_path: str, target_seq_id: str) -> Tuple[str, str]:
        """Find sequence by exact seq_id match in FASTA file."""
        fasta_file = Path(fasta_path)
        
        if not fasta_file.exists():
            self._log(f"ERROR: FASTA file not found: {fasta_path}")
            return None
        
        try:
            with open(fasta_file, 'r', encoding='utf-8') as f:
                current_header = None
                current_sequence = []
                
                for line in f:
                    line = line.strip()
                    
                    if line.startswith('>'):
                        # Check if previous sequence matches our target
                        if current_header and target_seq_id in current_header:
                            sequence = ''.join(current_sequence)
                            return (current_header, sequence)
                        
                        # Start new sequence
                        current_header = line
                        current_sequence = []
                    
                    elif current_header:  # Only collect sequence if we have a header
                        current_sequence.append(line)
                
                # Check the last sequence
                if current_header and target_seq_id in current_header:
                    sequence = ''.join(current_sequence)
                    return (current_header, sequence)
        
        except Exception as e:
            self._log(f"ERROR: Could not read FASTA file {fasta_path}: {e}")
        
        return None
    
    def _write_fasta(self, sequences: List[Tuple[str, str]]):
        """Write extracted sequences to output FASTA file."""
        try:
            with open(self.output_fasta, 'w', encoding='utf-8') as f:
                for header, sequence in sequences:
                    f.write(f"{header}\n")
                    # Write sequence in 80-character lines (standard FASTA format)
                    for i in range(0, len(sequence), 80):
                        f.write(f"{sequence[i:i+80]}\n")
            
            self._log(f"FASTA file written to: {self.output_fasta}")
            self._log(f"Extracted {len(sequences)} sequences")
            
        except Exception as e:
            self._log(f"ERROR: Could not write FASTA file: {e}")
    
    def _copy_alignment_files(self, best_rows: List[Dict]):
        """Copy alignment files for best sequences from the correct mode-specific alignment directory."""
        self._log("Copying alignment files...")
        
        # Check parent directory exists
        if not self.parent_dir.exists():
            self._log(f"ERROR: Parent directory not found: {self.parent_dir}")
            return
        
        if not self.parent_dir.is_dir():
            self._log(f"ERROR: Parent path is not a directory: {self.parent_dir}")
            return
        
        # Create alignment output directory
        try:
            self.alignment_output_dir.mkdir(parents=True, exist_ok=True)
            self._log(f"Created alignment output directory: {self.alignment_output_dir}")
        except Exception as e:
            self._log(f"ERROR: Could not create alignment directory: {e}")
            return
        
        copied_files = 0
        missing_files = 0
        
        for row in best_rows:
            seq_id = row['seq_id']
            file_path = row['file']
            
            # Extract search string from seq_id (take first 4 underscore-separated parts)
            search_id = '_'.join(seq_id.split('_')[:5])
            
            # Determine which directory to search based on file path and seq_id content
            target_align_dir = None
            mode_name = None
            subdir_name = "alignment"  # default subdirectory
            
            # Check if this is an fcleaner sequence
            if '_fcleaner' in seq_id:
                subdir_name = "fasta_cleaner/filter_pass_seqs"
            
            if 'concat_mode' in file_path:
                target_align_dir = self.parent_dir / "concat_mode" / subdir_name
                mode_name = "concat_mode"
            elif 'merge_mode' in file_path:
                target_align_dir = self.parent_dir / "merge_mode" / subdir_name
                mode_name = "merge_mode"
            else:
                self._log(f"WARNING: Could not determine mode for file: {file_path} (seq_id: {seq_id})")
                missing_files += 1
                continue
            
            self._log(f"Looking for alignment file containing '{search_id}' in {mode_name}/{subdir_name} (from seq_id: {seq_id})")
            
            # Check if target directory exists
            if not target_align_dir.exists() or not target_align_dir.is_dir():
                self._log(f"ERROR: Directory not found: {target_align_dir}")
                missing_files += 1
                continue
            
            # Search for files containing the search_id as substring
            matching_files = []
            try:
                for file in target_align_dir.iterdir():
                    if file.is_file() and file.suffix in ['.fasta', '.fas'] and search_id in file.name:
                        matching_files.append(file)
            except Exception as e:
                self._log(f"ERROR: Could not read directory {target_align_dir}: {e}")
                missing_files += 1
                continue
            
            if not matching_files:
                self._log(f"WARNING: No alignment file containing '{search_id}' found in {target_align_dir}")
                missing_files += 1
                continue
            
            # Handle multiple matches
            if len(matching_files) > 1:
                file_names = [f.name for f in matching_files]
                self._log(f"WARNING: Multiple alignment files found containing '{search_id}': {file_names}")
                self._log(f"Using first match: {matching_files[0].name}")
            
            # Copy the first (or only) matching file
            source_file = matching_files[0]
            try:
                # Use original filename for destination
                dest_file = self.alignment_output_dir / source_file.name
                shutil.copy2(source_file, dest_file)
                self._log(f"Copied: {source_file.name} from {mode_name}")
                copied_files += 1
            except Exception as e:
                self._log(f"ERROR: Failed to copy {source_file.name} from {target_align_dir}: {e}")
                missing_files += 1
        
        self._log(f"Alignment files copied: {copied_files}")
        self._log(f"Missing alignment files: {missing_files}")
        
        # Compress alignment directory
        if copied_files > 0:
            self._compress_alignment_directory()
    
    def _compress_alignment_directory(self):
        """Compress the alignment files directory using gzip (tar.gz format)."""
        self._log("Compressing alignment files...")
        
        try:
            # Create tar.gz archive
            archive_path = self.output_dir / "alignment_files.tar.gz"
            
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(self.alignment_output_dir, arcname='alignment_files')
            
            self._log(f"Alignment files compressed to: {archive_path}")
            
            # Remove the uncompressed directory
            shutil.rmtree(self.alignment_output_dir)
            self._log("Removed uncompressed alignment directory")
            
        except Exception as e:
            self._log(f"ERROR: Failed to compress alignment files: {e}")
    
    def _copy_json_files(self):
        """Copy JSON files from subdirectories to a flattened structure."""
        self._log("Copying JSON files...")
        
        # Check JSON parent directory exists
        if not self.json_dir.exists():
            self._log(f"ERROR: JSON parent directory not found: {self.json_dir}")
            return
        
        if not self.json_dir.is_dir():
            self._log(f"ERROR: JSON path is not a directory: {self.json_dir}")
            return
        
        # Create JSON output directory
        try:
            self.json_output_dir.mkdir(parents=True, exist_ok=True)
            self._log(f"Created JSON output directory: {self.json_output_dir}")
        except Exception as e:
            self._log(f"ERROR: Could not create JSON directory: {e}")
            return
        
        copied_files = 0
        subdirs_processed = 0
        
        try:
            # Iterate through immediate subdirectories only
            for item in self.json_dir.iterdir():
                if not item.is_dir():
                    continue
                
                subdirs_processed += 1
                subdir_name = item.name
                self._log(f"Processing subdirectory: {subdir_name}")
                
                # Look for JSON files in this subdirectory
                json_files = []
                try:
                    for file in item.iterdir():
                        if file.is_file() and file.suffix.lower() == '.json':
                            json_files.append(file)
                except Exception as e:
                    self._log(f"ERROR: Could not read subdirectory {item}: {e}")
                    continue
                
                if not json_files:
                    self._log(f"WARNING: No JSON files found in {subdir_name}")
                    continue
                
                # Copy JSON files to flattened structure
                for json_file in json_files:
                    try:
                        # Use original filename (flatten structure)
                        dest_file = self.json_output_dir / json_file.name
                        
                        # Handle potential filename conflicts
                        counter = 1
                        original_dest_file = dest_file
                        while dest_file.exists():
                            stem = original_dest_file.stem
                            suffix = original_dest_file.suffix
                            dest_file = self.json_output_dir / f"{stem}_{counter}{suffix}"
                            counter += 1
                        
                        if counter > 1:
                            self._log(f"WARNING: Filename conflict resolved, renamed to: {dest_file.name}")
                        
                        shutil.copy2(json_file, dest_file)
                        self._log(f"Copied: {json_file.name} from {subdir_name}")
                        copied_files += 1
                        
                    except Exception as e:
                        self._log(f"ERROR: Failed to copy {json_file.name} from {subdir_name}: {e}")
        
        except Exception as e:
            self._log(f"ERROR: Could not process JSON parent directory: {e}")
            return
        
        self._log(f"Processed {subdirs_processed} subdirectories")
        self._log(f"JSON files copied: {copied_files}")
        
        # Compress JSON directory
        if copied_files > 0:
            self._compress_json_directory()
        else:
            self._log("No JSON files found to compress")
    
    def _compress_json_directory(self):
        """Compress the JSON files directory using gzip (tar.gz format)."""
        self._log("Compressing JSON files...")
        
        try:
            # Create tar.gz archive
            archive_path = self.output_dir / "fastp_json.tar.gz"
            
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(self.json_output_dir, arcname='fastp_json')
            
            self._log(f"JSON files compressed to: {archive_path}")
            
            # Remove the uncompressed directory
            shutil.rmtree(self.json_output_dir)
            self._log("Removed uncompressed JSON directory")
            
        except Exception as e:
            self._log(f"ERROR: Failed to compress JSON files: {e}")
    
    # ============================================================================
    # STATS CSV FUNCTIONALITY
    # ============================================================================
    
    def _load_stats_csv(self) -> List[Dict]:
        """Load stats CSV file for matching with sequences."""
        self._log(f"Loading stats CSV: {self.stats_csv}")
        
        if not self.stats_csv.exists():
            self._log(f"ERROR: Stats CSV not found: {self.stats_csv}")
            return []
        
        stats_rows = []
        try:
            with open(self.stats_csv, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                stats_rows = list(reader)
                self._log(f"Loaded {len(stats_rows)} rows from stats CSV")
        except Exception as e:
            self._log(f"ERROR: Could not read stats CSV: {e}")
        
        return stats_rows
    
    def _merge_stats_data(self, best_rows: List[Dict]) -> List[Dict]:
        """Merge statistics data with best sequence rows."""
        self._log("Merging statistics data with sequences...")
        return self._add_stats_to_sequences(best_rows)
    
    def _add_stats_to_sequences(self, best_rows: List[Dict]) -> List[Dict]:
        """Add statistics data to sequence rows based on search criteria."""
        self._log("Adding statistics data to sequences...")
        
        enhanced_rows = []
        
        for row in best_rows:
            enhanced_row = self._process_sequence_stats(row)
            enhanced_rows.append(enhanced_row)
        
        return enhanced_rows
    
    def _process_sequence_stats(self, row: Dict) -> Dict:
        """Process statistics for a single sequence row."""
        seq_id = row['seq_id']
        
        # Extract search parameters
        search_id = '_'.join(seq_id.split('_')[:5])
        is_merge = '_merge' in seq_id
        is_fcleaner = '_fcleaner' in seq_id
        
        # Find matching stats
        matching_stats = self._find_matching_stats(search_id, is_merge)
        
        # Create enhanced row
        enhanced_row = row.copy()
        
        if matching_stats:
            self._add_stats_columns(enhanced_row, matching_stats, is_fcleaner, seq_id, search_id, is_merge)
        else:
            self._add_empty_stats_columns(enhanced_row, is_fcleaner, seq_id, search_id, is_merge)
        
        return enhanced_row
    
    def _add_stats_columns(self, enhanced_row: Dict, matching_stats: Dict, is_fcleaner: bool, seq_id: str, search_id: str, is_merge: bool):
        """Add statistics columns to a row when match is found."""
        columns_to_extract = self._get_stats_columns(is_fcleaner)
        
        for col in columns_to_extract:
            if col in matching_stats:
                enhanced_row[col] = matching_stats[col]
            else:
                enhanced_row[col] = ''
                self._log(f"WARNING: Column '{col}' not found in stats data for {seq_id}")
        
        self._log(f"Added stats for {seq_id} (search_id: {search_id}, mode: {'merge' if is_merge else 'concat'}, fcleaner: {is_fcleaner})")
    
    def _add_empty_stats_columns(self, enhanced_row: Dict, is_fcleaner: bool, seq_id: str, search_id: str, is_merge: bool):
        """Add empty statistics columns when no match is found."""
        columns_to_add = self._get_stats_columns(is_fcleaner)
        for col in columns_to_add:
            enhanced_row[col] = ''
        self._log(f"WARNING: No matching stats found for {seq_id} (search_id: {search_id}, mode: {'merge' if is_merge else 'concat'})")
    
    def _get_stats_columns(self, is_fcleaner: bool) -> List[str]:
        """Get the appropriate statistics columns based on fcleaner status."""
        if is_fcleaner:
            return [
                'cleaning_input_reads', 'cleaning_kept_reads', 'cleaning_removed_human',
                'cleaning_removed_at', 'cleaning_removed_outlier', 'cleaning_ambig_bases',
                'cleaning_cov_percent', 'cleaning_cov_avg', 'cleaning_cov_max', 'cleaning_cov_min'
            ]
        else:
            return [
                'mge_params', 'n_reads_in', 'n_reads_aligned', 'n_reads_skipped',
                'ref_length', 'cov_min', 'cov_max', 'cov_avg', 'cov_med'
            ]
    
    def _find_matching_stats(self, search_id: str, is_merge: bool) -> Optional[Dict]:
        """Find matching row in stats data based on search_id and mode."""
        matching_rows = []
        
        for stats_row in self.stats_data:
            filename = stats_row.get('Filename', '')
            mge_params = stats_row.get('mge_params', '')
            
            # Check if search_id is in filename
            if search_id not in filename:
                continue
            
            # Check mode matching
            if is_merge:
                # For merge mode, look for 'merge' in mge_params
                if 'merge' not in mge_params:
                    continue
            else:
                # For concat mode, mge_params should NOT contain 'merge'
                if 'merge' in mge_params:
                    continue
            
            matching_rows.append(stats_row)
        
        if len(matching_rows) == 0:
            return None
        elif len(matching_rows) == 1:
            return matching_rows[0]
        else:
            # Multiple matches - log warning and return first
            filenames = [row.get('Filename', 'unknown') for row in matching_rows]
            self._log(f"WARNING: Multiple stats matches for search_id '{search_id}' (merge: {is_merge}): {filenames[:3]}{'...' if len(filenames) > 3 else ''}")
            return matching_rows[0]


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Extract best sequences from CSV metadata and FASTA files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python extract_best_sequences.py -i sequences.csv -f best_sequences.fasta -o /path/to/output/dir
  python extract_best_sequences.py -i sequences.csv -f best_sequences.fasta -o /path/to/output/dir -a /home/user/MGE_outputs/YB-4227_snpseq01188_00232
  python extract_best_sequences.py -i sequences.csv -f best_sequences.fasta -o /path/to/output/dir -a /home/user/MGE_outputs/YB-4227_snpseq01188_00232 -c stats.csv
  python extract_best_sequences.py -i sequences.csv -f best_sequences.fasta -o /path/to/output/dir -j /path/to/json/parent/dir
  python extract_best_sequences.py -i sequences.csv -f best_sequences.fasta -o /path/to/output/dir -a /home/user/MGE_outputs/YB-4227_snpseq01188_00232 -c stats.csv -j /path/to/json/parent/dir
        """
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Input fasta_compare CSV file with sequence metadata'
    )
    
    parser.add_argument(
        '-f', '--fasta',
        required=True,
        help='Output FASTA filename (will be created in output directory)'
    )
    
    parser.add_argument(
        '-o', '--out',
        required=True,
        help='Output directory for all generated files'
    )
    
    parser.add_argument(
        '-a', '--align',
        help='Parent directory containing *_mode/alignment subdirectories (optional)'
    )
    
    parser.add_argument(
        '-c', '--csv',
        help='Stats CSV file to merge with sequence metadata (optional)'
    )
    
    parser.add_argument(
        '-j', '--json',
        help='Parent directory containing subdirectories with JSON files (optional)'
    )
    
    args = parser.parse_args()
    
    # Initialize extractor and run
    extractor = SequenceExtractor(args.input, args.out, args.fasta, args.align, args.csv, args.json)
    success = extractor.extract_best_sequences()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
