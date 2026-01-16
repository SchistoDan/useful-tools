#!/usr/bin/env python3
"""
Find and copy paired-end FASTQ files based on string matching.

Searches recursively through directories for .fastq and .fastq.gz files
containing specified strings in their filenames, then copies them to an
output directory.
"""

import argparse
import shutil
import logging
from pathlib import Path
from collections import defaultdict

def setup_logging(output_dir: Path) -> logging.Logger:
    """Set up logging to both console and file."""
    logger = logging.getLogger('find_fastq_pairs')
    logger.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_format = logging.Formatter('%(levelname)s: %(message)s')
    console_handler.setFormatter(console_format)
    logger.addHandler(console_handler)
    
    # File handler
    log_file = output_dir / 'find_fastq_pairs.log'
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_format)
    logger.addHandler(file_handler)
    
    return logger


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Find and copy paired-end FASTQ files based on string matching.'
    )
    parser.add_argument(
        '--in', 
        dest='input_file',
        required=True,
        type=Path,
        help='Input text file with one search string per line'
    )
    parser.add_argument(
        '--search',
        required=True,
        type=Path,
        help='Directory to search recursively for FASTQ files'
    )
    parser.add_argument(
        '--out',
        required=True,
        type=Path,
        help='Output directory for copied files'
    )
    return parser.parse_args()


def read_search_strings(input_file: Path) -> list[str]:
    """Read search strings from input file, one per line."""
    strings = []
    with open(input_file, 'r') as f:
        for line in f:
            stripped = line.strip()
            if stripped:  # Skip empty lines
                strings.append(stripped)
    return strings


def find_fastq_files(search_dir: Path) -> list[Path]:
    """Recursively find all FASTQ files in directory."""
    fastq_files = []
    
    # Find .fastq files
    fastq_files.extend(search_dir.rglob('*.fastq'))
    
    # Find .fastq.gz files
    fastq_files.extend(search_dir.rglob('*.fastq.gz'))
    
    return fastq_files


def match_files_to_strings(
    fastq_files: list[Path], 
    search_strings: list[str]
) -> dict[str, list[Path]]:
    """Match FASTQ files to search strings (exact substring match)."""
    matches = defaultdict(list)
    
    for search_string in search_strings:
        for fastq_file in fastq_files:
            if search_string in fastq_file.name:
                matches[search_string].append(fastq_file)
    
    return matches


def check_pairs(
    matched_files: list[Path],
    search_string: str,
    logger: logging.Logger
) -> None:
    """Check if matched files form complete R1/R2 pairs and warn if not."""
    r1_files = [f for f in matched_files if '_R1_' in f.name or '_R1.' in f.name]
    r2_files = [f for f in matched_files if '_R2_' in f.name or '_R2.' in f.name]
    
    # Build sets of base names (everything before _R1_ or _R2_)
    def get_base(filepath: Path) -> str:
        name = filepath.name
        for pattern in ['_R1_', '_R1.', '_R2_', '_R2.']:
            if pattern in name:
                return name.split(pattern)[0]
        return name
    
    r1_bases = {get_base(f): f for f in r1_files}
    r2_bases = {get_base(f): f for f in r2_files}
    
    # Check for unpaired R1 files
    for base, filepath in r1_bases.items():
        if base not in r2_bases:
            logger.warning(f"Unpaired R1 file (no R2 found) for '{search_string}': {filepath.name}")
    
    # Check for unpaired R2 files
    for base, filepath in r2_bases.items():
        if base not in r1_bases:
            logger.warning(f"Unpaired R2 file (no R1 found) for '{search_string}': {filepath.name}")
    
    # Check for files that don't match R1/R2 pattern
    other_files = [f for f in matched_files if f not in r1_files and f not in r2_files]
    for filepath in other_files:
        logger.warning(f"File doesn't match R1/R2 pattern for '{search_string}': {filepath.name}")


def copy_with_rename(
    source: Path, 
    dest_dir: Path, 
    logger: logging.Logger
) -> Path:
    """Copy file to destination, renaming if necessary to avoid conflicts."""
    dest = dest_dir / source.name
    
    if not dest.exists():
        shutil.copy2(source, dest)
        return dest
    
    # Need to rename - handle .fastq.gz extension properly
    if source.name.endswith('.fastq.gz'):
        base = source.name[:-9]  # Remove .fastq.gz
        ext = '.fastq.gz'
    elif source.name.endswith('.fastq'):
        base = source.name[:-6]  # Remove .fastq
        ext = '.fastq'
    else:
        base = source.stem
        ext = source.suffix
    
    counter = 1
    while dest.exists():
        new_name = f"{base}_copy{counter}{ext}"
        dest = dest_dir / new_name
        counter += 1
    
    logger.warning(f"Filename conflict: {source.name} -> {dest.name}")
    shutil.copy2(source, dest)
    return dest


def main():
    args = parse_args()
    
    # Validate inputs
    if not args.input_file.exists():
        raise FileNotFoundError(f"Input file not found: {args.input_file}")
    
    if not args.search.exists():
        raise FileNotFoundError(f"Search directory not found: {args.search}")
    
    # Create output directory
    args.out.mkdir(parents=True, exist_ok=True)
    
    # Set up logging
    logger = setup_logging(args.out)
    
    logger.info(f"Input file: {args.input_file}")
    logger.info(f"Search directory: {args.search}")
    logger.info(f"Output directory: {args.out}")
    
    # Read search strings
    search_strings = read_search_strings(args.input_file)
    logger.info(f"Loaded {len(search_strings)} search strings")
    
    # Find all FASTQ files
    logger.info("Searching for FASTQ files...")
    fastq_files = find_fastq_files(args.search)
    logger.info(f"Found {len(fastq_files)} FASTQ files")
    
    # Match files to search strings
    matches = match_files_to_strings(fastq_files, search_strings)
    
    # Track statistics
    total_copied = 0
    strings_with_matches = 0
    strings_without_matches = []
    
    # Copy matched files
    for search_string in search_strings:
        matched_files = matches.get(search_string, [])
        
        if not matched_files:
            strings_without_matches.append(search_string)
            logger.warning(f"No matches found for: {search_string}")
            continue
        
        strings_with_matches += 1
        logger.info(f"String '{search_string}' matched {len(matched_files)} file(s)")
        
        # Check for proper R1/R2 pairing
        check_pairs(matched_files, search_string, logger)
        
        for source_file in matched_files:
            dest_file = copy_with_rename(source_file, args.out, logger)
            logger.debug(f"Copied: {source_file} -> {dest_file}")
            total_copied += 1
    
    # Summary
    logger.info("=" * 50)
    logger.info("SUMMARY")
    logger.info("=" * 50)
    logger.info(f"Search strings processed: {len(search_strings)}")
    logger.info(f"Strings with matches: {strings_with_matches}")
    logger.info(f"Strings without matches: {len(strings_without_matches)}")
    logger.info(f"Total files copied: {total_copied}")
    
    if strings_without_matches:
        logger.warning("The following strings had no matches:")
        for s in strings_without_matches:
            logger.warning(f"  - {s}")
    
    print(f"\nDone! Copied {total_copied} files to {args.out}")
    print(f"Log file: {args.out / 'find_fastq_pairs.log'}")


if __name__ == '__main__':
    main()
