#!/usr/bin/env python3
"""
CSV Encoding Debugging Tool

This script helps diagnose encoding issues in CSV files by:
1. Attempting to read the file with different encodings
2. Identifying problematic characters and their positions
3. Displaying the first few rows of the CSV file when successfully read
"""

import pandas as pd
import sys
import os
import argparse
import unicodedata

# Common single-byte encodings to check for character interpretation
ENCODINGS_TO_CHECK = ['utf-8', 'latin-1', 'cp1252', 'ISO-8859-1', 'windows-1252']


def get_line_and_column(content: bytes, position: int) -> tuple[int, int, str]:
    """
    Given a byte position, return the line number, column number, and the full line content.
    """
    lines = content[:position].split(b'\n')
    line_number = len(lines)
    column_number = len(lines[-1]) + 1 if lines else 1
    
    # Get the full line containing this position
    all_lines = content.split(b'\n')
    if line_number <= len(all_lines):
        try:
            full_line = all_lines[line_number - 1].decode('utf-8', errors='replace')
        except Exception:
            full_line = repr(all_lines[line_number - 1])
    else:
        full_line = "<unable to retrieve line>"
    
    return line_number, column_number, full_line


def interpret_byte_as_char(byte_val: int) -> dict:
    """
    Attempt to interpret a single byte value as a character in various encodings.
    Returns a dict with encoding names as keys and interpreted characters as values.
    """
    interpretations = {}
    byte_data = bytes([byte_val])
    
    for encoding in ['latin-1', 'cp1252', 'ISO-8859-1']:
        try:
            char = byte_data.decode(encoding)
            char_name = unicodedata.name(char, 'UNKNOWN')
            interpretations[encoding] = {
                'char': char,
                'name': char_name,
                'unicode': f'U+{ord(char):04X}'
            }
        except Exception as e:
            interpretations[encoding] = {'error': str(e)}
    
    return interpretations


def check_utf8_validity(content: bytes, position: int) -> dict:
    """
    Check if the byte at the given position is part of a valid or invalid UTF-8 sequence.
    Returns detailed information about the UTF-8 status.
    """
    byte_val = content[position]
    result = {
        'byte_value': byte_val,
        'hex': f'0x{byte_val:02X}',
        'binary': f'{byte_val:08b}',
        'is_valid_utf8': False,
        'utf8_role': None,
        'sequence_info': None
    }
    
    # Determine what role this byte plays in UTF-8
    if byte_val <= 0x7F:
        result['utf8_role'] = 'ASCII (single-byte character)'
        result['is_valid_utf8'] = True
        result['sequence_info'] = f"ASCII character: '{chr(byte_val)}'"
    elif 0x80 <= byte_val <= 0xBF:
        result['utf8_role'] = 'UTF-8 continuation byte'
        # Check if it's part of a valid sequence by looking backwards
        start = position
        while start > 0 and 0x80 <= content[start - 1] <= 0xBF:
            start -= 1
        if start > 0:
            start -= 1  # Include the potential lead byte
        
        # Try to decode the sequence
        for end in range(position + 1, min(position + 4, len(content)) + 1):
            try:
                seq = content[start:end]
                decoded = seq.decode('utf-8')
                result['is_valid_utf8'] = True
                result['sequence_info'] = f"Part of valid UTF-8 sequence: {seq.hex(' ')} -> '{decoded}'"
                break
            except UnicodeDecodeError:
                continue
        
        if not result['is_valid_utf8']:
            result['sequence_info'] = "Orphaned continuation byte (invalid UTF-8)"
            
    elif 0xC0 <= byte_val <= 0xDF:
        result['utf8_role'] = 'UTF-8 lead byte (2-byte sequence)'
        try:
            seq = content[position:position + 2]
            decoded = seq.decode('utf-8')
            result['is_valid_utf8'] = True
            result['sequence_info'] = f"Valid 2-byte sequence: {seq.hex(' ')} -> '{decoded}' ({unicodedata.name(decoded, 'UNKNOWN')})"
        except (UnicodeDecodeError, IndexError) as e:
            result['sequence_info'] = f"Invalid 2-byte sequence: {str(e)}"
            
    elif 0xE0 <= byte_val <= 0xEF:
        result['utf8_role'] = 'UTF-8 lead byte (3-byte sequence)'
        try:
            seq = content[position:position + 3]
            decoded = seq.decode('utf-8')
            result['is_valid_utf8'] = True
            result['sequence_info'] = f"Valid 3-byte sequence: {seq.hex(' ')} -> '{decoded}' ({unicodedata.name(decoded, 'UNKNOWN')})"
        except (UnicodeDecodeError, IndexError) as e:
            result['sequence_info'] = f"Invalid 3-byte sequence: {str(e)}"
            
    elif 0xF0 <= byte_val <= 0xF7:
        result['utf8_role'] = 'UTF-8 lead byte (4-byte sequence)'
        try:
            seq = content[position:position + 4]
            decoded = seq.decode('utf-8')
            result['is_valid_utf8'] = True
            result['sequence_info'] = f"Valid 4-byte sequence: {seq.hex(' ')} -> '{decoded}' ({unicodedata.name(decoded, 'UNKNOWN')})"
        except (UnicodeDecodeError, IndexError) as e:
            result['sequence_info'] = f"Invalid 4-byte sequence: {str(e)}"
    else:
        result['utf8_role'] = 'Invalid UTF-8 lead byte'
        result['sequence_info'] = f"Byte 0x{byte_val:02X} is never valid in UTF-8"
    
    return result


def format_context_display(content: bytes, position: int, context_size: int = 30) -> str:
    """
    Create a nicely formatted context display around a position.
    """
    start = max(0, position - context_size)
    end = min(len(content), position + context_size)
    
    before = content[start:position]
    problem_byte = content[position:position + 1]
    after = content[position + 1:end]
    
    def safe_decode(b: bytes) -> str:
        result = ""
        for byte in b:
            if 32 <= byte <= 126:
                result += chr(byte)
            elif byte == 10:
                result += "\\n"
            elif byte == 13:
                result += "\\r"
            elif byte == 9:
                result += "\\t"
            else:
                result += f"<{byte:02X}>"
        return result
    
    before_str = safe_decode(before)
    after_str = safe_decode(after)
    
    return f"{before_str}>>>[0x{problem_byte[0]:02X}]<<<{after_str}"


def find_problematic_bytes(file_path: str, max_problems: int = 20, show_valid_utf8: bool = False):
    """
    Find and report problematic bytes in a file that might cause encoding issues.
    
    Args:
        file_path: Path to the file to analyze
        max_problems: Maximum number of problems to report
        show_valid_utf8: If True, also report valid UTF-8 multi-byte sequences
    """
    print(f"\n{'='*80}")
    print(f"BYTE-LEVEL ANALYSIS: {file_path}")
    print(f"{'='*80}")
    
    with open(file_path, 'rb') as f:
        content = f.read()
    
    print(f"File size: {len(content):,} bytes")
    
    problem_count = 0
    valid_utf8_count = 0
    invalid_utf8_count = 0
    positions_reported = set()  # Avoid reporting the same multi-byte sequence multiple times
    
    i = 0
    while i < len(content):
        byte_val = content[i]
        
        if byte_val > 0x7F:  # Non-ASCII byte
            if i in positions_reported:
                i += 1
                continue
                
            utf8_info = check_utf8_validity(content, i)
            
            if utf8_info['is_valid_utf8'] and not show_valid_utf8:
                valid_utf8_count += 1
                # Skip the rest of the multi-byte sequence
                if 0xC0 <= byte_val <= 0xDF:
                    i += 2
                elif 0xE0 <= byte_val <= 0xEF:
                    i += 3
                elif 0xF0 <= byte_val <= 0xF7:
                    i += 4
                else:
                    i += 1
                continue
            
            if not utf8_info['is_valid_utf8']:
                invalid_utf8_count += 1
            
            problem_count += 1
            
            if problem_count <= max_problems:
                line_num, col_num, full_line = get_line_and_column(content, i)
                
                print(f"\n{'-'*80}")
                print(f"ISSUE #{problem_count}")
                print(f"{'-'*80}")
                print(f"  Location:        Byte position {i:,} (Line {line_num}, Column {col_num})")
                print(f"  Byte value:      {utf8_info['hex']} (decimal: {byte_val}, binary: {utf8_info['binary']})")
                print(f"  UTF-8 role:      {utf8_info['utf8_role']}")
                print(f"  UTF-8 validity:  {'✓ VALID' if utf8_info['is_valid_utf8'] else '✗ INVALID'}")
                print(f"  Sequence info:   {utf8_info['sequence_info']}")
                
                # Show interpretations in other encodings
                if not utf8_info['is_valid_utf8']:
                    print(f"\n  Possible interpretations in other encodings:")
                    interpretations = interpret_byte_as_char(byte_val)
                    for enc, info in interpretations.items():
                        if 'char' in info:
                            print(f"    {enc:15} -> '{info['char']}' ({info['name']}, {info['unicode']})")
                        else:
                            print(f"    {enc:15} -> Error: {info.get('error', 'Unknown')}")
                
                print(f"\n  Context (surrounding bytes):")
                print(f"    {format_context_display(content, i)}")
                
                print(f"\n  Full line (with replacements for unprintable chars):")
                print(f"    {full_line[:200]}{'...' if len(full_line) > 200 else ''}")
                
                positions_reported.add(i)
        
        i += 1
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    print(f"  Total non-ASCII bytes found:     {problem_count + valid_utf8_count}")
    print(f"  Valid UTF-8 multi-byte chars:    {valid_utf8_count}")
    print(f"  Invalid/problematic bytes:       {invalid_utf8_count}")
    
    if problem_count > max_problems:
        print(f"\n  Note: Only showing first {max_problems} issues. {problem_count - max_problems} more issues not displayed.")
    
    if invalid_utf8_count == 0 and valid_utf8_count > 0:
        print(f"\n  ✓ File appears to be valid UTF-8 with {valid_utf8_count} multi-byte character(s).")
    elif invalid_utf8_count == 0 and valid_utf8_count == 0:
        print(f"\n  ✓ File contains only ASCII characters.")
    else:
        print(f"\n  ✗ File contains {invalid_utf8_count} byte(s) that are not valid UTF-8.")
        print(f"    This file is likely encoded in Latin-1, Windows-1252, or another single-byte encoding.")
    
    return invalid_utf8_count


def try_read_csv(file_path: str):
    """Try to read a CSV file with different encodings and report results."""
    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'cp1252', 'ISO-8859-1', 'ascii', 'windows-1252']
    
    print(f"\n{'='*80}")
    print(f"CSV PARSING ATTEMPTS")
    print(f"{'='*80}")
    
    successful_encodings = []
    
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            successful_encodings.append(encoding)
            print(f"  ✓ {encoding:15} - Successfully read ({df.shape[0]} rows, {df.shape[1]} columns)")
        except UnicodeDecodeError as e:
            print(f"  ✗ {encoding:15} - Failed: {str(e)[:60]}...")
        except Exception as e:
            print(f"  ✗ {encoding:15} - Error: {str(e)[:60]}...")
    
    if successful_encodings:
        # Use the first successful encoding to show preview
        preferred_encoding = successful_encodings[0]
        df = pd.read_csv(file_path, encoding=preferred_encoding)
        
        print(f"\n{'='*80}")
        print(f"FILE PREVIEW (using {preferred_encoding})")
        print(f"{'='*80}")
        print(f"\nColumn names ({len(df.columns)} columns):")
        for i, col in enumerate(df.columns):
            print(f"  [{i}] {col}")
        
        print(f"\nFirst 5 rows:")
        print(df.head(5).to_string(max_colwidth=50))
        
        print(f"\nData shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        
        return df, preferred_encoding
    
    print(f"\n✗ Failed to read the file with any of the attempted encodings.")
    return None, None


def main():
    parser = argparse.ArgumentParser(
        description='Debug encoding issues in CSV files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s data.csv                    # Analyze data.csv
  %(prog)s data.csv --max-issues 50    # Show up to 50 issues
  %(prog)s data.csv --show-valid-utf8  # Also report valid UTF-8 multi-byte chars
        """
    )
    parser.add_argument('file', help='Path to the CSV file to analyze')
    parser.add_argument('--max-issues', '-m', type=int, default=20,
                        help='Maximum number of issues to display (default: 20)')
    parser.add_argument('--show-valid-utf8', '-v', action='store_true',
                        help='Also report valid UTF-8 multi-byte sequences')
    
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    
    args = parser.parse_args()
    
    if not os.path.exists(args.file):
        print(f"Error: The file '{args.file}' does not exist.")
        sys.exit(1)
    
    print(f"\n{'#'*80}")
    print(f"# CSV ENCODING DIAGNOSTIC TOOL")
    print(f"# File: {args.file}")
    print(f"{'#'*80}")
    
    # First try to identify problematic bytes
    invalid_count = find_problematic_bytes(
        args.file, 
        max_problems=args.max_issues,
        show_valid_utf8=args.show_valid_utf8
    )
    
    # Then try to read with different encodings
    df, successful_encoding = try_read_csv(args.file)
    
    # Final recommendation
    print(f"\n{'='*80}")
    print(f"RECOMMENDATION")
    print(f"{'='*80}")
    
    if df is not None:
        if invalid_count == 0:
            print(f"  The file is valid UTF-8. Use encoding='utf-8' when reading.")
        else:
            print(f"  The file can be read with encoding='{successful_encoding}'")
            print(f"  ")
            print(f"  To use in pandas:")
            print(f"    df = pd.read_csv('{args.file}', encoding='{successful_encoding}')")
            print(f"  ")
            print(f"  To convert to UTF-8 (recommended for long-term compatibility):")
            print(f"    iconv -f {successful_encoding.upper()} -t UTF-8 '{args.file}' > '{args.file}.utf8.csv'")
    else:
        print(f"  The file could not be read with any common encoding.")
        print(f"  It may have severe corruption or use an uncommon encoding.")
        print(f"  Try using the 'file' command to detect encoding:")
        print(f"    file -i '{args.file}'")


if __name__ == "__main__":
    main()
