#!/usr/bin/env python3
"""MS Forms Extension Request Processor.

Provides a small CLI utility that parses tab-delimited MS Forms exports,
deduplicates submissions, snaps due dates to Sundays, and writes per-assignment
CSVs plus a human-readable summary.
"""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
from collections import defaultdict
import io
import os
from pathlib import Path
import re
from typing import Iterable, List, Tuple, Optional, Dict, Any

def parse_date(date_str):
    """Parse date in MM/DD/YYYY format"""
    try:
        return datetime.strptime(date_str.strip(), '%m/%d/%Y')
    except ValueError:
        return None

def get_next_sunday(date):
    """If date is not Sunday, move to next Sunday. If already Sunday, keep it."""
    # weekday() returns 0-6 (Monday-Sunday), so Sunday is 6
    days_until_sunday = (6 - date.weekday()) % 7
    if days_until_sunday == 0:
        return date  # Already Sunday
    return date + timedelta(days=days_until_sunday)

def format_date(date):
    """Format date to MM/DD/YYYY"""
    return date.strftime('%m/%d/%Y')

def get_day_name(date):
    """Get day of week name"""
    return date.strftime('%A')

def detect_delimiter(lines: List[str]) -> str:
    """Attempt to detect whether the payload is comma- or tab-delimited."""

    if not lines:
        return ','

    sample = '\n'.join(lines[:10])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=',\t')
        return dialect.delimiter
    except csv.Error:
        first_line = lines[0]
        if first_line.count(',') >= first_line.count('\t'):
            return ','
        return '\t'


def process_extension_data(
    data_lines: Iterable[str],
) -> Tuple[List[dict], List[dict], Optional[Dict[str, Any]]]:
    """Process MS Forms extension request data.

    Returns a tuple of successfully parsed records, error dictionaries, and
    metadata needed for producing processed copies.
    """

    records: List[dict] = []
    errors: List[dict] = []
    all_assignments: set = set()

    lines = list(data_lines)

    if not lines:
        return [], errors, None

    delimiter = detect_delimiter(lines)
    reader = csv.reader(lines, delimiter=delimiter)

    try:
        raw_header = next(reader)
    except StopIteration:
        return [], [{'message': 'No header row found', 'row': None}], None

    header = [col.strip().lstrip('\ufeff') for col in raw_header]

    # Map column names to indices
    col_map = {}
    for i, col in enumerate(header):
        col_map[col] = i
    
    # Required columns (exact names from MS Forms export)
    email_col = 'Email'
    name_col = 'Name'
    assignment_col = 'Which assignment due date do you want to change?'
    date_col = 'What would you like to new date to be change too?'
    
    required_cols = [email_col, name_col, assignment_col, date_col]
    
    # Validate we have required columns
    missing_cols = [col for col in required_cols if col not in col_map]
    if missing_cols:
        errors.append(
            {
                'message': f"Missing required columns: {', '.join(missing_cols)}",
                'row': None,
            }
        )
        return [], errors, None
    
    table_data = {
        'header': raw_header,
        'rows': [],
        'col_map': col_map,
        'delimiter': delimiter,
        'all_assignments': None,
    }

    done_col = col_map.get('DONE?')

    # Process data rows
    for row_num, fields in enumerate(reader, start=2):
        if not any(field.strip() for field in fields):
            continue

        if len(fields) < len(raw_header):
            fields.extend([''] * (len(raw_header) - len(fields)))

        table_data['rows'].append({'row_num': row_num, 'fields': fields[:]})

        assignment = fields[col_map[assignment_col]].strip() if col_map[assignment_col] < len(fields) else ''
        if assignment:
            all_assignments.add(assignment)

        already_done = False
        if done_col is not None and done_col < len(fields):
            already_done = fields[done_col].strip() == '*'
        if already_done:
            continue

        try:
            email = fields[col_map[email_col]].strip() if col_map[email_col] < len(fields) else ''
            name = fields[col_map[name_col]].strip() if col_map[name_col] < len(fields) else ''
            assignment = fields[col_map[assignment_col]].strip() if col_map[assignment_col] < len(fields) else ''
            requested_date_str = fields[col_map[date_col]].strip() if col_map[date_col] < len(fields) else ''

            # Validate required fields
            missing = []
            if not email:
                missing.append('Email')
            if not name:
                missing.append('Name')
            if not assignment:
                missing.append('Assignment')
            if not requested_date_str:
                missing.append('RequestedDate')

            if missing:
                errors.append(
                    {
                        'message': f"Missing fields ({', '.join(missing)})",
                        'row': row_num,
                        'line': '\t'.join(fields),
                    }
                )
                continue

            # Parse date
            requested_date = parse_date(requested_date_str)
            if not requested_date:
                errors.append(
                    {
                        'message': f"Invalid date format '{requested_date_str}' (expected MM/DD/YYYY)",
                        'row': row_num,
                        'line': '\t'.join(fields),
                    }
                )
                continue

            records.append({
                'email': email,
                'name': name,
                'assignment': assignment,
                'requested_date': requested_date,
                'row_num': row_num
            })

        except Exception as e:
            errors.append({'message': str(e), 'row': row_num, 'line': '\t'.join(fields)})
            continue

    table_data['all_assignments'] = sorted(all_assignments)
    return records, errors, table_data

def deduplicate_records(records):
    """Keep only the latest date for each (Assignment, Email) combination"""
    dedup = {}
    
    for record in records:
        key = (record['assignment'], record['email'])
        
        if key not in dedup:
            dedup[key] = record
        else:
            # Keep the one with latest date
            if record['requested_date'] > dedup[key]['requested_date']:
                dedup[key] = record
    
    return list(dedup.values())

def adjust_dates(records):
    """Adjust dates to Sunday"""
    for record in records:
        adjusted = get_next_sunday(record['requested_date'])
        record['original_date'] = record['requested_date']
        record['due_date'] = adjusted
    return records

def sanitize_filename(text):
    """Convert text to valid filename"""
    filename = text.lower()
    filename = re.sub(r'[^a-z0-9]+', '_', filename)
    filename = filename.strip('_')
    return filename

def create_output_files(records, output_dir='./extensions_output', all_assignments=None):
    """Create CSV files per assignment.

    Returns a tuple of ``file_info`` data and any I/O errors encountered.
    """

    io_errors: List[str] = []

    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        io_errors.append(f"Unable to create output directory '{output_dir}': {exc}")
        return [], io_errors
    
    # Group by assignment
    by_assignment = defaultdict(list)
    for record in records:
        by_assignment[record['assignment']].append(record)

    if all_assignments:
        for assignment in all_assignments:
            if assignment not in by_assignment:
                by_assignment[assignment] = []
    
    file_info = []
    
    for assignment, assignment_records in sorted(by_assignment.items()):
        # Create filename
        filename = sanitize_filename(assignment)
        filename = f"{filename}_extensions.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Sort by email
        assignment_records.sort(key=lambda x: x['email'])
        
        # Write CSV with BOM (UTF-8-sig) and trim trailing newline
        try:
            buffer = io.StringIO()
            writer = csv.writer(buffer, lineterminator='\n')
            writer.writerow(['Email', 'Name', 'Assignment', 'DueDate', 'RECORD'])

            for record in assignment_records:
                due_date_str = format_date(record['due_date'])
                record_str = f"{record['email']} - {record['name']} - {record['assignment']} - {due_date_str}"
                writer.writerow([
                    record['email'],
                    record['name'],
                    record['assignment'],
                    due_date_str,
                    record_str,
                ])

            contents = buffer.getvalue().rstrip('\n')
            with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
                f.write(contents)
        except OSError as exc:
            io_errors.append(f"Failed to write '{filepath}': {exc}")
            continue
        
        # Collect info for summary
        if assignment_records:
            dates = [record['due_date'] for record in assignment_records]
            file_info.append({
                'assignment': assignment,
                'filename': filename,
                'num_students': len(assignment_records),
                'earliest_date': format_date(min(dates)),
                'latest_date': format_date(max(dates))
            })
        else:
            file_info.append({
                'assignment': assignment,
                'filename': filename,
                'num_students': 0,
                'earliest_date': 'N/A',
                'latest_date': 'N/A'
            })
    
    return file_info, io_errors


def write_processed_copy(input_path: Optional[str], table_data: Optional[Dict[str, Any]], processed_rows: Iterable[int]):
    """Write a copy of the original input marking processed rows with a ``*``."""

    if not input_path or not table_data:
        return None, 'No input file or parsed table data available'

    done_col = table_data['col_map'].get('DONE?') if table_data.get('col_map') else None
    if done_col is None:
        return None, "Input file does not include a 'DONE?' column"

    processed_path = Path(input_path)
    output_path = processed_path.with_name(f"{processed_path.stem}_PROCESSED{processed_path.suffix}")

    delimiter = table_data.get('delimiter', ',')
    processed_set = set(processed_rows)

    try:
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=delimiter, lineterminator='\n')
        writer.writerow(table_data['header'])

        for row in table_data['rows']:
            fields = row['fields'][:]
            if row['row_num'] in processed_set:
                if done_col >= len(fields):
                    fields.extend([''] * (done_col - len(fields) + 1))
                fields[done_col] = '*'
            writer.writerow(fields)

        contents = buffer.getvalue().rstrip('\n')
        with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
            f.write(contents)
    except OSError as exc:
        return None, str(exc)

    return str(output_path), None

def generate_summary(records, file_info, errors, output_dir, io_errors=None, failures_path=None):
    """Generate summary report"""
    
    summary_lines = []
    summary_lines.append("\n" + "="*70)
    summary_lines.append("EXTENSION REQUEST PROCESSING SUMMARY")
    summary_lines.append("="*70)
    summary_lines.append(f"\nTotal Assignments: {len(file_info)}")
    summary_lines.append(f"Total Students: {len(records)}")
    summary_lines.append(f"\nOutput Directory: {os.path.abspath(output_dir)}")
    if file_info:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append("PER-ASSIGNMENT BREAKDOWN")
        summary_lines.append("-"*70)

        for info in file_info:
            summary_lines.append(f"\n{info['assignment']}")
            summary_lines.append(f"  File: {info['filename']}")
            summary_lines.append(f"  Students: {info['num_students']}")
            if info['num_students'] > 0:
                summary_lines.append(f"  Date Range: {info['earliest_date']} to {info['latest_date']}")
            else:
                summary_lines.append(f"  Date Range: N/A (no extensions)")

    if failures_path:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append("Rejected rows written to:")
        summary_lines.append(f"  {failures_path}")

    if io_errors:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append("FILE I/O ISSUES")
        summary_lines.append("-"*70)
        for issue in io_errors:
            summary_lines.append(f"  • {issue}")

    if errors:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append(f"PARSING ERRORS ({len(errors)} found)")
        summary_lines.append("-"*70)
        for error in errors:
            row_info = f"Row {error['row']}: " if error.get('row') else ''
            summary_lines.append(f"  • {row_info}{error['message']}")
    else:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append("✓ No errors detected")
        summary_lines.append("-"*70)
    
    summary_lines.append("\n" + "="*70)
    
    # Print and save summary
    summary_text = '\n'.join(summary_lines)
    print(summary_text)
    
    # Save summary to file
    summary_file = os.path.join(output_dir, 'SUMMARY.txt')
    with open(summary_file, 'w', encoding='utf-8') as f:
        f.write(summary_text)

    print(f"\nSummary saved to: {summary_file}")


def write_failure_report(errors, output_dir):
    """Write rejected rows (if any) to a CSV file."""

    if not errors:
        return None

    failures_path = os.path.join(output_dir, 'failures.csv')
    try:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator='\n')
        writer.writerow(['Row', 'Message', 'Line'])
        for error in errors:
            writer.writerow([
                error.get('row') or '',
                error['message'],
                error.get('line', ''),
            ])

        contents = buffer.getvalue().rstrip('\n')
        with open(failures_path, 'w', newline='', encoding='utf-8') as f:
            f.write(contents)
        return failures_path
    except OSError as exc:
        print(f"Unable to write failure report: {exc}")
        return None

def read_from_file(filename):
    """Read data from a file"""

    candidates = []

    primary = Path(filename).expanduser()
    candidates.append(primary)

    # If the file was provided as a relative path, also try resolving it
    # relative to the script's directory. This helps when the CLI is invoked
    # from another working directory (e.g., via shortcuts).
    if not primary.is_absolute():
        candidates.append(Path(__file__).parent / filename)

    tried_paths = []

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in tried_paths:
            continue
        tried_paths.append(resolved)

        if resolved.exists():
            try:
                with open(resolved, 'r', encoding='utf-8') as f:
                    return [line.rstrip('\n') for line in f.readlines()]
            except Exception as e:
                print(f"Error reading file '{resolved}': {e}")
                return None

    tried = ', '.join(str(path) for path in tried_paths)
    print(
        f"Error: File '{filename}' not found (searched: {tried}). Current working "
        f"directory: {Path.cwd()}"
    )
    return None

def read_from_clipboard():
    """Try to read from clipboard (requires pyperclip)"""
    try:
        import pyperclip
        data = pyperclip.paste()
        return data.split('\n')
    except ImportError:
        return None

def read_from_stdin():
    """Read from stdin (paste directly)"""
    print("Paste your MS Forms data (tab-separated, with headers).")
    print("Press Ctrl+D (Unix/Mac) or Ctrl+Z + Enter (Windows) when done:\n")
    
    lines = []
    try:
        while True:
            line = input()
            lines.append(line)
    except EOFError:
        pass
    
    return lines


def parse_arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        '--input-file',
        help='Path to a tab-delimited MS Forms export to process.',
    )
    parser.add_argument(
        '--clipboard',
        action='store_true',
        help='Read the data from the clipboard (requires pyperclip).',
    )
    parser.add_argument(
        '--output-dir',
        default='./extensions_output',
        help='Directory where CSVs and summary should be written.',
    )
    parser.add_argument(
        '--no-adjust',
        action='store_true',
        help='Skip moving requested dates to the following Sunday.',
    )
    return parser.parse_args(argv)

def main(argv=None):
    args = parse_arguments(argv)

    print("="*70)
    print("MS Forms Extension Request Processor")
    print("="*70)

    lines = None

    if args.input_file:
        lines = read_from_file(args.input_file)
    elif args.clipboard:
        lines = read_from_clipboard()
        if lines is None:
            print("Clipboard support is unavailable (pyperclip not installed).")
    else:
        # Fallback to the interactive workflow when no arguments are supplied.
        print("\nHow would you like to provide data?")
        print("1. Paste directly (type/paste into terminal)")
        print("2. Read from file")

        choice = input("\nSelect option (1 or 2): ").strip()

        if choice == '1':
            lines = read_from_stdin()
        elif choice == '2':
            filename = input("\nEnter filename (e.g., extension_requests.txt): ").strip()
            lines = read_from_file(filename)
        else:
            print("Invalid option")
            return

    if not lines or not any(line.strip() for line in lines):
        print("No data provided")
        return
    
    # Remove empty lines from start/end
    lines = [line for line in lines if line.strip()]
    
    # Process
    print("\nProcessing...")
    records, errors, table_data = process_extension_data(lines)

    if not records and errors:
        print("\nFailed to parse data:")
        for error in errors:
            print(f"  • {error}")
        return

    if not records and not errors:
        print("\nNo new extension requests to process (all rows already marked DONE?).")

    print(f"✓ Parsed {len(records)} records")
    
    # Deduplicate
    original_count = len(records)
    records = deduplicate_records(records)
    if len(records) < original_count:
        print(f"✓ After deduplication: {len(records)} records (removed {original_count - len(records)} duplicates)")
    else:
        print(f"✓ No duplicates found")
    
    # Adjust dates
    if not args.no_adjust:
        records = adjust_dates(records)
        adjusted_count = sum(1 for r in records if r['original_date'] != r['due_date'])
        print(f"✓ Adjusted {adjusted_count} dates to Sunday")
    else:
        for record in records:
            record['due_date'] = record['requested_date']
            record['original_date'] = record['requested_date']
        print("✓ Skipped Sunday adjustment (--no-adjust)")

    # Create output files
    output_dir = args.output_dir
    all_assignments = table_data.get('all_assignments') if table_data else None
    file_info, io_errors = create_output_files(records, output_dir, all_assignments=all_assignments)
    print(f"✓ Created {len(file_info)} CSV files")

    processed_copy_path = None
    processed_copy_error = None
    if args.input_file and table_data:
        processed_rows = {record['row_num'] for record in records}
        processed_copy_path, processed_copy_error = write_processed_copy(
            args.input_file,
            table_data,
            processed_rows,
        )
        if processed_copy_path:
            print(f"✓ Wrote processed input copy to {processed_copy_path}")
        elif processed_copy_error:
            print(f"⚠️ Unable to write processed input copy: {processed_copy_error}")

    failures_path = write_failure_report(errors, output_dir)
    if failures_path:
        print(f"✓ Wrote rejected rows to {failures_path}")

    # Summary
    generate_summary(
        records,
        file_info,
        errors,
        output_dir,
        io_errors=io_errors,
        failures_path=failures_path,
    )

if __name__ == '__main__':
    main()
