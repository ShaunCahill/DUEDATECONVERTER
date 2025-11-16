#!/usr/bin/env python3
"""
MS Forms Extension Request Processor
Processes student extension requests, deduplicates, adjusts to Sunday, and creates per-assignment CSV files.
"""

import csv
from datetime import datetime, timedelta
from collections import defaultdict
import os
from pathlib import Path
import re
import sys

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

def process_extension_data(data_lines):
    """Process MS Forms extension request data"""
    
    records = []
    errors = []
    
    if not data_lines:
        return [], errors
    
    # Parse header
    header = data_lines[0].split('\t')
    
    # Map column names to indices
    col_map = {}
    for i, col in enumerate(header):
        col_map[col.strip()] = i
    
    # Required columns (exact names from MS Forms export)
    email_col = 'Email'
    name_col = 'Name'
    assignment_col = 'Which assignment due date do you want to change?'
    date_col = 'What would you like to new date to be change too?'
    
    required_cols = [email_col, name_col, assignment_col, date_col]
    
    # Validate we have required columns
    missing_cols = [col for col in required_cols if col not in col_map]
    if missing_cols:
        errors.append(f"ERROR: Missing required columns: {', '.join(missing_cols)}")
        return [], errors
    
    # Process data rows
    for row_num, line in enumerate(data_lines[1:], start=2):
        if not line.strip():  # Skip empty lines
            continue
            
        fields = line.split('\t')
        
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
                errors.append(f"Row {row_num}: Missing fields ({', '.join(missing)})")
                continue
            
            # Parse date
            requested_date = parse_date(requested_date_str)
            if not requested_date:
                errors.append(f"Row {row_num}: Invalid date format '{requested_date_str}' (expected MM/DD/YYYY)")
                continue
            
            records.append({
                'email': email,
                'name': name,
                'assignment': assignment,
                'requested_date': requested_date,
                'row_num': row_num
            })
        
        except Exception as e:
            errors.append(f"Row {row_num}: {str(e)}")
            continue
    
    return records, errors

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

def create_output_files(records, output_dir='./extensions_output'):
    """Create CSV files per assignment"""
    
    # Create output directory
    Path(output_dir).mkdir(exist_ok=True)
    
    # Group by assignment
    by_assignment = defaultdict(list)
    for record in records:
        by_assignment[record['assignment']].append(record)
    
    file_info = []
    
    for assignment, assignment_records in sorted(by_assignment.items()):
        # Create filename
        filename = sanitize_filename(assignment)
        filename = f"{filename}_extensions.csv"
        filepath = os.path.join(output_dir, filename)
        
        # Sort by email
        assignment_records.sort(key=lambda x: x['email'])
        
        # Write CSV with BOM (UTF-8-sig)
        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['Email', 'Name', 'Assignment', 'DueDate'])
            
            for record in assignment_records:
                writer.writerow([
                    record['email'],
                    record['name'],
                    record['assignment'],
                    format_date(record['due_date'])
                ])
        
        # Collect info for summary
        dates = [record['due_date'] for record in assignment_records]
        file_info.append({
            'assignment': assignment,
            'filename': filename,
            'num_students': len(assignment_records),
            'earliest_date': format_date(min(dates)),
            'latest_date': format_date(max(dates))
        })
    
    return file_info

def generate_summary(records, file_info, errors, output_dir):
    """Generate summary report"""
    
    summary_lines = []
    summary_lines.append("\n" + "="*70)
    summary_lines.append("EXTENSION REQUEST PROCESSING SUMMARY")
    summary_lines.append("="*70)
    summary_lines.append(f"\nTotal Assignments: {len(file_info)}")
    summary_lines.append(f"Total Students: {len(records)}")
    summary_lines.append(f"\nOutput Directory: {os.path.abspath(output_dir)}")
    summary_lines.append("\n" + "-"*70)
    summary_lines.append("PER-ASSIGNMENT BREAKDOWN")
    summary_lines.append("-"*70)
    
    for info in file_info:
        summary_lines.append(f"\n{info['assignment']}")
        summary_lines.append(f"  File: {info['filename']}")
        summary_lines.append(f"  Students: {info['num_students']}")
        summary_lines.append(f"  Date Range: {info['earliest_date']} to {info['latest_date']}")
    
    if errors:
        summary_lines.append("\n" + "-"*70)
        summary_lines.append(f"ERRORS ({len(errors)} found)")
        summary_lines.append("-"*70)
        for error in errors:
            summary_lines.append(f"  • {error}")
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
    with open(summary_file, 'w') as f:
        f.write(summary_text)
    
    print(f"\nSummary saved to: {summary_file}")

def read_from_file(filename):
    """Read data from a file"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return [line.rstrip('\n') for line in f.readlines()]
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found")
        return None
    except Exception as e:
        print(f"Error reading file: {e}")
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

def main():
    print("="*70)
    print("MS Forms Extension Request Processor")
    print("="*70)
    
    # Input method selection
    print("\nHow would you like to provide data?")
    print("1. Paste directly (type/paste into terminal)")
    print("2. Read from file")
    
    choice = input("\nSelect option (1 or 2): ").strip()
    
    lines = None
    
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
    records, errors = process_extension_data(lines)
    
    if not records:
        print("\nFailed to parse data:")
        for error in errors:
            print(f"  • {error}")
        return
    
    print(f"✓ Parsed {len(records)} records")
    
    # Deduplicate
    original_count = len(records)
    records = deduplicate_records(records)
    if len(records) < original_count:
        print(f"✓ After deduplication: {len(records)} records (removed {original_count - len(records)} duplicates)")
    else:
        print(f"✓ No duplicates found")
    
    # Adjust dates
    records = adjust_dates(records)
    adjusted_count = sum(1 for r in records if r['original_date'] != r['due_date'])
    print(f"✓ Adjusted {adjusted_count} dates to Sunday")
    
    # Create output files
    output_dir = './extensions_output'
    file_info = create_output_files(records, output_dir)
    print(f"✓ Created {len(file_info)} CSV files")
    
    # Summary
    generate_summary(records, file_info, errors, output_dir)

if __name__ == '__main__':
    main()
