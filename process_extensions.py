#!/usr/bin/env python3
"""MS Forms Extension Request Processor.

Provides a small CLI utility that parses tab-delimited MS Forms exports,
deduplicates submissions, snaps due dates to Sundays, and writes per-assignment
CSVs plus a human-readable summary.
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
from pathlib import Path
from typing import Iterable, List, Tuple, Optional, Dict, Any, Set

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

__all__ = [
    # Data classes
    "ExtensionRecord",
    "ParseError",
    "TableData",
    # Core functions
    "parse_date",
    "get_next_sunday",
    "format_date",
    "get_day_name",
    "detect_delimiter",
    "process_extension_data",
    "deduplicate_records",
    "adjust_dates",
    "sanitize_filename",
    # Output functions
    "create_output_files",
    "write_processed_copy",
    "generate_summary",
    "write_failure_report",
    # Input functions
    "read_from_file",
    "read_from_clipboard",
    "read_from_stdin",
    # CLI
    "parse_arguments",
    "main",
    # Configuration
    "ColumnConfig",
    "DEFAULT_COLUMNS",
]

# ---------------------------------------------------------------------------
# Logging Configuration
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Number of lines to sample for delimiter detection
DELIMITER_SAMPLE_SIZE = 10


@dataclass
class ColumnConfig:
    """Configuration for expected column names in the input data."""

    email: str = "Email"
    name: str = "Name"
    assignment: str = "Which assignment due date do you want to change?"
    date: str = "What would you like to new date to be change too?"
    done: str = "DONE?"

    @property
    def required(self) -> List[str]:
        """Return list of required column names."""
        return [self.email, self.name, self.assignment, self.date]


DEFAULT_COLUMNS = ColumnConfig()

# ---------------------------------------------------------------------------
# Data Classes
# ---------------------------------------------------------------------------


@dataclass
class ExtensionRecord:
    """Represents a single extension request record."""

    email: str
    name: str
    assignment: str
    requested_date: datetime
    row_num: int
    original_date: Optional[datetime] = None
    due_date: Optional[datetime] = None

    def with_adjusted_date(self, adjusted: datetime) -> ExtensionRecord:
        """Return a new record with the adjusted due date set."""
        return ExtensionRecord(
            email=self.email,
            name=self.name,
            assignment=self.assignment,
            requested_date=self.requested_date,
            row_num=self.row_num,
            original_date=self.requested_date,
            due_date=adjusted,
        )


@dataclass
class ParseError:
    """Represents an error encountered during parsing."""

    message: str
    row: Optional[int] = None
    line: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility."""
        return {
            "message": self.message,
            "row": self.row,
            "line": self.line,
        }


@dataclass
class TableData:
    """Metadata about the parsed table for producing processed copies."""

    header: List[str]
    rows: List[Dict[str, Any]] = field(default_factory=list)
    col_map: Dict[str, int] = field(default_factory=dict)
    delimiter: str = ","
    all_assignments: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Date Utilities
# ---------------------------------------------------------------------------


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date in MM/DD/YYYY format.

    Args:
        date_str: Date string to parse.

    Returns:
        Parsed datetime object, or None if parsing fails.
    """
    try:
        return datetime.strptime(date_str.strip(), "%m/%d/%Y")
    except ValueError:
        return None


def get_next_sunday(date: datetime) -> datetime:
    """If date is not Sunday, move to next Sunday. If already Sunday, keep it.

    Args:
        date: The input date.

    Returns:
        The same date if it's Sunday, otherwise the next Sunday.
    """
    # weekday() returns 0-6 (Monday-Sunday), so Sunday is 6
    days_until_sunday = (6 - date.weekday()) % 7
    if days_until_sunday == 0:
        return date  # Already Sunday
    return date + timedelta(days=days_until_sunday)


def format_date(date: datetime) -> str:
    """Format date to MM/DD/YYYY.

    Args:
        date: The date to format.

    Returns:
        Formatted date string.
    """
    return date.strftime("%m/%d/%Y")


def get_day_name(date: datetime) -> str:
    """Get day of week name.

    Args:
        date: The date to get the day name for.

    Returns:
        The day of week name (e.g., "Monday").
    """
    return date.strftime("%A")


# ---------------------------------------------------------------------------
# CSV Utilities
# ---------------------------------------------------------------------------


def detect_delimiter(lines: List[str]) -> str:
    """Attempt to detect whether the payload is comma- or tab-delimited.

    Args:
        lines: List of lines to analyze.

    Returns:
        Detected delimiter character (',' or '\\t').
    """
    if not lines:
        return ","

    sample = "\n".join(lines[:DELIMITER_SAMPLE_SIZE])
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",\t")
        return dialect.delimiter
    except csv.Error:
        first_line = lines[0]
        if first_line.count(",") >= first_line.count("\t"):
            return ","
        return "\t"


def _write_csv_content(
    writer: csv.writer,
    header: List[str],
    rows: Iterable[List[str]],
) -> str:
    """Write CSV content to a StringIO buffer and return trimmed content.

    Args:
        writer: CSV writer instance.
        header: Header row.
        rows: Data rows.

    Returns:
        CSV content as string with trailing newline removed.
    """
    writer.writerow(header)
    for row in rows:
        writer.writerow(row)


# ---------------------------------------------------------------------------
# Core Processing
# ---------------------------------------------------------------------------


def process_extension_data(
    data_lines: Iterable[str],
    columns: Optional[ColumnConfig] = None,
) -> Tuple[List[ExtensionRecord], List[ParseError], Optional[TableData]]:
    """Process MS Forms extension request data.

    Args:
        data_lines: Iterable of input lines to process.
        columns: Column configuration. Defaults to DEFAULT_COLUMNS.

    Returns:
        A tuple of (records, errors, table_data) where:
        - records: Successfully parsed ExtensionRecord objects
        - errors: List of ParseError objects for failed rows
        - table_data: TableData for producing processed copies, or None on failure
    """
    if columns is None:
        columns = DEFAULT_COLUMNS

    records: List[ExtensionRecord] = []
    errors: List[ParseError] = []
    all_assignments: Set[str] = set()

    lines = list(data_lines)

    if not lines:
        return [], errors, None

    delimiter = detect_delimiter(lines)
    reader = csv.reader(lines, delimiter=delimiter)

    try:
        raw_header = next(reader)
    except StopIteration:
        return [], [ParseError(message="No header row found")], None

    header = [col.strip().lstrip("\ufeff") for col in raw_header]

    # Map column names to indices
    col_map: Dict[str, int] = {}
    for i, col in enumerate(header):
        col_map[col] = i

    # Validate we have required columns
    missing_cols = [col for col in columns.required if col not in col_map]
    if missing_cols:
        errors.append(
            ParseError(message=f"Missing required columns: {', '.join(missing_cols)}")
        )
        return [], errors, None

    table_data = TableData(
        header=raw_header,
        col_map=col_map,
        delimiter=delimiter,
    )

    done_col = col_map.get(columns.done)

    # Process data rows
    for row_num, fields in enumerate(reader, start=2):
        if not any(field.strip() for field in fields):
            continue

        if len(fields) < len(raw_header):
            fields.extend([""] * (len(raw_header) - len(fields)))

        table_data.rows.append({"row_num": row_num, "fields": fields[:]})

        assignment = (
            fields[col_map[columns.assignment]].strip()
            if col_map[columns.assignment] < len(fields)
            else ""
        )
        if assignment:
            all_assignments.add(assignment)

        already_done = False
        if done_col is not None and done_col < len(fields):
            already_done = fields[done_col].strip() == "*"
        if already_done:
            continue

        email = (
            fields[col_map[columns.email]].strip()
            if col_map[columns.email] < len(fields)
            else ""
        )
        name = (
            fields[col_map[columns.name]].strip()
            if col_map[columns.name] < len(fields)
            else ""
        )
        assignment = (
            fields[col_map[columns.assignment]].strip()
            if col_map[columns.assignment] < len(fields)
            else ""
        )
        requested_date_str = (
            fields[col_map[columns.date]].strip()
            if col_map[columns.date] < len(fields)
            else ""
        )

        # Validate required fields
        missing: List[str] = []
        if not email:
            missing.append("Email")
        if not name:
            missing.append("Name")
        if not assignment:
            missing.append("Assignment")
        if not requested_date_str:
            missing.append("RequestedDate")

        if missing:
            errors.append(
                ParseError(
                    message=f"Missing fields ({', '.join(missing)})",
                    row=row_num,
                    line="\t".join(fields),
                )
            )
            continue

        # Parse date
        requested_date = parse_date(requested_date_str)
        if not requested_date:
            errors.append(
                ParseError(
                    message=f"Invalid date format '{requested_date_str}' (expected MM/DD/YYYY)",
                    row=row_num,
                    line="\t".join(fields),
                )
            )
            continue

        records.append(
            ExtensionRecord(
                email=email,
                name=name,
                assignment=assignment,
                requested_date=requested_date,
                row_num=row_num,
            )
        )

    table_data.all_assignments = sorted(all_assignments)
    return records, errors, table_data


def deduplicate_records(records: List[ExtensionRecord]) -> List[ExtensionRecord]:
    """Keep only the latest date for each (Assignment, Email) combination.

    Args:
        records: List of extension records.

    Returns:
        Deduplicated list of records (new list, does not mutate input).
    """
    dedup: Dict[Tuple[str, str], ExtensionRecord] = {}

    for record in records:
        key = (record.assignment, record.email)

        if key not in dedup:
            dedup[key] = record
        else:
            # Keep the one with latest date
            if record.requested_date > dedup[key].requested_date:
                dedup[key] = record

    return list(dedup.values())


def adjust_dates(records: List[ExtensionRecord]) -> List[ExtensionRecord]:
    """Adjust dates to Sunday, returning new records.

    Args:
        records: List of extension records.

    Returns:
        New list of records with adjusted dates (does not mutate input).
    """
    result: List[ExtensionRecord] = []
    for record in records:
        adjusted = get_next_sunday(record.requested_date)
        result.append(record.with_adjusted_date(adjusted))
    return result


def sanitize_filename(text: str) -> str:
    """Convert text to valid filename.

    Args:
        text: Text to sanitize.

    Returns:
        Sanitized filename string.
    """
    filename = text.lower()
    filename = re.sub(r"[^a-z0-9]+", "_", filename)
    filename = filename.strip("_")
    return filename


# ---------------------------------------------------------------------------
# Output Functions
# ---------------------------------------------------------------------------


def create_output_files(
    records: List[ExtensionRecord],
    output_dir: str = "./extensions_output",
    all_assignments: Optional[List[str]] = None,
    dry_run: bool = False,
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Create CSV files per assignment.

    Args:
        records: List of extension records to write.
        output_dir: Directory to write output files.
        all_assignments: Optional list of all assignment names to include.
        dry_run: If True, do not write files, just return what would be written.

    Returns:
        A tuple of (file_info, io_errors) where:
        - file_info: List of dicts with assignment, filename, num_students, dates
        - io_errors: List of error messages for I/O failures
    """
    io_errors: List[str] = []

    if not dry_run:
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            io_errors.append(f"Unable to create output directory '{output_dir}': {exc}")
            return [], io_errors

    # Group by assignment
    by_assignment: Dict[str, List[ExtensionRecord]] = defaultdict(list)
    for record in records:
        by_assignment[record.assignment].append(record)

    if all_assignments:
        for assignment in all_assignments:
            if assignment not in by_assignment:
                by_assignment[assignment] = []

    file_info: List[Dict[str, Any]] = []

    for assignment, assignment_records in sorted(by_assignment.items()):
        # Create filename
        filename = sanitize_filename(assignment)
        filename = f"{filename}_extensions.csv"
        filepath = os.path.join(output_dir, filename)

        # Sort by email
        assignment_records.sort(key=lambda x: x.email)

        if not dry_run:
            # Write CSV with BOM (UTF-8-sig)
            try:
                buffer = io.StringIO()
                writer = csv.writer(buffer, lineterminator="\n")
                writer.writerow(["Email", "Name", "Assignment", "DueDate", "RECORD"])

                for record in assignment_records:
                    due_date_str = format_date(record.due_date) if record.due_date else ""
                    record_str = f"{record.email} - {record.name} - {record.assignment} - {due_date_str}"
                    writer.writerow(
                        [
                            record.email,
                            record.name,
                            record.assignment,
                            due_date_str,
                            record_str,
                        ]
                    )

                contents = buffer.getvalue().rstrip("\n")
                with open(filepath, "w", newline="", encoding="utf-8-sig") as f:
                    f.write(contents)
            except OSError as exc:
                io_errors.append(f"Failed to write '{filepath}': {exc}")
                continue

        # Collect info for summary
        if assignment_records:
            dates = [r.due_date for r in assignment_records if r.due_date]
            file_info.append(
                {
                    "assignment": assignment,
                    "filename": filename,
                    "num_students": len(assignment_records),
                    "earliest_date": format_date(min(dates)) if dates else "N/A",
                    "latest_date": format_date(max(dates)) if dates else "N/A",
                }
            )
        else:
            file_info.append(
                {
                    "assignment": assignment,
                    "filename": filename,
                    "num_students": 0,
                    "earliest_date": "N/A",
                    "latest_date": "N/A",
                }
            )

    return file_info, io_errors


def write_processed_copy(
    input_path: Optional[str],
    table_data: Optional[TableData],
    processed_rows: Iterable[int],
    dry_run: bool = False,
) -> Tuple[Optional[str], Optional[str]]:
    """Write a copy of the original input marking processed rows with a ``*``.

    Args:
        input_path: Path to the original input file.
        table_data: Parsed table metadata.
        processed_rows: Row numbers that were successfully processed.
        dry_run: If True, do not write files, just return what would be written.

    Returns:
        A tuple of (output_path, error_message).
    """
    if not input_path or not table_data:
        return None, "No input file or parsed table data available"

    done_col = table_data.col_map.get("DONE?")
    if done_col is None:
        return None, "Input file does not include a 'DONE?' column"

    processed_path = Path(input_path)
    output_path = processed_path.with_name(
        f"{processed_path.stem}_PROCESSED{processed_path.suffix}"
    )

    if dry_run:
        return str(output_path), None

    delimiter = table_data.delimiter
    processed_set = set(processed_rows)

    try:
        buffer = io.StringIO()
        writer = csv.writer(buffer, delimiter=delimiter, lineterminator="\n")
        writer.writerow(table_data.header)

        for row in table_data.rows:
            fields = row["fields"][:]
            if row["row_num"] in processed_set:
                if done_col >= len(fields):
                    fields.extend([""] * (done_col - len(fields) + 1))
                fields[done_col] = "*"
            writer.writerow(fields)

        contents = buffer.getvalue().rstrip("\n")
        with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
            f.write(contents)
    except OSError as exc:
        return None, str(exc)

    return str(output_path), None


def generate_summary(
    records: List[ExtensionRecord],
    file_info: List[Dict[str, Any]],
    errors: List[ParseError],
    output_dir: str,
    io_errors: Optional[List[str]] = None,
    failures_path: Optional[str] = None,
    dry_run: bool = False,
) -> str:
    """Generate summary report.

    Args:
        records: List of processed extension records.
        file_info: List of file info dictionaries.
        errors: List of parsing errors.
        output_dir: Output directory path.
        io_errors: Optional list of I/O error messages.
        failures_path: Optional path to failures file.
        dry_run: If True, do not write files.

    Returns:
        The summary text.
    """
    summary_lines: List[str] = []
    summary_lines.append("\n" + "=" * 70)
    summary_lines.append("EXTENSION REQUEST PROCESSING SUMMARY")
    summary_lines.append("=" * 70)
    summary_lines.append(f"\nTotal Assignments: {len(file_info)}")
    summary_lines.append(f"Total Students: {len(records)}")
    summary_lines.append(f"\nOutput Directory: {os.path.abspath(output_dir)}")
    if file_info:
        summary_lines.append("\n" + "-" * 70)
        summary_lines.append("PER-ASSIGNMENT BREAKDOWN")
        summary_lines.append("-" * 70)

        for info in file_info:
            summary_lines.append(f"\n{info['assignment']}")
            summary_lines.append(f"  File: {info['filename']}")
            summary_lines.append(f"  Students: {info['num_students']}")
            if info["num_students"] > 0:
                summary_lines.append(
                    f"  Date Range: {info['earliest_date']} to {info['latest_date']}"
                )
            else:
                summary_lines.append("  Date Range: N/A (no extensions)")

    if failures_path:
        summary_lines.append("\n" + "-" * 70)
        summary_lines.append("Rejected rows written to:")
        summary_lines.append(f"  {failures_path}")

    if io_errors:
        summary_lines.append("\n" + "-" * 70)
        summary_lines.append("FILE I/O ISSUES")
        summary_lines.append("-" * 70)
        for issue in io_errors:
            summary_lines.append(f"  * {issue}")

    if errors:
        summary_lines.append("\n" + "-" * 70)
        summary_lines.append(f"PARSING ERRORS ({len(errors)} found)")
        summary_lines.append("-" * 70)
        for error in errors:
            row_info = f"Row {error.row}: " if error.row else ""
            summary_lines.append(f"  * {row_info}{error.message}")
    else:
        summary_lines.append("\n" + "-" * 70)
        summary_lines.append("[OK] No errors detected")
        summary_lines.append("-" * 70)

    summary_lines.append("\n" + "=" * 70)

    summary_text = "\n".join(summary_lines)

    # Print summary
    logger.info(summary_text)

    if not dry_run:
        # Save summary to file
        summary_file = os.path.join(output_dir, "SUMMARY.txt")
        try:
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(summary_text)
            logger.info(f"\nSummary saved to: {summary_file}")
        except OSError as exc:
            logger.error(f"Failed to write summary: {exc}")

    return summary_text


def write_failure_report(
    errors: List[ParseError],
    output_dir: str,
    dry_run: bool = False,
) -> Optional[str]:
    """Write rejected rows (if any) to a CSV file.

    Args:
        errors: List of parsing errors.
        output_dir: Directory to write the failure report.
        dry_run: If True, do not write files.

    Returns:
        Path to the failures file, or None if no errors or write failed.
    """
    if not errors:
        return None

    failures_path = os.path.join(output_dir, "failures.csv")

    if dry_run:
        return failures_path

    try:
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        writer.writerow(["Row", "Message", "Line"])
        for error in errors:
            writer.writerow(
                [
                    error.row or "",
                    error.message,
                    error.line or "",
                ]
            )

        contents = buffer.getvalue().rstrip("\n")
        with open(failures_path, "w", newline="", encoding="utf-8") as f:
            f.write(contents)
        return failures_path
    except OSError as exc:
        logger.error(f"Unable to write failure report: {exc}")
        return None


# ---------------------------------------------------------------------------
# Input Functions
# ---------------------------------------------------------------------------


def read_from_file(filename: str) -> Optional[List[str]]:
    """Read data from a file.

    Args:
        filename: Path to the file to read.

    Returns:
        List of lines from the file, or None if reading failed.
    """
    candidates: List[Path] = []

    primary = Path(filename).expanduser()
    candidates.append(primary)

    # If the file was provided as a relative path, also try resolving it
    # relative to the script's directory. This helps when the CLI is invoked
    # from another working directory (e.g., via shortcuts).
    if not primary.is_absolute():
        candidates.append(Path(__file__).parent / filename)

    tried_paths: List[Path] = []

    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved in tried_paths:
            continue
        tried_paths.append(resolved)

        if resolved.exists():
            try:
                with open(resolved, "r", encoding="utf-8") as f:
                    return [line.rstrip("\n") for line in f.readlines()]
            except OSError as e:
                logger.error(f"Error reading file '{resolved}': {e}")
                return None

    tried = ", ".join(str(path) for path in tried_paths)
    logger.error(
        f"Error: File '{filename}' not found (searched: {tried}). Current working "
        f"directory: {Path.cwd()}"
    )
    return None


def read_from_clipboard() -> Optional[List[str]]:
    """Try to read from clipboard (requires pyperclip).

    Returns:
        List of lines from clipboard, or None if pyperclip is not available.
    """
    try:
        import pyperclip

        data = pyperclip.paste()
        return data.split("\n")
    except ImportError:
        return None


def read_from_stdin() -> List[str]:
    """Read from stdin (paste directly).

    Returns:
        List of lines entered by the user.
    """
    print("Paste your MS Forms data (tab-separated, with headers).")
    print("Press Ctrl+D (Unix/Mac) or Ctrl+Z + Enter (Windows) when done:\n")

    lines: List[str] = []
    try:
        while True:
            line = input()
            lines.append(line)
    except EOFError:
        pass

    return lines


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    """Parse command line arguments.

    Args:
        argv: Command line arguments. Defaults to sys.argv[1:].

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-file",
        help="Path to a tab-delimited MS Forms export to process.",
    )
    parser.add_argument(
        "--clipboard",
        action="store_true",
        help="Read the data from the clipboard (requires pyperclip).",
    )
    parser.add_argument(
        "--output-dir",
        default="./extensions_output",
        help="Directory where CSVs and summary should be written.",
    )
    parser.add_argument(
        "--no-adjust",
        action="store_true",
        help="Skip moving requested dates to the following Sunday.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be done without writing any files.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose output.",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Suppress non-essential output.",
    )
    return parser.parse_args(argv)


def _setup_logging(verbose: bool = False, quiet: bool = False) -> None:
    """Configure logging based on verbosity settings.

    Args:
        verbose: Enable debug-level logging.
        quiet: Suppress info-level logging.
    """
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logging.basicConfig(
        level=level,
        format="%(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main(argv: Optional[List[str]] = None) -> int:
    """Main entry point for the CLI.

    Args:
        argv: Command line arguments. Defaults to sys.argv[1:].

    Returns:
        Exit code (0 for success, non-zero for errors).
    """
    args = parse_arguments(argv)
    _setup_logging(verbose=args.verbose, quiet=args.quiet)

    logger.info("=" * 70)
    logger.info("MS Forms Extension Request Processor")
    logger.info("=" * 70)

    if args.dry_run:
        logger.info("[DRY RUN] No files will be written.")

    lines: Optional[List[str]] = None

    if args.input_file:
        lines = read_from_file(args.input_file)
    elif args.clipboard:
        lines = read_from_clipboard()
        if lines is None:
            logger.error("Clipboard support is unavailable (pyperclip not installed).")
            return 1
    else:
        # Fallback to the interactive workflow when no arguments are supplied.
        logger.info("\nHow would you like to provide data?")
        logger.info("1. Paste directly (type/paste into terminal)")
        logger.info("2. Read from file")

        choice = input("\nSelect option (1 or 2): ").strip()

        if choice == "1":
            lines = read_from_stdin()
        elif choice == "2":
            filename = input(
                "\nEnter filename (e.g., extension_requests.txt): "
            ).strip()
            lines = read_from_file(filename)
        else:
            logger.error("Invalid option")
            return 1

    if not lines or not any(line.strip() for line in lines):
        logger.error("No data provided")
        return 1

    # Remove empty lines from start/end
    lines = [line for line in lines if line.strip()]

    # Process
    logger.info("\nProcessing...")
    records, errors, table_data = process_extension_data(lines)

    if not records and errors:
        logger.error("\nFailed to parse data:")
        for error in errors:
            logger.error(f"  * {error.message}")
        return 1

    if not records and not errors:
        logger.info("\nNo new extension requests to process (all rows already marked DONE?).")

    logger.info(f"[OK] Parsed {len(records)} records")

    # Deduplicate
    original_count = len(records)
    records = deduplicate_records(records)
    if len(records) < original_count:
        logger.info(
            f"[OK] After deduplication: {len(records)} records "
            f"(removed {original_count - len(records)} duplicates)"
        )
    else:
        logger.info("[OK] No duplicates found")

    # Adjust dates
    if not args.no_adjust:
        records = adjust_dates(records)
        adjusted_count = sum(
            1 for r in records if r.original_date != r.due_date
        )
        logger.info(f"[OK] Adjusted {adjusted_count} dates to Sunday")
    else:
        # Set due_date equal to requested_date for records without adjustment
        records = [
            ExtensionRecord(
                email=r.email,
                name=r.name,
                assignment=r.assignment,
                requested_date=r.requested_date,
                row_num=r.row_num,
                original_date=r.requested_date,
                due_date=r.requested_date,
            )
            for r in records
        ]
        logger.info("[OK] Skipped Sunday adjustment (--no-adjust)")

    # Create output files
    output_dir = args.output_dir
    all_assignments = table_data.all_assignments if table_data else None
    file_info, io_errors = create_output_files(
        records, output_dir, all_assignments=all_assignments, dry_run=args.dry_run
    )
    logger.info(f"[OK] Created {len(file_info)} CSV files")

    processed_copy_path: Optional[str] = None
    processed_copy_error: Optional[str] = None
    if args.input_file and table_data:
        processed_rows = {record.row_num for record in records}
        processed_copy_path, processed_copy_error = write_processed_copy(
            args.input_file,
            table_data,
            processed_rows,
            dry_run=args.dry_run,
        )
        if processed_copy_path:
            logger.info(f"[OK] Wrote processed input copy to {processed_copy_path}")
        elif processed_copy_error:
            logger.warning(
                f"[WARN] Unable to write processed input copy: {processed_copy_error}"
            )

    failures_path = write_failure_report(errors, output_dir, dry_run=args.dry_run)
    if failures_path:
        logger.info(f"[OK] Wrote rejected rows to {failures_path}")

    # Summary
    generate_summary(
        records,
        file_info,
        errors,
        output_dir,
        io_errors=io_errors,
        failures_path=failures_path,
        dry_run=args.dry_run,
    )

    # Return appropriate exit code
    if io_errors:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
