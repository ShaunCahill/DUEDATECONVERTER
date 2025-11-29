"""Tests for the process_extensions module."""

import csv
import io
import textwrap
from pathlib import Path
from unittest import mock

import pytest

import process_extensions
from process_extensions import (
    ColumnConfig,
    ExtensionRecord,
    ParseError,
    TableData,
    adjust_dates,
    create_output_files,
    deduplicate_records,
    get_next_sunday,
    main,
    parse_date,
    process_extension_data,
    read_from_clipboard,
    read_from_file,
    read_from_stdin,
    sanitize_filename,
    write_failure_report,
    write_processed_copy,
)


# ---------------------------------------------------------------------------
# Date Utility Tests
# ---------------------------------------------------------------------------


def test_parse_date_valid_and_invalid():
    """Test date parsing with valid and invalid inputs."""
    assert parse_date("01/15/2024").month == 1
    assert parse_date("13/01/2024") is None
    assert parse_date("invalid") is None
    assert parse_date("") is None


def test_get_next_sunday_advances_and_preserves():
    """Test that get_next_sunday moves to next Sunday or preserves if already Sunday."""
    friday = parse_date("02/02/2024")
    sunday = parse_date("02/04/2024")
    assert get_next_sunday(friday) == sunday
    assert get_next_sunday(sunday) == sunday


# ---------------------------------------------------------------------------
# Record Processing Tests
# ---------------------------------------------------------------------------


def test_deduplicate_records_keeps_latest_date():
    """Test that deduplication keeps the record with the latest date."""
    records = [
        ExtensionRecord(
            email="a@example.com",
            name="Alice",
            assignment="HW1",
            requested_date=parse_date("02/01/2024"),
            row_num=2,
        ),
        ExtensionRecord(
            email="a@example.com",
            name="Alice",
            assignment="HW1",
            requested_date=parse_date("02/05/2024"),
            row_num=3,
        ),
        ExtensionRecord(
            email="b@example.com",
            name="Bob",
            assignment="HW1",
            requested_date=parse_date("02/03/2024"),
            row_num=4,
        ),
    ]

    deduped = deduplicate_records(records)
    assert len(deduped) == 2
    for record in deduped:
        if record.email == "a@example.com":
            assert record.requested_date == parse_date("02/05/2024")


def test_deduplicate_records_does_not_mutate_input():
    """Test that deduplicate_records does not mutate the input list."""
    original = [
        ExtensionRecord(
            email="a@example.com",
            name="Alice",
            assignment="HW1",
            requested_date=parse_date("02/01/2024"),
            row_num=2,
        ),
    ]
    original_len = len(original)

    deduplicate_records(original)

    assert len(original) == original_len


def test_adjust_dates_records_include_original_and_adjusted_fields():
    """Test that adjust_dates sets both original_date and due_date fields."""
    records = [
        ExtensionRecord(
            email="a@example.com",
            name="Alice",
            assignment="HW1",
            requested_date=parse_date("02/02/2024"),
            row_num=2,
        ),
        ExtensionRecord(
            email="b@example.com",
            name="Bob",
            assignment="HW1",
            requested_date=parse_date("02/04/2024"),
            row_num=3,
        ),
    ]
    adjusted = adjust_dates(records)

    assert adjusted[0].due_date.weekday() == 6  # Sunday
    assert adjusted[0].original_date == parse_date("02/02/2024")
    # Already Sunday
    assert adjusted[1].due_date == parse_date("02/04/2024")


def test_adjust_dates_does_not_mutate_input():
    """Test that adjust_dates does not mutate the input list."""
    original = ExtensionRecord(
        email="a@example.com",
        name="Alice",
        assignment="HW1",
        requested_date=parse_date("02/02/2024"),
        row_num=2,
    )
    records = [original]

    adjust_dates(records)

    # Original record should not have due_date set
    assert original.due_date is None


# ---------------------------------------------------------------------------
# Data Processing Tests
# ---------------------------------------------------------------------------


def test_process_extension_data_collects_errors_for_missing_columns():
    """Test that missing required columns are reported as errors."""
    data = textwrap.dedent(
        """Email\tName\tWhich assignment due date do you want to change?\n"""
    ).strip().split("\n")

    records, errors, _ = process_extension_data(data)
    assert records == []
    assert errors and "Missing required columns" in errors[0].message


def test_process_extension_data_collects_row_level_errors():
    """Test that row-level errors are collected properly."""
    data = textwrap.dedent(
        """
        Email\tName\tWhich assignment due date do you want to change?\tWhat would you like to new date to be change too?
        s@example.com\tSam\tHW1\t13/01/2024
        \tPat\tHW1\t01/30/2024
        """
    ).strip().split("\n")

    records, errors, _ = process_extension_data(data)
    assert len(records) == 0
    assert len(errors) == 2
    assert all(error.row for error in errors)


def test_process_extension_data_handles_csv_and_done_column(tmp_path):
    """Test that CSV format and DONE? column are handled correctly."""
    csv_data = textwrap.dedent(
        """
        Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?,DONE?
        a@example.com,Alice,HW1,01/30/2024,
        b@example.com,Bob,HW1,02/05/2024,*
        """
    ).strip().split("\n")

    records, errors, table = process_extension_data(csv_data)

    assert not errors
    assert len(records) == 1
    assert records[0].email == "a@example.com"
    assert table
    assert len(table.rows) == 2


def test_process_extension_data_handles_bom_and_spaced_filename(tmp_path):
    """Test that BOM (byte order mark) in headers is handled correctly."""
    target = tmp_path / "COMM 495 - Project Management F2025.csv"
    target.write_text(
        textwrap.dedent(
            """
            \ufeffEmail,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?
            a@example.com,Alice,HW1,01/30/2024
            """
        ).lstrip(),
        encoding="utf-8-sig",
    )

    lines = target.read_text(encoding="utf-8").splitlines()

    records, errors, _ = process_extension_data(lines)

    assert not errors
    assert len(records) == 1
    assert records[0].email == "a@example.com"


def test_process_extension_data_with_custom_columns():
    """Test that custom column configuration works."""
    custom_cols = ColumnConfig(
        email="StudentEmail",
        name="StudentName",
        assignment="Assignment",
        date="DueDate",
    )

    data = textwrap.dedent(
        """
        StudentEmail,StudentName,Assignment,DueDate
        a@example.com,Alice,HW1,01/30/2024
        """
    ).strip().split("\n")

    records, errors, _ = process_extension_data(data, columns=custom_cols)

    assert not errors
    assert len(records) == 1
    assert records[0].email == "a@example.com"


def test_process_extension_data_returns_extension_records():
    """Test that process_extension_data returns ExtensionRecord objects."""
    data = textwrap.dedent(
        """
        Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?
        a@example.com,Alice,HW1,01/30/2024
        """
    ).strip().split("\n")

    records, errors, _ = process_extension_data(data)

    assert not errors
    assert len(records) == 1
    assert isinstance(records[0], ExtensionRecord)
    assert records[0].email == "a@example.com"
    assert records[0].name == "Alice"


# ---------------------------------------------------------------------------
# Output File Tests
# ---------------------------------------------------------------------------


def test_write_processed_copy_marks_processed_rows(tmp_path):
    """Test that processed rows are marked with asterisks in the output."""
    source = tmp_path / "input.csv"
    source.write_text(
        textwrap.dedent(
            """
            Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?,DONE?
            a@example.com,Alice,HW1,01/30/2024,
            b@example.com,Bob,HW1,02/05/2024,
            """
        ).lstrip()
    )

    lines = source.read_text().splitlines()
    records, errors, table = process_extension_data(lines)
    assert not errors

    processed_rows = {record.row_num for record in records[:1]}
    output_path, error = write_processed_copy(str(source), table, processed_rows)
    assert error is None

    with open(output_path, newline="", encoding="utf-8-sig") as f:
        reader = list(csv.reader(f))

    assert reader[0][-1] == "DONE?"
    assert reader[1][-1] == "*"
    assert reader[2][-1] == ""

    raw_contents = Path(output_path).read_text(encoding="utf-8-sig")
    assert not raw_contents.endswith("\n")


def test_write_processed_copy_dry_run(tmp_path):
    """Test that dry_run mode does not write files."""
    source = tmp_path / "input.csv"
    source.write_text(
        textwrap.dedent(
            """
            Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?,DONE?
            a@example.com,Alice,HW1,01/30/2024,
            """
        ).lstrip()
    )

    lines = source.read_text().splitlines()
    records, errors, table = process_extension_data(lines)
    assert not errors

    processed_rows = {record.row_num for record in records}
    output_path, error = write_processed_copy(
        str(source), table, processed_rows, dry_run=True
    )

    assert output_path is not None
    assert error is None
    # File should not exist because dry_run=True
    assert not Path(output_path).exists()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Hello World", "hello_world"),
        ("HW#1: Arrays", "hw_1_arrays"),
        ("  spaces  ", "spaces"),
        ("UPPERCASE", "uppercase"),
    ],
)
def test_sanitize_filename(raw, expected):
    """Test filename sanitization with various inputs."""
    assert sanitize_filename(raw) == expected


def test_create_output_files_adds_record_column_and_unix_newlines(tmp_path):
    """Test that output files have correct format and RECORD column."""
    records = [
        ExtensionRecord(
            email="21sf55@queensu.ca",
            name="Saba Fadaei",
            assignment="LAB: No-Code",
            requested_date=parse_date("11/30/2025"),
            row_num=2,
            original_date=parse_date("11/30/2025"),
            due_date=parse_date("11/30/2025"),
        )
    ]

    file_info, io_errors = create_output_files(records, output_dir=tmp_path)

    assert not io_errors
    assert file_info and file_info[0]["filename"].endswith("_extensions.csv")

    output_file = tmp_path / file_info[0]["filename"]
    raw_contents = output_file.read_text(encoding="utf-8-sig")

    assert "\r" not in raw_contents
    assert not raw_contents.endswith("\n")

    with open(output_file, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    assert rows[0] == ["Email", "Name", "Assignment", "DueDate", "RECORD"]
    assert rows[1][-1] == "21sf55@queensu.ca - Saba Fadaei - LAB: No-Code - 11/30/2025"


def test_create_output_files_dry_run(tmp_path):
    """Test that dry_run mode does not write files."""
    records = [
        ExtensionRecord(
            email="a@example.com",
            name="Alice",
            assignment="HW1",
            requested_date=parse_date("01/30/2024"),
            row_num=2,
            original_date=parse_date("01/30/2024"),
            due_date=parse_date("02/04/2024"),
        )
    ]

    file_info, io_errors = create_output_files(
        records, output_dir=tmp_path / "output", dry_run=True
    )

    assert not io_errors
    assert len(file_info) == 1
    # Directory should not exist because dry_run=True
    assert not (tmp_path / "output").exists()


def test_write_failure_report_trims_trailing_newline(tmp_path):
    """Test that failure report does not have trailing newline."""
    errors = [
        ParseError(row=2, message="Missing fields", line="a,b,c"),
    ]

    failures_path = write_failure_report(errors, str(tmp_path))
    assert failures_path

    text = Path(failures_path).read_text(encoding="utf-8")
    assert not text.endswith("\n")


def test_write_failure_report_dry_run(tmp_path):
    """Test that dry_run mode does not write failure report."""
    errors = [
        ParseError(row=2, message="Missing fields", line="a,b,c"),
    ]

    failures_path = write_failure_report(errors, str(tmp_path), dry_run=True)
    assert failures_path
    assert not Path(failures_path).exists()


# ---------------------------------------------------------------------------
# Input Function Tests
# ---------------------------------------------------------------------------


def test_read_from_file_checks_script_directory(monkeypatch, tmp_path):
    """Test that read_from_file falls back to script directory."""
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    data_file = source_dir / "COMM 394 - Coding Literacy for Managers F2025.csv"
    data_file.write_text("Email", encoding="utf-8")

    monkeypatch.setattr(
        process_extensions,
        "__file__",
        str(source_dir / "process_extensions.py"),
    )

    working_dir = tmp_path / "working"
    working_dir.mkdir()
    monkeypatch.chdir(working_dir)

    lines = read_from_file("COMM 394 - Coding Literacy for Managers F2025.csv")

    assert lines == ["Email"]


def test_read_from_file_reports_all_attempts(capsys, tmp_path, monkeypatch):
    """Test that read_from_file reports all attempted paths."""
    # Set up logging to capture output
    import logging

    logging.basicConfig(level=logging.ERROR, format="%(message)s")

    missing = tmp_path / "missing.csv"

    result = read_from_file(str(missing))

    assert result is None


def test_read_from_clipboard_returns_none_without_pyperclip(monkeypatch):
    """Test that read_from_clipboard returns None when pyperclip is not available."""
    # Temporarily make pyperclip unavailable
    import sys

    original_modules = sys.modules.copy()

    def mock_import(name, *args, **kwargs):
        if name == "pyperclip":
            raise ImportError("No module named 'pyperclip'")
        return original_modules.get(name)

    monkeypatch.setattr("builtins.__import__", mock_import)

    # Clear the cached import if any
    if "pyperclip" in sys.modules:
        monkeypatch.delitem(sys.modules, "pyperclip")

    result = read_from_clipboard()
    assert result is None


def test_read_from_stdin(monkeypatch, capsys):
    """Test reading from stdin."""
    inputs = iter(["header", "row1", "row2"])

    def mock_input(prompt=""):
        try:
            return next(inputs)
        except StopIteration:
            raise EOFError

    monkeypatch.setattr("builtins.input", mock_input)

    result = read_from_stdin()

    assert result == ["header", "row1", "row2"]


# ---------------------------------------------------------------------------
# CLI Tests
# ---------------------------------------------------------------------------


def test_main_with_input_file(tmp_path):
    """Test main() with an input file."""
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        textwrap.dedent(
            """
            Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?,DONE?
            a@example.com,Alice,HW1,01/30/2024,
            """
        ).lstrip()
    )

    output_dir = tmp_path / "output"

    exit_code = main(
        [
            "--input-file",
            str(input_file),
            "--output-dir",
            str(output_dir),
            "--quiet",
        ]
    )

    assert exit_code == 0
    assert output_dir.exists()
    assert (output_dir / "SUMMARY.txt").exists()


def test_main_with_dry_run(tmp_path):
    """Test main() with --dry-run flag."""
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        textwrap.dedent(
            """
            Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?
            a@example.com,Alice,HW1,01/30/2024
            """
        ).lstrip()
    )

    output_dir = tmp_path / "output"

    exit_code = main(
        [
            "--input-file",
            str(input_file),
            "--output-dir",
            str(output_dir),
            "--dry-run",
            "--quiet",
        ]
    )

    assert exit_code == 0
    # Output directory should not exist because of dry-run
    assert not output_dir.exists()


def test_main_with_no_adjust(tmp_path):
    """Test main() with --no-adjust flag."""
    input_file = tmp_path / "input.csv"
    input_file.write_text(
        textwrap.dedent(
            """
            Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?
            a@example.com,Alice,HW1,01/30/2024
            """
        ).lstrip()
    )

    output_dir = tmp_path / "output"

    exit_code = main(
        [
            "--input-file",
            str(input_file),
            "--output-dir",
            str(output_dir),
            "--no-adjust",
            "--quiet",
        ]
    )

    assert exit_code == 0

    # Check that the date was not adjusted
    output_file = output_dir / "hw1_extensions.csv"
    with open(output_file, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.reader(f))

    # 01/30/2024 is a Tuesday, should remain 01/30/2024 with --no-adjust
    assert rows[1][3] == "01/30/2024"


def test_main_with_missing_file(tmp_path, capsys):
    """Test main() with a missing input file."""
    exit_code = main(
        [
            "--input-file",
            str(tmp_path / "missing.csv"),
            "--quiet",
        ]
    )

    assert exit_code == 1


def test_main_with_empty_data(tmp_path):
    """Test main() with empty input data."""
    input_file = tmp_path / "input.csv"
    input_file.write_text("")

    exit_code = main(
        [
            "--input-file",
            str(input_file),
            "--quiet",
        ]
    )

    assert exit_code == 1


def test_main_with_invalid_data(tmp_path):
    """Test main() with invalid input data (missing columns)."""
    input_file = tmp_path / "input.csv"
    input_file.write_text("Email,Name\na@example.com,Alice\n")

    exit_code = main(
        [
            "--input-file",
            str(input_file),
            "--quiet",
        ]
    )

    assert exit_code == 1


# ---------------------------------------------------------------------------
# Dataclass Tests
# ---------------------------------------------------------------------------


def test_extension_record_with_adjusted_date():
    """Test ExtensionRecord.with_adjusted_date method."""
    record = ExtensionRecord(
        email="a@example.com",
        name="Alice",
        assignment="HW1",
        requested_date=parse_date("01/30/2024"),
        row_num=2,
    )

    adjusted = record.with_adjusted_date(parse_date("02/04/2024"))

    # Original should be unchanged
    assert record.due_date is None
    assert record.original_date is None

    # Adjusted should have new values
    assert adjusted.due_date == parse_date("02/04/2024")
    assert adjusted.original_date == parse_date("01/30/2024")
    assert adjusted.email == record.email
    assert adjusted.row_num == record.row_num


def test_parse_error_to_dict():
    """Test ParseError.to_dict method."""
    error = ParseError(
        message="Test error",
        row=5,
        line="a,b,c",
    )

    d = error.to_dict()

    assert d["message"] == "Test error"
    assert d["row"] == 5
    assert d["line"] == "a,b,c"


def test_column_config_required():
    """Test ColumnConfig.required property."""
    config = ColumnConfig()

    required = config.required

    assert len(required) == 4
    assert "Email" in required
    assert "Name" in required
    assert "DONE?" not in required  # DONE? is optional
