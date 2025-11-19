import csv
import textwrap

import pytest

from process_extensions import (
    adjust_dates,
    create_output_files,
    deduplicate_records,
    get_next_sunday,
    parse_date,
    process_extension_data,
    sanitize_filename,
    write_processed_copy,
)


def test_parse_date_valid_and_invalid():
    assert parse_date('01/15/2024').month == 1
    assert parse_date('13/01/2024') is None


def test_get_next_sunday_advances_and_preserves():
    friday = parse_date('02/02/2024')
    sunday = parse_date('02/04/2024')
    assert get_next_sunday(friday) == sunday
    assert get_next_sunday(sunday) == sunday


def test_deduplicate_records_keeps_latest_date():
    records = [
        {'assignment': 'HW1', 'email': 'a@example.com', 'requested_date': parse_date('02/01/2024')},
        {'assignment': 'HW1', 'email': 'a@example.com', 'requested_date': parse_date('02/05/2024')},
        {'assignment': 'HW1', 'email': 'b@example.com', 'requested_date': parse_date('02/03/2024')},
    ]

    deduped = deduplicate_records(records)
    assert len(deduped) == 2
    for record in deduped:
        if record['email'] == 'a@example.com':
            assert record['requested_date'] == parse_date('02/05/2024')


def test_adjust_dates_records_include_original_and_adjusted_fields():
    records = [
        {'requested_date': parse_date('02/02/2024')},
        {'requested_date': parse_date('02/04/2024')},
    ]
    adjusted = adjust_dates(records)

    assert adjusted[0]['due_date'].weekday() == 6  # Sunday
    assert adjusted[0]['original_date'] == parse_date('02/02/2024')
    # Already Sunday
    assert adjusted[1]['due_date'] == parse_date('02/04/2024')


def test_process_extension_data_collects_errors_for_missing_columns():
    data = textwrap.dedent(
        """Email\tName\tWhich assignment due date do you want to change?\n"""
    ).strip().split('\n')

    records, errors, _ = process_extension_data(data)
    assert records == []
    assert errors and 'Missing required columns' in errors[0]['message']


def test_process_extension_data_collects_row_level_errors():
    data = textwrap.dedent(
        """
        Email\tName\tWhich assignment due date do you want to change?\tWhat would you like to new date to be change too?
        s@example.com\tSam\tHW1\t13/01/2024
        \tPat\tHW1\t01/30/2024
        """
    ).strip().split('\n')

    records, errors, _ = process_extension_data(data)
    assert len(records) == 0
    assert len(errors) == 2
    assert all(error.get('row') for error in errors)


def test_process_extension_data_handles_csv_and_done_column(tmp_path):
    csv_data = textwrap.dedent(
        """
        Email,Name,Which assignment due date do you want to change?,What would you like to new date to be change too?,DONE?
        a@example.com,Alice,HW1,01/30/2024,
        b@example.com,Bob,HW1,02/05/2024,*
        """
    ).strip().split('\n')

    records, errors, table = process_extension_data(csv_data)

    assert not errors
    assert len(records) == 1
    assert records[0]['email'] == 'a@example.com'
    assert table
    assert len(table['rows']) == 2


def test_write_processed_copy_marks_processed_rows(tmp_path):
    source = tmp_path / 'input.csv'
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

    processed_rows = {record['row_num'] for record in records[:1]}
    output_path, error = write_processed_copy(str(source), table, processed_rows)
    assert error is None

    with open(output_path, newline='', encoding='utf-8-sig') as f:
        reader = list(csv.reader(f))

    assert reader[0][-1] == 'DONE?'
    assert reader[1][-1] == '*'
    assert reader[2][-1] == ''


@pytest.mark.parametrize(
    'raw,expected',
    [
        ('Hello World', 'hello_world'),
        ('HW#1: Arrays', 'hw_1_arrays'),
    ],
)
def test_sanitize_filename(raw, expected):
    assert sanitize_filename(raw) == expected


def test_create_output_files_adds_record_column_and_unix_newlines(tmp_path):
    records = [
        {
            'email': '21sf55@queensu.ca',
            'name': 'Saba Fadaei',
            'assignment': 'LAB: No-Code',
            'due_date': parse_date('11/30/2025'),
        }
    ]

    file_info, io_errors = create_output_files(records, output_dir=tmp_path)

    assert not io_errors
    assert file_info and file_info[0]['filename'].endswith('_extensions.csv')

    output_file = tmp_path / file_info[0]['filename']
    raw_contents = output_file.read_text(encoding='utf-8-sig')

    assert '\r' not in raw_contents

    with open(output_file, newline='', encoding='utf-8-sig') as f:
        rows = list(csv.reader(f))

    assert rows[0] == ['Email', 'Name', 'Assignment', 'DueDate', 'RECORD']
    assert rows[1][-1] == '21sf55@queensu.ca - Saba Fadaei - LAB: No-Code - 11/30/2025'
