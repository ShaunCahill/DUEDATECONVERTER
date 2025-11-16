import textwrap

import pytest

from process_extensions import (
    adjust_dates,
    deduplicate_records,
    get_next_sunday,
    parse_date,
    process_extension_data,
    sanitize_filename,
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

    records, errors = process_extension_data(data)
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

    records, errors = process_extension_data(data)
    assert len(records) == 0
    assert len(errors) == 2
    assert all(error.get('row') for error in errors)


@pytest.mark.parametrize(
    'raw,expected',
    [
        ('Hello World', 'hello_world'),
        ('HW#1: Arrays', 'hw_1_arrays'),
    ],
)
def test_sanitize_filename(raw, expected):
    assert sanitize_filename(raw) == expected
