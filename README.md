# Due Date Converter

A small CLI utility for processing Microsoft Forms extension requests. It reads the
tab-delimited export produced by the form, validates each submission, deduplicates
repeat entries, snaps requested dates to the following Sunday, and emits per-assignment
CSVs along with a textual summary.

## Requirements

* Python 3.9+
* `pyperclip` (optional) – only needed for the `--clipboard` flag.

Install the optional dependency with:

```bash
pip install pyperclip
```

## Usage

The main entry point is `process_extensions.py`. You can provide data in three ways:

1. **Command line argument:**
   ```bash
   python process_extensions.py --input-file path/to/export.txt
   ```
2. **Clipboard:**
   ```bash
   python process_extensions.py --clipboard
   ```
   This requires `pyperclip` to be installed.
3. **Interactive prompt:** Run the script without arguments. It will ask whether you
   want to paste data directly into the terminal or specify a file path.

### Arguments

| Flag | Description |
| --- | --- |
| `--input-file PATH` | Path to the tab-delimited MS Forms export. |
| `--clipboard` | Read the export from the system clipboard. |
| `--output-dir DIR` | Directory where CSVs, `SUMMARY.txt`, and `failures.csv` are written. Defaults to `./extensions_output`. |
| `--no-adjust` | Skip snapping requested dates to the following Sunday. |
| `--dry-run` | Preview what would be done without writing any files. |
| `--verbose`, `-v` | Enable verbose output for debugging. |
| `--quiet`, `-q` | Suppress non-essential output. |

### Input expectations

The script expects the exact column names used in the exported form:

* `Email`
* `Name`
* `Which assignment due date do you want to change?`
* `What would you like to new date to be change too?`

Rows missing any required field, or rows whose date cannot be parsed as
`MM/DD/YYYY`, are reported in `failures.csv` within the output directory.

### Output

For every assignment, a CSV named `<assignment>_extensions.csv` is created
(with a sanitized assignment name). Each row in these files includes the
student email, name, assignment, and the adjusted due date.

A `SUMMARY.txt` file is also produced. It contains:

* Total assignments processed
* Total unique students
* A per-assignment breakdown with file name, student count, and earliest/latest
  due dates
* File I/O issues and parsing errors, if any

Rejected rows (missing data, invalid dates, etc.) are written to
`failures.csv` when applicable.

### Exit Codes

The script returns appropriate exit codes for automation:

* `0` - Success
* `1` - Error (missing file, invalid data, I/O errors, etc.)

## API Usage

The module exposes a clean public API via `__all__` for programmatic use:

```python
from process_extensions import (
    # Data classes
    ExtensionRecord,
    ParseError,
    TableData,
    ColumnConfig,

    # Core functions
    parse_date,
    process_extension_data,
    deduplicate_records,
    adjust_dates,

    # Output functions
    create_output_files,
    generate_summary,
)

# Process data with custom column names
custom_cols = ColumnConfig(
    email="StudentEmail",
    name="StudentName",
    assignment="Assignment",
    date="DueDate",
)
records, errors, table = process_extension_data(lines, columns=custom_cols)
```

## Testing

Run the automated tests with:

```bash
pytest
```

The test suite includes 35+ tests covering:
* Date parsing and adjustment
* Record deduplication
* CSV/TSV format detection
* BOM (byte order mark) handling
* Dry-run mode
* CLI argument handling
* Error collection and reporting
