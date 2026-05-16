import csv
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import load_rules, now_utc, parse_args, raw_batch_file, report_dir, write_json


def main() -> None:
    args = parse_args("Valida formato inicial CSV y columnas obligatorias.")
    raw = raw_batch_file(args.date, args.device_id)
    rules = load_rules()
    required = rules["required_columns"]

    total = 0
    malformed_rows = 0
    missing_columns = 0
    rows_with_required_columns = 0

    if not raw.exists():
        raise FileNotFoundError(f"Raw batch not found: {raw}")

    with raw.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        header = reader.fieldnames or []
        missing_header = [col for col in required if col not in header]
        for line_no, row in enumerate(reader, start=2):
            total += 1
            if None in row:
                malformed_rows += 1
                print(f"Line {line_no}: malformed CSV row with extra values {row[None]}")
                continue
            missing = [col for col in required if col not in row]
            if missing:
                missing_columns += 1
                print(f"Line {line_no}: missing columns {missing}")
            else:
                rows_with_required_columns += 1

    report = {
        "run_date": args.date,
        "checked_at": now_utc(),
        "raw_file": str(raw),
        "total_lines": total,
        "missing_header_columns": missing_header,
        "malformed_rows": malformed_rows,
        "rows_with_required_columns": rows_with_required_columns,
        "rows_missing_columns": missing_columns,
        "required_columns": required,
    }
    out = report_dir(args.date) / "validate_raw_report.json"
    write_json(out, report)
    print(json.dumps(report, indent=2))

    if total == 0 or malformed_rows > 0 or missing_header:
        raise SystemExit("Raw validation failed: empty file, malformed CSV, or missing header columns.")


if __name__ == "__main__":
    main()
