import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import REQUIRED_COLUMNS, inbox_file, parse_args, raw_batch_file, read_csv_rows, write_csv_rows
from ingestion.generate_iot_data import generate_rows


def main() -> None:
    args = parse_args("Prepara el lote raw: ESP32 real si existe, simulador si no.", include_min_records=True)
    inbox = inbox_file(args.date)
    out = raw_batch_file(args.date, args.device_id)
    real_rows = read_csv_rows(inbox)

    if len(real_rows) >= args.min_real_records:
        write_csv_rows(out, real_rows, REQUIRED_COLUMNS)
        source = "esp32"
        count = len(real_rows)
    else:
        rows = generate_rows(args.date, args.device_id)
        write_csv_rows(out, rows, REQUIRED_COLUMNS)
        source = "simulator"
        count = len(rows)

    print(
        f"Prepared raw batch at {out} with {count} records "
        f"(source={source}, real_records_available={len(real_rows)})"
    )


if __name__ == "__main__":
    main()
