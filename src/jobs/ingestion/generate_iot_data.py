import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import DEVICE_ID, REQUIRED_COLUMNS, parse_args, raw_batch_file, write_csv_rows


def generate_rows(run_date: str, device_id: str, total: int = 240) -> list[dict]:
    rng = random.Random(20260515)
    start = datetime.fromisoformat(f"{run_date}T08:00:00")
    rows = []
    for i in range(total):
        ts = start + timedelta(minutes=2 * i)
        temp = 23.0 + 4.5 * rng.random() + rng.uniform(-1.0, 1.0)
        humidity = 48.0 + 18.0 * rng.random() + rng.uniform(-2.0, 2.0)
        battery = max(0.0, 96.0 - i * 0.035 + rng.uniform(-0.3, 0.3))
        rows.append(
            {
                "event_id": f"evt_{run_date.replace('-', '')}_{i:06d}",
                "device_id": device_id,
                "timestamp": ts.isoformat(),
                "temperature": round(temp, 2),
                "humidity": round(humidity, 2),
                "battery": round(battery, 2),
                "status": "OK" if temp < 29.5 and battery > 20 else "WARN",
                "source": "simulator",
            }
        )

    # Errores controlados para demostrar reglas de calidad.
    rows[12]["temperature"] = 150
    rows[27]["humidity"] = -10
    rows[39]["battery"] = 130
    rows[55]["timestamp"] = ""
    rows[72]["event_id"] = rows[71]["event_id"]
    rows[90]["temperature"] = None
    rows[118]["timestamp"] = "2026-99-99T25:61:00"
    rows[160]["status"] = "UNKNOWN"
    return rows


def main() -> None:
    args = parse_args("Genera datos IoT simulados compatibles con la ESP32.")
    out = raw_batch_file(args.date, args.device_id)
    rows = generate_rows(args.date, args.device_id or DEVICE_ID)
    write_csv_rows(out, rows, REQUIRED_COLUMNS)
    print(f"Generated {len(rows)} simulator CSV rows at {out}")


if __name__ == "__main__":
    main()
