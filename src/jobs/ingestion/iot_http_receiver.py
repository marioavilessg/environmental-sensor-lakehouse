import csv
import json
import os
from datetime import date
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[3]))
HOST = os.getenv("IOT_RECEIVER_HOST", "0.0.0.0")
PORT = int(os.getenv("IOT_RECEIVER_PORT", "5050"))
CSV_COLUMNS = ["event_id", "device_id", "timestamp", "temperature", "humidity", "battery", "status", "source"]


def inbox_path() -> Path:
    run_date = date.today().isoformat()
    path = ROOT / "data" / "iot" / "inbox" / f"esp32_events_{run_date}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def parse_payload(body: str, content_type: str) -> dict:
    if "application/json" in content_type:
        payload = json.loads(body)
        if not isinstance(payload, dict):
            raise ValueError("JSON payload must be an object")
        return payload

    # CSV line without header:
    # event_id,device_id,timestamp,temperature,humidity,battery,status,source
    rows = list(csv.reader([body.strip()]))
    if not rows or len(rows[0]) != len(CSV_COLUMNS):
        raise ValueError("CSV payload must contain 8 comma-separated values")
    return dict(zip(CSV_COLUMNS, rows[0]))


def append_csv_row(path: Path, row: dict) -> None:
    must_write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        if must_write_header:
            writer.writeheader()
        writer.writerow(row)


class IotHandler(BaseHTTPRequestHandler):
    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/health":
            self._send(200, {"status": "ok"})
            return
        self._send(404, {"error": "not_found"})

    def do_POST(self) -> None:
        if not self.path.startswith("/iot/events"):
            self._send(404, {"error": "not_found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        try:
            payload = parse_payload(body, self.headers.get("Content-Type", ""))
        except Exception as exc:
            self._send(400, {"error": "invalid_payload", "detail": str(exc)})
            return

        append_csv_row(inbox_path(), payload)
        self._send(201, {"status": "stored"})

    def log_message(self, fmt: str, *args) -> None:
        print("%s - %s" % (self.address_string(), fmt % args))


def main() -> None:
    server = ThreadingHTTPServer((HOST, PORT), IotHandler)
    print(f"IoT HTTP receiver listening on http://{HOST}:{PORT}/iot/events")
    server.serve_forever()


if __name__ == "__main__":
    main()
