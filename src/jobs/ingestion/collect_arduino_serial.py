import argparse
import csv
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import DEVICE_ID, REQUIRED_COLUMNS, inbox_file, raw_batch_file


ARDUINO_COLUMNS = ["device_id", "temperature", "humidity", "battery", "status"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Lee datos CSV desde Arduino por puerto serie y los guarda con el esquema raw IoT."
    )
    parser.add_argument("--port", help="Puerto serie del Arduino. Ejemplo: COM3, COM4 o /dev/ttyUSB0.")
    parser.add_argument("--list-ports", action="store_true", help="Muestra los puertos serie disponibles y termina.")
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--device-id", default=DEVICE_ID)
    parser.add_argument("--battery", default="100.00")
    parser.add_argument("--status", default="OK")
    parser.add_argument("--max-records", type=int, default=0, help="0 significa capturar hasta Ctrl+C.")
    parser.add_argument("--timeout", type=float, default=2.0, help="Timeout de lectura serie en segundos.")
    parser.add_argument(
        "--output",
        choices=["inbox", "raw", "both"],
        default="both",
        help="Destino del CSV. 'both' deja datos listos para inspeccion y para el pipeline.",
    )
    parser.add_argument("--source", default="arduino_serial")
    return parser.parse_args()


def append_csv_row(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    must_write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=REQUIRED_COLUMNS, extrasaction="ignore")
        if must_write_header:
            writer.writeheader()
        writer.writerow(row)


def extract_number(text: str) -> str:
    match = re.search(r"[-+]?\d+(?:[.,]\d+)?", text)
    if not match:
        raise ValueError(f"No se encontro ningun numero en: {text}")
    return match.group(0).replace(",", ".")


def clean_serial_text(text: str) -> str:
    return "".join(ch for ch in text.replace("\ufffd", "") if ch.isprintable()).strip()


def parse_arduino_line(line: str) -> dict | None:
    clean = clean_serial_text(line)
    if not clean:
        return None
    if clean.lower() == ",".join(ARDUINO_COLUMNS):
        return None

    values = next(csv.reader([clean]))
    if len(values) != len(ARDUINO_COLUMNS):
        raise ValueError(f"Se esperaban {len(ARDUINO_COLUMNS)} campos y llegaron {len(values)}: {clean}")

    return dict(zip(ARDUINO_COLUMNS, values))


class ArduinoLineParser:
    def __init__(self, device_id: str, battery: str, status: str) -> None:
        self.device_id = device_id
        self.battery = battery
        self.status = status
        self.pending_temperature: str | None = None

    def parse(self, line: str) -> dict | None:
        clean = clean_serial_text(line)
        if not clean:
            return None
        if set(clean) == {"-"}:
            return None

        values = next(csv.reader([clean]))
        if len(values) == len(ARDUINO_COLUMNS):
            if clean.lower() == ",".join(ARDUINO_COLUMNS):
                return None
            return dict(zip(ARDUINO_COLUMNS, values))

        lower = clean.lower()
        if "temperatura" in lower or "mperatura" in lower:
            self.pending_temperature = extract_number(clean)
            return None

        if "humedad" in lower or "umedad" in lower:
            if self.pending_temperature is None:
                raise ValueError(f"Humedad recibida sin temperatura previa: {clean}")
            row = {
                "device_id": self.device_id,
                "temperature": self.pending_temperature,
                "humidity": extract_number(clean),
                "battery": self.battery,
                "status": self.status,
            }
            self.pending_temperature = None
            return row

        raise ValueError(
            "Formato no reconocido. Usa CSV 'device_id,temperature,humidity,battery,status' "
            f"o lineas 'Temperatura:'/'Humedad:'. Recibido: {clean}"
        )


def build_event(raw: dict, run_date: str, source: str) -> dict:
    timestamp = datetime.now().replace(microsecond=0).isoformat()
    event_suffix = datetime.now().strftime("%H%M%S%f")
    return {
        "event_id": f"evt_{run_date.replace('-', '')}_{event_suffix}",
        "device_id": raw["device_id"],
        "timestamp": timestamp,
        "temperature": raw["temperature"],
        "humidity": raw["humidity"],
        "battery": raw["battery"],
        "status": raw["status"],
        "source": source,
    }


def output_paths(run_date: str, device_id: str, output: str) -> list[Path]:
    paths = []
    if output in {"inbox", "both"}:
        paths.append(inbox_file(run_date))
    if output in {"raw", "both"}:
        paths.append(raw_batch_file(run_date, device_id))
    return paths


def main() -> None:
    args = parse_args()
    try:
        import serial
        from serial.tools import list_ports
    except ImportError as exc:
        raise SystemExit("Falta pyserial. Instala con: python -m pip install pyserial") from exc

    if args.list_ports:
        ports = list(list_ports.comports())
        if not ports:
            print("No se han encontrado puertos serie.")
            return
        for port in ports:
            print(f"{port.device}: {port.description}")
        return

    if not args.port:
        raise SystemExit("Indica --port COMx o usa --list-ports para ver los puertos disponibles.")

    print(f"Leyendo Arduino en {args.port} a {args.baudrate} baudios. Pulsa Ctrl+C para parar.")
    stored = 0
    line_parser = ArduinoLineParser(args.device_id, args.battery, args.status)

    try:
        with serial.Serial(args.port, args.baudrate, timeout=args.timeout) as ser:
            time.sleep(2)
            ser.reset_input_buffer()

            while args.max_records <= 0 or stored < args.max_records:
                line = ser.readline().decode("utf-8", errors="replace")
                if not line:
                    continue

                try:
                    parsed = line_parser.parse(line)
                    if parsed is None:
                        continue
                    event = build_event(parsed, args.date, args.source)
                except ValueError as exc:
                    print(f"Linea ignorada: {exc}")
                    continue

                for path in output_paths(args.date, event["device_id"], args.output):
                    append_csv_row(path, event)

                stored += 1
                print(
                    f"{stored:04d} -> {event['timestamp']} "
                    f"{event['device_id']} temp={event['temperature']} "
                    f"hum={event['humidity']} status={event['status']}"
                )
    except KeyboardInterrupt:
        print("\nCaptura detenida por el usuario.")
    except serial.SerialException as exc:
        raise SystemExit(
            f"No se pudo abrir el puerto {args.port}: {exc}\n\n"
            "Comprueba esto:\n"
            "- Cierra el Monitor Serie o Plotter Serie del Arduino IDE.\n"
            "- Cierra cualquier otra terminal/script que este usando ese COM.\n"
            "- Desconecta y conecta de nuevo el Arduino.\n"
            "- Ejecuta de nuevo: python src/jobs/ingestion/collect_arduino_serial.py --list-ports\n"
            "- Si el puerto cambio, usa el nuevo COM en --port."
        ) from exc

    print(f"Registros guardados: {stored}")


if __name__ == "__main__":
    main()
