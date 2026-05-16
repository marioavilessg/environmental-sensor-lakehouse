import sys
from pathlib import Path
from pathlib import PurePosixPath

sys.path.append(str(Path(__file__).resolve().parents[1]))

from common.iot_common import hdfs_bronze_path, parse_args, raw_batch_file, run_cmd


def main() -> None:
    args = parse_args("Carga el lote raw en Bronze HDFS sin transformarlo.")
    raw = raw_batch_file(args.date, args.device_id)
    if not raw.exists():
        raise FileNotFoundError(f"Raw batch not found: {raw}")

    bronze = hdfs_bronze_path(args.date, args.device_id)
    target = str(PurePosixPath(bronze) / raw.name)
    run_cmd(["hdfs", "dfs", "-mkdir", "-p", bronze])
    run_cmd(["hdfs", "dfs", "-put", "-f", str(raw), target])
    run_cmd(["hdfs", "dfs", "-ls", bronze])
    print(f"Bronze raw file loaded to hdfs://namenode:9000{target}")


if __name__ == "__main__":
    main()
