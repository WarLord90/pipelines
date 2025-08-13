import csv, time, logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def extract(src: Path) -> list[dict]:
    logging.info("Extract")
    rows = []
    with src.open(newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows

def transform(rows: list[dict]) -> list[dict]:
    logging.info("Transform")
    out = []
    for r in rows:
        r = {k: v.strip() if isinstance(v, str) else v for k, v in r.items()}
        if r.get("status") == "active":
            out.append(r)
    return out

def load(rows: list[dict], dst: Path) -> None:
    logging.info("Load")
    if not rows: 
        return
    headers = rows[0].keys()
    with dst.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        w.writerows(rows)

def run(src="data/input.csv", dst="data/output.csv", retries=3):
    attempt = 0
    while True:
        try:
            data = extract(Path(src))
            data = transform(data)
            load(data, Path(dst))
            logging.info("Done")
            return
        except Exception as e:
            attempt += 1
            logging.warning(f"Fail {attempt}: {e}")
            if attempt >= retries:
                raise
            time.sleep(2)

if __name__ == "__main__":
    run()
