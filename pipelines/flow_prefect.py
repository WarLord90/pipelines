from pathlib import Path
from prefect import flow, task
from pipelines.etl import extract, transform, load

@task(retries=3, retry_delay_seconds=5)
def t_extract(p):
    # p llega como str; conviértelo a Path
    return extract(Path(p))

@task
def t_transform(rows):
    return transform(rows)

@task
def t_load(rows, p):
    # p llega como str; conviértelo a Path
    load(rows, Path(p))

@flow(log_prints=True)
def etl_flow(src="data/input.csv", dst="data/output_prefect.csv"):
    rows = t_extract.submit(src)
    clean = t_transform.submit(rows)
    t_load.submit(clean, dst)

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--src", default="data/input.csv")
    p.add_argument("--dst", default="data/output_prefect.csv")
    args = p.parse_args()
    etl_flow(src=args.src, dst=args.dst)
