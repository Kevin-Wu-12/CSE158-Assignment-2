import gzip, ast, pandas as pd
from multiprocessing import Pool, cpu_count

def parse_line(line):
    try:
        return ast.literal_eval(line)
    except:
        return None

def load_ultrafast_parallel(path, workers=None):
    # Automatically detect CPU cores
    if workers is None:
        workers = cpu_count()  # use all CPUs available

    print("Reading file...")
    with gzip.open(path, 'rt', encoding='utf-8') as f:
        lines = f.read().splitlines()

    print(f"Parsing {len(lines):,} lines using {workers} workers...")

    with Pool(processes=workers) as p:
        parsed = p.map(parse_line, lines, chunksize=5000)

    parsed = [x for x in parsed if x is not None]
    df = pd.DataFrame(parsed)
    print("Done.")
    return df

if __name__ == "__main__":
    items = load_ultrafast_parallel("data/australian_users_items.json.gz")
    reviews = load_ultrafast_parallel("data/australian_user_reviews.json.gz")

    items.to_parquet("items.parquet")
    reviews.to_parquet("reviews.parquet")

    print("Saved to Parquet!")
