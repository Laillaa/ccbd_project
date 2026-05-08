# Create a synthetic event dataset with a fixed schema:
# ts (timestamp), user_id (int), region (string), event_type (string), value (float)
# Optional variable: payload 
# Topic: Financial transactions

import argparse
import numpy as np
import pandas as pd
import os

import pyarrow as pa
import pyarrow.parquet as pq

seed = 42
SIZE_TO_ROWS = {"S": 5_000_000, "M": 25_000_000, "L": 100_000_000,}


def generate_transactions(n_rows: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    regions = ["CH", "FR", "DE", "IT", "ES", "UK", "MK"]

    event_types = ["payment", "withdrawal", "transfer"]

    start_ts = pd.Timestamp("2025-01-01").value // 10**9
    end_ts = pd.Timestamp("2025-12-31").value // 10**9

    ts = pd.to_datetime(rng.integers(start_ts, end_ts, size=n_rows), unit="s")
    user_id = rng.integers(1, 500_001, size=n_rows)
    region = rng.choice(regions, size=n_rows, p=[0.20, 0.15, 0.15, 0.10, 0.10, 0.20, 0.10])
    event_type = rng.choice(event_types, size=n_rows, p=[0.70, 0.15, 0.15])

    value = np.empty(n_rows, dtype=float)
    value[event_type == "payment"] = rng.lognormal(3.2, 0.8, size=(event_type == "payment").sum())
    value[event_type == "withdrawal"] = rng.lognormal(4.0, 0.5, size=(event_type == "withdrawal").sum())
    value[event_type == "transfer"] = rng.lognormal(4.5, 0.9, size=(event_type == "transfer").sum())
    value = np.round(value, 2)

    merchant_info = {
        "Amazon": "shopping",
        "Migros": "groceries",
        "Coop": "groceries",
        "Netflix": "subscription",
        "Spotify": "subscription",
        "Apple": "electronics",
        "Google": "electronics",
        "SBB": "transport",
        "Shell": "fuel",
        "IKEA": "shopping"
    }

    region_to_currency = {
        "CH": "CHF",
        "FR": "EUR",
        "DE": "EUR",
        "IT": "EUR",
        "ES": "EUR",
        "UK": "GBP",
        "MK": "MKD"
    }

    merchants = list(merchant_info.keys())
    merchant = rng.choice(merchants, size=n_rows)
    category = np.array([merchant_info[m] for m in merchant])
    currency = np.array([region_to_currency[r] for r in region])

    payload = np.char.add("merchant=", merchant)
    payload = np.char.add(payload, ";category=")
    payload = np.char.add(payload, category)
    payload = np.char.add(payload, ";currency=")
    payload = np.char.add(payload, currency)

    return pd.DataFrame({
        "ts": ts,
        "user_id": user_id,
        "region": region,
        "event_type": event_type,
        "value": value,
        "payload": payload
    })

def write_transactions_chunked(output, n_rows, codec, chunk_rows, seed=42):

    output_dir = os.path.dirname(output)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    writer = None
    rows_written = 0
    chunk_id = 0

    try:
        while rows_written < n_rows:

            current_chunk_rows = min(chunk_rows, n_rows - rows_written)

            df = generate_transactions(n_rows=current_chunk_rows, seed=seed + chunk_id)

            table = pa.Table.from_pandas(df, preserve_index=False)

            if writer is None:
                writer = pq.ParquetWriter(output, table.schema, compression=codec)

            writer.write_table(table)

            rows_written += current_chunk_rows
            chunk_id += 1

            print(f"Wrote chunk {chunk_id} "
                f"({rows_written:,}/{n_rows:,} rows)"
            )

    finally:
        if writer is not None:
            writer.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], required=True)
    parser.add_argument("--codec", choices=["snappy", "zstd", "gzip", "None"], required=True)
    parser.add_argument("--chunk-rows", type=int, default=1_000_000)
    args = parser.parse_args()

    n_rows = SIZE_TO_ROWS[args.size]
    output_dir = os.path.join("data", f"size_{args.size}", f"transactions_{args.codec}.parquet")

    write_transactions_chunked(output=output_dir, n_rows=n_rows, codec=args.codec, chunk_rows=args.chunk_rows, seed=seed)

if __name__ == "__main__":
    main()