# Create a synthetic event dataset with a fixed schema:
# ts (timestamp), user_id (int), region (string), event_type (string), value (float)
# Optional variable: payload 
# Topic: Financial transactions

import argparse
import numpy as np
import pandas as pd

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", choices=["S", "M", "L"], required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--codec", choices=["snappy", "zstd", "gzip"], required=True)
    args = parser.parse_args()

    n_rows = SIZE_TO_ROWS[args.size]
    df = generate_transactions(n_rows=n_rows, seed=seed)
    df.to_parquet(args.output, compression=args.codec, index=False)

    # Save as CSV (same name, different extension)
    # csv_output = args.output.replace(".parquet", ".csv")
    # df.to_csv(csv_output, index=False)

if __name__ == "__main__":
    main()