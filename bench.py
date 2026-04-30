import os
import time
import argparse
import pandas as pd
import pyarrow.dataset as ds
import boto3
from botocore.config import Config

from upload import upload_file
from download import download_file

sizes = ["S", "M"]
codecs = ["snappy", "zstd", "gzip"]

def create_s3_client(endpoint_url=None):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        config=Config(signature_version="s3v4"),
    )

def get_local_size(path):
    return os.path.getsize(path)

def list_prefix(s3, bucket, prefix):

    start = time.time()
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    end = time.time()

    object_count = response.get("KeyCount", 0)
    total_bytes = sum(obj["Size"] for obj in response.get("Contents", []))

    return end - start, object_count, total_bytes

def run_query(path):
    dataset = ds.dataset(path, format="parquet")

    # creating a query that filters by region and time range
    query_filter = (
        (ds.field("region") == "CH") &
        (ds.field("ts") >= pd.Timestamp("2025-06-01")) &
        (ds.field("ts") < pd.Timestamp("2025-09-01"))
    )

    # Start measuring the time to scan + filtering + grouping
    start = time.time()

    # applying the filter on the dataset
    table = dataset.to_table(filter=query_filter)

    df = table.to_pandas()
    # grouping by event type and aaggregate
    result = df.groupby("event_type")["value"].agg(["count", "mean"])

    end = time.time()

    return end - start, result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument("--local-root", default="data")
    parser.add_argument("--download-root", default="downloads")
    parser.add_argument("--output", default="results.csv")

    args = parser.parse_args()

    s3 = create_s3_client(args.endpoint_url)

    rows = []

    for size in sizes:
        for codec in codecs:
            print(f"Benchmarking size={size}, codec={codec}")

            local_path = f"{args.local_root}/size_{size}/transactions_{codec}.parquet"
            key = f"curated/size_{size}/{codec}/transactions_{codec}.parquet"
            prefix = f"curated/size_{size}/{codec}/"
            downloaded_path = f"{args.download_root}/size_{size}/transactions_{codec}.parquet"

            stored_bytes = get_local_size(local_path)

            _ , upload_time, upload_throughput = upload_file(s3, local_path, args.bucket, key)
            listing_time, object_count, s3_bytes = list_prefix(s3, args.bucket, prefix)
            _ , download_time, download_throughput = download_file(s3, args.bucket, key, downloaded_path)
            
            query_time, result = run_query(downloaded_path)

            rows.append({
                "size": size,
                "codec": codec,
                "stored_bytes_local": stored_bytes,
                "stored_bytes_s3": s3_bytes,
                "upload_time_seconds": upload_time,
                "upload_throughput": upload_throughput,
                "listing_time_seconds": listing_time,
                "download_time_seconds": download_time,
                "download_throughput": download_throughput,
                "query_time_seconds": query_time})

            print("---------------------------------------")
            print(f"Upload throughput: {upload_throughput:.2f} MB/s")
            print(f"Download throughput: {download_throughput:.2f} MB/s")
            print(f"Listing time: {listing_time:.4f}s")
            print(f"Query time: {query_time:.4f}s")
            print("---------------------------------------")

    df = pd.DataFrame(rows)
    df.to_csv(args.output, index=False)
    print(f"Results written to {args.output}")

if __name__ == "__main__":
    main()