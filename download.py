import os
import time
import argparse
import boto3
from botocore.config import Config


def create_s3_client(endpoint_url=None):
    return boto3.client("s3", endpoint_url=endpoint_url, config=Config(signature_version="s3v4"))


def download_file(s3, bucket, key, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    start = time.time()
    s3.download_file(bucket, key, local_path)
    end = time.time()

    size_bytes = os.path.getsize(local_path)
    duration = end - start
    mb = size_bytes / (1024 * 1024)
    if duration > 0:
        throughput = mb / duration 
    else:
        throughput = 0

    return size_bytes, duration, throughput


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--endpoint-url", default=None)
    parser.add_argument("--download-root", default="downloads")

    args = parser.parse_args()

    sizes = ["S", "M", "L"]
    codecs = ["snappy", "zstd", "gzip", "None"]

    s3 = create_s3_client(args.endpoint_url)

    for size in sizes:
        for codec in codecs:
            key = f"curated/size_{size}/{codec}/transactions_{codec}.parquet"
            local_path = f"{args.download_root}/size_{size}/transactions_{codec}.parquet"

            size_bytes, duration, throughput = download_file(s3, args.bucket, key, local_path)

            print("---------------------------------------")
            print(f"Transactions file of size: {size} with codec: {codec}")
            print(f" - Downloaded {size_bytes} bytes in {duration:.2f}s")
            print(f" - Throughput:{throughput:.2f} MB/s")
            print("---------------------------------------")


if __name__ == "__main__":
    main()