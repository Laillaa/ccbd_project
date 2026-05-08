import os
import time
import argparse
import boto3
from botocore.config import Config


def create_s3_client(endpoint_url=None):
    # creates a connection to S3
    return boto3.client("s3", endpoint_url=endpoint_url, config=Config(signature_version="s3v4"))


def upload_file(s3, local_path, bucket, key):
    size_bytes = os.path.getsize(local_path)

    start = time.time()
    s3.upload_file(local_path, bucket, key)
    end = time.time()

    duration = end - start
    # throughput = transfer size (converted bytes into megabytes) / duration (seconds)
    mb = size_bytes / (1024 * 1024)
    if duration > 0:
        throughput = mb / duration 
    else:
        throughput = 0

    return size_bytes, duration, throughput


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--s3-prefix", default="curated")
    # we don't use AWS (Amazon web service) S3 so we need to explicitly connect to a server
    parser.add_argument("--endpoint-url", default=None)

    args = parser.parse_args()

    sizes = ["S", "M", "L"]
    codecs = ["snappy", "zstd", "gzip", "None"]

    s3 = create_s3_client(args.endpoint_url)

    for size in sizes:
        for codec in codecs:
            local_path = f"data/size_{size}/transactions_{codec}.parquet"

            # key = curated/<dataset_id>....
            key = f"curated/size_{size}/{codec}/transactions_{codec}.parquet"

            # key_raw = raw/<dataset_id>/.... if we need to store the csv
            # key_raw = f"raw/size_{size}/{codec}/transactions_{codec}.csv"

            size_bytes, duration, throughput = upload_file(s3, local_path, args.bucket, key)

            print(f"Transactions files of size: {size} with codec: {codec}")
            print(f" - Uploaded {size_bytes} bytes in {duration:.2f}s ")
            print(f"({throughput:.2f} MB/s)")
            print("---------------------------------------")

if __name__ == "__main__":
    main()