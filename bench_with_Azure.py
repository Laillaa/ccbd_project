'''
Upload/download = moving data
Query = processing data
Listing = finding data
'''
import os
import time
import argparse
import pandas as pd
import pyarrow.dataset as ds
from azure.storage.blob import BlobServiceClient


sizes = ["S", "M", "L"]
codecs = ["snappy", "zstd", "gzip", "None"]

def create_azure_client(connection_string):
    return BlobServiceClient.from_connection_string(connection_string)

def upload_file_azure(blob_service, local_path, container, blob_name):
    start = time.time()

    blob_client = blob_service.get_blob_client(container=container, blob=blob_name)

    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)

    end = time.time()

    file_size = os.path.getsize(local_path)
    throughput = file_size / (1024 * 1024) / (end - start)

    return file_size, end - start, throughput

def download_file_azure(blob_service, container, blob_name, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    start = time.time()

    blob_client = blob_service.get_blob_client(container=container,blob=blob_name)

    with open(local_path, "wb") as f:
        f.write(blob_client.download_blob().readall())

    end = time.time()

    file_size = os.path.getsize(local_path)
    throughput = file_size / (1024 * 1024) / (end - start)

    return file_size, end - start, throughput

def get_local_size(path):
    return os.path.getsize(path)

# Measures the time required to retrieve metadata (object names and sizes) 
# for all files under a given prefix in object storage.
def list_prefix_azure(blob_service, container, prefix):
    container_client = blob_service.get_container_client(container)

    start = time.time()
    blobs = list(container_client.list_blobs(name_starts_with=prefix))
    end = time.time()

    object_count = len(blobs)
    total_bytes = sum(blob.size for blob in blobs)

    return end - start, object_count, total_bytes

def run_query(path, region=None, event_type=None, start_date=None, end_date=None, min_value=None, max_value=None):

    dataset = ds.dataset(path, format="parquet")

    query_filter = None

    # creating a query filter that is more dynamic
    if region is not None:
        condition = ds.field("region") == region
        query_filter = condition if query_filter is None else query_filter & condition

    if event_type is not None:
        condition = ds.field("event_type") == event_type
        query_filter = condition if query_filter is None else query_filter & condition

    if start_date is not None:
        condition = ds.field("ts") >= pd.Timestamp(start_date)
        query_filter = condition if query_filter is None else query_filter & condition

    if end_date is not None:
        condition = ds.field("ts") < pd.Timestamp(end_date)
        query_filter = condition if query_filter is None else query_filter & condition

    if min_value is not None:
        condition = ds.field("value") >= min_value
        query_filter = condition if query_filter is None else query_filter & condition

    if max_value is not None:
        condition = ds.field("value") <= max_value
        query_filter = condition if query_filter is None else query_filter & condition


    # Start measuring the time to scan + filtering + grouping
    start = time.time()

    # applying the filter on the dataset
    table = dataset.to_table(filter=query_filter)

    df = table.to_pandas()
    # grouping by event type and aggregate
    result = df.groupby("event_type")["value"].agg(["count", "mean"])

    end = time.time()

    return end - start, result

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", required=True)
    parser.add_argument("--connection-string", required=True)
    parser.add_argument("--local-root", default="data")
    parser.add_argument("--download-root", default="downloads")
    parser.add_argument("--output", default="results_with_Azure.csv")

    parser.add_argument("--region", default=None)
    parser.add_argument("--event-type", default=None)
    parser.add_argument("--start-date", default=None)
    parser.add_argument("--end-date", default=None)
    parser.add_argument("--min-value", type=float, default=None)
    parser.add_argument("--max-value", type=float, default=None)


    args = parser.parse_args()

    blob_service = create_azure_client(args.connection_string)

    rows = []

    for size in sizes:
        for codec in codecs:
            print(f"Benchmarking size={size}, codec={codec}")

            local_path = f"{args.local_root}/size_{size}/transactions_{codec}.parquet"
            blob_name = f"curated/size_{size}/{codec}/transactions_{codec}.parquet"
            prefix = f"curated/size_{size}/{codec}/"
            downloaded_path = f"{args.download_root}/size_{size}/transactions_{codec}.parquet"

            stored_bytes = get_local_size(local_path)

            _, upload_time, upload_throughput = upload_file_azure(blob_service, local_path, args.container,blob_name) 
            listing_time, object_count, azure_bytes = list_prefix_azure(blob_service, args.container, prefix)
            _, download_time, download_throughput = download_file_azure(blob_service, args.container, blob_name, downloaded_path)
            
            query_time, result = run_query(
                downloaded_path, 
                region=args.region, 
                event_type=args.event_type, 
                start_date=args.start_date,
                end_date=args.end_date,
                min_value=args.min_value,
                max_value=args.max_value,)

            rows.append({
                "size": size,
                "codec": codec,
                "stored_bytes_local": stored_bytes,
                "stored_bytes_azure": azure_bytes,
                "object_count": object_count,
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