# Compression codecs comparison

## Goal 
Evaluate how different Parquet compression codecs impact storage size, transfer time, and query time. You will use the same dataset and only change the codec.

## What you must implement
- Write the curated dataset in Parquet with at least two codecs: snappy and zstd. (Optional: gzip for comparison.)
- Keep layout and file sizing policy constant across codecs.
- Run the fixed analytics query using pyarrow.dataset.

## Experiments (run for S/M/L)
- Total stored bytes per codec.
- Upload/download throughput per codec.
- Query runtime per codec.

## Expected discussion
Explain the CPU vs IO trade-off: stronger compression reduces bytes transferred but may increase CPU cost. Conclude with a recommendation (e.g., “zstd for storage efficiency when CPU is available; snappy for speed”) based on your measurements.
