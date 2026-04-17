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

## Deliverables
This is what you have to produce:
1. URL of a accessible GIT repository with files described below. If you use a private GIT repository, you must share access to the teacher and assistant.
  - dataset_gen.py (as described in §4.1);
  - upload.py / download.py
  - bench.py (produces results.csv)
  - analysis.ipynb (interactive Python Notebook). The notebook must be functional and runnable (i.e., we must be able to import the .ipynyb without your support, and execute it). The results produced by the notebook should be self-explanatory.
  - README.md, which must describe, at the very least: how to run, what are the dependencies, endpoint config, how to reproduce the results.
2. Report (4-6 pages) including:
  - Your chosen variant and work methodology. The report must clearly indicate how tasks have been split between the team members, who did what. Ideally, we might want to see exactly in which git commit(s) each task has been implemented. Specifically, in the gist history, we should see different commits coming from different team members.
  - Experimental results on S/M/L (plots + tables)
  - Interpretation and concrete recommendations
  - Limitations and threats to validity
3. Video-recording of demo (2-3 minutes). In case of networking issues during the demo day, we would still like to see you demo your project. Prepare a video recording, and save the file in the same Git repository used for the rest of the deliverables/files.
4. During the demo (8-10 minutes including questions):
  - Show a reproducible run (or a subset) and the key plot/result
  - Answer questions on design trade-offs and reliability

The exact schedule of the demos for all projects will be announced at a later stage.

The URL of the GIT repository, the report, and the video-recording must be available and submitted via Moodle before: 21st of May 2026 11:59PM. We will put online the required Moodle box to do so in the next few days
