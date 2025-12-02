# Tutorial: CellProfiler SQLite on S3 to Parquet

A narrative, start-to-finish walkthrough for image analysts who want a working Parquet export from a CellProfiler SQLite file stored in the cloud.

## What you will accomplish

- Pull a CellProfiler SQLite file directly from S3 (unsigned/public) and convert each compartment table to Parquet.
- Keep a persistent local cache so the download is reused and avoids “file vanished” errors on temp disks.
- Verify the outputs quickly (file names and row counts) without needing to understand the internals.

```{admonition} If your data looks like this, change...
- Local SQLite instead of S3: set `source_path` to the local `.sqlite` file; remove `no_sign_request`; keep `local_cache_dir`.
- Only certain compartments: add `targets=["cells", "nuclei"]` (case-insensitive).
- Memory constrained: lower `chunk_size` (e.g., 10000) and ensure `CACHE_DIR` has space.
```

## Setup (copy-paste)

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install cytotable
```

## Inputs and outputs

- **Input:** A single-plate CellProfiler SQLite file from the open Cell Painting Gallery
  `s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/BR00126114.sqlite`
  No credentials are required (`no_sign_request=True`).
- **Output:** Four Parquet files (Image, Cells, Cytoplasm, Nuclei) in `./outputs/br00126114`.

## Before you start

- Install Cytotable (and DuckDB is bundled):
  `pip install cytotable`
- Make sure you have enough local disk space (~1–2 GB) for the cached SQLite and Parquet outputs.
- If you prefer to download the file first, you can also `aws s3 cp` the same path locally, then set `source_path` to the local file and drop `no_sign_request`.

## Step 1: define your paths

```bash
export SOURCE_PATH="s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/BR00126114.sqlite"
export DEST_PATH="./outputs/br00126114"
export CACHE_DIR="./sqlite_s3_cache"
mkdir -p "$DEST_PATH" "$CACHE_DIR"
```

## Step 2: run the conversion (minimal Python)

```python
import os
import cytotable

# If you used the bash exports above:
SOURCE_PATH = os.environ["SOURCE_PATH"]
DEST_PATH = os.environ["DEST_PATH"]
CACHE_DIR = os.environ["CACHE_DIR"]

# (Alternatively, set them directly as strings in Python.)

result = cytotable.convert(
    source_path=SOURCE_PATH,
    source_datatype="sqlite",
    dest_path=DEST_PATH,
    dest_datatype="parquet",
    # Preset matches common CellProfiler SQLite layout from the Cell Painting Gallery
    preset="cellprofiler_sqlite_cpg0016_jump",
    # Use a cache directory you control so the downloaded SQLite is reusable
    local_cache_dir=CACHE_DIR,
    # This dataset is public; unsigned requests avoid credential prompts
    no_sign_request=True,
    # Reasonable chunking for large tables; adjust up/down if you hit memory limits
    chunk_size=30000,
)

print(result)
```

Why these flags matter (in plain language):

- `local_cache_dir`: keeps the downloaded SQLite file somewhere predictable so DuckDB can open it reliably.
- `preset`: selects the right table names and page keys for this dataset.
- `chunk_size`: processes data in pieces so you don’t need excessive RAM.
- `no_sign_request`: needed because the sample bucket is public and unsigned.

## Step 3: check that the outputs look right

You should see four Parquet files in the destination directory:

```bash
ls "$DEST_PATH"
# Image.parquet  Cells.parquet  Cytoplasm.parquet  Nuclei.parquet
```

## What success looks like

- A stable local cache of the SQLite file remains in `CACHE_DIR` (useful for repeated runs).
- Four Parquet files exist in `DEST_PATH` and can be read by DuckDB/Pandas/PyArrow.
- No temporary-file or “unable to open database file” errors occur during the run.
