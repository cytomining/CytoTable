# Tutorial: CellProfiler SQLite or CSV to Parquet

A start-to-finish walkthrough for image analysts who want a working Parquet export from CellProfiler outputs (SQLite or CSV), including public S3 and local data.

## What you will accomplish

- Convert CellProfiler outputs to Parquet with a preset that matches common table/column layouts.
- Handle both SQLite (typical Cell Painting Gallery exports) and CSV folder outputs.
- Keep a persistent local cache so downloads are reused and avoid “file vanished” errors on temp disks.
- Verify the outputs quickly (file names and row counts) without needing to understand the internals.

```{admonition} If your data looks like this, change...
- Local SQLite instead of S3: set `source_path` to the local `.sqlite` file; remove `no_sign_request`; keep `local_cache_dir`.
- CellProfiler CSV folders: point `source_path` to the folder that contains `Cells.csv`, `Cytoplasm.csv`, etc.; set `source_datatype="csv"` and `preset="cellprofiler_csv"`.
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

- **SQLite example (public S3):** `s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/BR00126114.sqlite`
  No credentials are required (`no_sign_request=True`).
- **CSV example (local folder):** `./tests/data/cellprofiler/ExampleHuman` which contains `Cells.csv`, `Cytoplasm.csv`, `Nuclei.csv`, etc.
- **Outputs:** Parquet files for each compartment (Image, Cells, Cytoplasm, Nuclei) in `./outputs/...`.

## Before you start

- Install Cytotable (and DuckDB is bundled):
  `pip install cytotable`
- Make sure you have enough local disk space (~1–2 GB) for the cached SQLite and Parquet outputs.
- If you prefer to download the file first, you can also `aws s3 cp` the same path locally, then set `source_path` to the local file and drop `no_sign_request`.

## Step 1: choose your input type

Pick one of the two setups below.

**SQLite from public S3 (Cell Painting Gallery)**

```bash
export SOURCE_PATH="s3://cellpainting-gallery/cpg0016-jump/source_4/workspace/backend/2021_08_23_Batch12/BR00126114/BR00126114.sqlite"
export SOURCE_DATATYPE="sqlite"
export PRESET="cellprofiler_sqlite_cpg0016_jump"
export DEST_PATH="./outputs/br00126114.parquet"
export CACHE_DIR="./sqlite_s3_cache"
mkdir -p "$(dirname "$DEST_PATH")" "$CACHE_DIR"
```

**CellProfiler CSV folder (local or mounted storage)**

```bash
export SOURCE_PATH="./tests/data/cellprofiler/ExampleHuman"
export SOURCE_DATATYPE="csv"
export PRESET="cellprofiler_csv"
export DEST_PATH="./outputs/examplehuman.parquet"
export CACHE_DIR="./csv_cache"
mkdir -p "$(dirname "$DEST_PATH")" "$CACHE_DIR"
```

## Step 2: run the conversion (minimal Python)

```python
import os
import cytotable

# If you used the bash exports above:
SOURCE_PATH = os.environ["SOURCE_PATH"]
SOURCE_DATATYPE = os.environ["SOURCE_DATATYPE"]
DEST_PATH = os.environ["DEST_PATH"]
PRESET = os.environ["PRESET"]
CACHE_DIR = os.environ["CACHE_DIR"]

# (Alternatively, set them directly as strings in Python.)

result = cytotable.convert(
    source_path=SOURCE_PATH,
    source_datatype=SOURCE_DATATYPE,
    dest_path=DEST_PATH,
    dest_datatype="parquet",
    preset=PRESET,
    local_cache_dir=CACHE_DIR,
    # For public S3 (SQLite or CSV) add:
    no_sign_request=True,
    # Reasonable chunking for large tables; adjust up/down if you hit memory limits
    chunk_size=30000,
)

print(result)
```

Why these flags matter (in plain language):

- `local_cache_dir`: keeps downloaded data somewhere predictable so DuckDB can open it reliably.
- `preset`: selects the right table names and page keys for this dataset (SQLite or CSV).
- `chunk_size`: processes data in pieces so you don’t need excessive RAM.
- `no_sign_request`: needed because the sample bucket is public and unsigned.

## Step 3: check that the outputs look right

You should see Parquet files in the destination directory.
If you set `join=True` (handy for the SQLite example), you get a single `. parquet` file containing all compartments.
If you set `join=False` (handy for CSV folders), you get separate Parquet files for each compartment.

```bash
ls "$DEST_PATH"
# SQLite example: br00126114.parquet
# CSV example: examplehuman.parquet/Cells.parquet (and Cytoplasm, Nuclei, Image)
```

## What success looks like

- A stable local cache of the SQLite file or CSV downloads remains in `CACHE_DIR` (useful for repeated runs).
- Parquet outputs exist in `DEST_PATH` and can be read by DuckDB/Pandas/PyArrow.
- No temporary-file or “unable to open database file” errors occur during the run.
