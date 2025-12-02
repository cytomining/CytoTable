# Tutorial: Merging multiple plates with Tablenumber

Goal: combine multiple CellProfiler SQLite exports (plates) into a single Parquet output while preserving plate identity via `TableNumber`.

## What you will accomplish

- Point Cytotable at a folder of multiple plate exports.
- Add `TableNumber` so downstream analyses can distinguish rows from different plates.
- Verify merged outputs.

## Setup (copy-paste)

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install cytotable
```

## Inputs and outputs

- **Input:** A folder of CellProfiler SQLite files (example structure):  
  `data/plates/PlateA.sqlite`  
  `data/plates/PlateB.sqlite`
- **Output:** Parquet files (Image/Cells/Cytoplasm/Nuclei) under `./outputs/multi_plate`, with a `Metadata_TableNumber` column indicating plate.

## Step 1: define your paths

```bash
export SOURCE_PATH="./data/plates"
export DEST_PATH="./outputs/multi_plate"
export CACHE_DIR="./sqlite_cache"
mkdir -p "$DEST_PATH" "$CACHE_DIR"
```

## Step 2: run the conversion with tablenumber

```python
import os
import cytotable

source_path = os.environ["SOURCE_PATH"]
dest_path = os.environ["DEST_PATH"]
cache_dir = os.environ["CACHE_DIR"]

result = cytotable.convert(
    source_path=source_path,
    source_datatype="sqlite",
    dest_path=dest_path,
    dest_datatype="parquet",
    preset="cellprofiler_sqlite",
    local_cache_dir=cache_dir,
    add_tablenumber=True,  # key for multi-plate merges
    chunk_size=30000,
)

print(result)
```

Why this matters:

- `add_tablenumber=True` adds `Metadata_TableNumber` so you can filter/group by plate later.
- Pointing `source_path` to a folder makes Cytotable glob multiple plates.
- `local_cache_dir` keeps each plate cached locally for reliable DuckDB access.

## Step 3: validate plate separation

You should see one Parquet per compartment (`Cells`, `Cytoplasm`, `Nuclei`, etc.) in `DEST_PATH`. Opening a file with Pandas or PyArrow should show `Metadata_TableNumber` present and non-zero rows. If you processed multiple plates, expect multiple distinct values in that column.

## Scenario callouts (“if your data looks like this...”)

- **Local SQLite files:** set `source_path` to the folder of local `.sqlite` files; remove `no_sign_request`.
- **Only certain compartments:** pass `targets=["cells", "nuclei"]` to limit tables.
- **Memory constrained:** lower `chunk_size` (e.g., 10000) and ensure `CACHE_DIR` is on a disk with enough space for all plates + parquet output.
