# Tutorial: NPZ embeddings + metadata to Parquet

A start-to-finish walkthrough for turning NPZ files (for example, DeepProfiler outputs) plus metadata into Parquet.
This uses a small example bundled in the repo.

## What you will accomplish

- Read NPZ feature files and matching metadata from disk.
- Combine them into Parquet with a preset that aligns common keys.
- Validate the output shape and schema.

```{admonition} If your data looks like this, change...
- NPZ in a different folder: point `source_path` there; keep `preset="deepprofiler"`.
- Memory constrained: add `chunk_size=10000` to the convert call.
- `.npy` files or plain CSV feature tables: this tutorial/preset does not cover them; use the CellProfiler CSV/SQLite flows instead.
```

## Setup (copy-paste)

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install cytotable
```

## Inputs and outputs

- **Input:** Example NPZ + metadata in this repo: `tests/data/deepprofiler/pycytominer_example`
- **Output:** A Parquet file under `./outputs/deepprofiler_example.parquet`

## Step 1: define your paths

```bash
export SOURCE_PATH="tests/data/deepprofiler/pycytominer_example"
export DEST_PATH="./outputs/deepprofiler_example.parquet"
mkdir -p "$DEST_PATH"
```

## Step 2: run the conversion

```python
import os
import cytotable

source_path = os.environ["SOURCE_PATH"]
dest_path = os.environ["DEST_PATH"]

result = cytotable.convert(
    source_path=source_path,
    source_datatype="npz",
    dest_path=dest_path,
    dest_datatype="parquet",
    preset="deepprofiler",
    concat=True,
    join=False,
)

print(result)
```

Notes (why these flags matter):

- `preset="deepprofiler"` aligns NPZ feature arrays with metadata columns.
- `concat=True` merges multiple NPZ shards.
- `join=False` writes per-table Parquet files (the preset produces `all_files.npz` as the logical table).

## Step 3: validate the output

You should see `deepprofiler_example.parquet` in `DEST_PATH`.
Opening it with Pandas or PyArrow should show non-zero rows and both feature (`efficientnet_*`) and metadata columns.

## What success looks like

- A Parquet file `deepprofiler_example.parquet` exists in `DEST_PATH`.
- DuckDB/Pandas can read the file; row count is non-zero.
- Feature columns (for example, `efficientnet_*`) and metadata columns (plate/well/site) both appear.
