# Software Engineering Guide

This page is for engineers and power users who want to tune CytoTable beyond the narrative tutorials. It focuses on performance, reliability, and integration patterns.

## Performance and scaling

- **Chunk size (`chunk_size`)**: Larger chunks reduce overhead but increase peak memory. Start at 30k (default in examples), adjust down for memory-constrained environments, up for fast disks/large RAM.
- **Threads (DuckDB)**: We set `PRAGMA threads` based on `cytotable.constants.MAX_THREADS`. Override via env var `CYTOTABLE_MAX_THREADS` to align with container CPU limits.
- **I/O locality**: For remote SQLite/NPZ, always set `local_cache_dir` to a stable, non-tmpfs path. Reuse the cache across runs to avoid redundant downloads.

Example: tuned convert with explicit threads and chunk size

```python
import os
import cytotable

os.environ["CYTOTABLE_MAX_THREADS"] = "4"

cytotable.convert(
    source_path="s3://my-bucket/plate.sqlite",
    source_datatype="sqlite",
    dest_path="./out/plate",
    dest_datatype="parquet",
    preset="cellprofiler_sqlite",
    local_cache_dir="./cache/sqlite",
    chunk_size=50000,  # larger chunks, more RAM, faster on beefy nodes
    no_sign_request=True,
)
```

## Cloud paths and auth

- **Unsigned/public S3**: use `no_sign_request=True`. This keeps DuckDB + cloudpathlib using unsigned clients consistently.
- **Signed/private S3**: rely on ambient AWS creds or pass `profile_name`, `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token`. These kwargs flow into cloudpathlib’s client via `_build_path`.
- **GCS/Azure**: supported through cloudpathlib; pass provider-specific kwargs the same way you would construct the CloudPath client.

Signed S3 example with a specific profile

```python
import cytotable

cytotable.convert(
    source_path="s3://my-private-bucket/exports/plate.sqlite",
    source_datatype="sqlite",
    dest_path="./out/private-plate",
    dest_datatype="parquet",
    preset="cellprofiler_sqlite",
    local_cache_dir="./cache/private",
    profile_name="science-prod",
)
```

## Data layout and presets

- Prefer presets when available (for example, `cellprofiler_sqlite_cpg0016_jump`, `cellprofiler_csv`) because they set table names and page keys. For custom layouts, pass `targets=[...]` and `page_keys={...}` to `convert`.
- Multi-plate runs: point `source_path` to a parent directory; CytoTable will glob and group per-table. Keep per-run `dest_path` directories to avoid mixing outputs.

Custom layout example with explicit targets and page keys

```python
import cytotable

cytotable.convert(
    source_path="/data/plates/",
    source_datatype="sqlite",
    dest_path="./out/plates",
    dest_datatype="parquet",
    targets=["cells", "cytoplasm", "nuclei"],  # which tables to include
    page_keys={"cells": "ImageNumber", "cytoplasm": "ImageNumber", "nuclei": "ImageNumber"},
    add_tablenumber=True,
    chunk_size=20000,
)
```

## Reliability tips

- **Stable cache**: If you see “unable to open database file” on cloud SQLite, ensure `local_cache_dir` is set and writable. DuckDB reads from the cached path.
- **Disk space**: Parquet output size ~10–30% of CSV; SQLite is denser. Ensure the cache volume can hold both the source and outputs simultaneously.
- **Restartability**: `dest_path` is overwritten per run; use unique destination directories for incremental runs to avoid partial-output confusion.

## Testing and CI entry points

- Unit tests live under `tests/`; sample datasets are in `tests/data/`. Add targeted fixtures when introducing new formats/presets.
- For quick smoke tests, run `python -m pytest tests/test_convert_threaded.py -k convert` and a docs build `sphinx-build docs/source docs/build` to ensure examples render.
- Keep new presets documented in `docs/source/overview.md` and mention edge cases (auth, cache, table naming).

Smoke-test commands

```bash
python -m pytest tests/test_convert_threaded.py -k convert
sphinx-build docs/source docs/build
```

## Embedding CytoTable in pipelines

- **Python API**: `cytotable.convert(...)` is synchronous; wrap in your workflow engine (Airflow, Prefect, Nextflow via Python) as a task step.
- **CLI wrapper**: not bundled; if you add one, surface the same flags as `convert` and mirror logging levels.
- **Logging**: uses the standard logging system. Set `CYTOTABLE_LOG_LEVEL=INFO` (or `DEBUG`) in container/CI to capture more detail during runs.

Simple function you can call from any orchestrator (Airflow task, Nextflow Python, shell)

```python
import cytotable

def run_cytotable(source, dest, cache):
    return cytotable.convert(
        source_path=source,
        source_datatype="sqlite",
        dest_path=dest,
        dest_datatype="parquet",
        preset="cellprofiler_sqlite",
        local_cache_dir=cache,
        chunk_size=30000,
    )

if __name__ == "__main__":
    run_cytotable(
        "s3://my-bucket/plate.sqlite",
        "./out/plate",
        "./cache/sqlite",
    )
```
