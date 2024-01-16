# Overview

This page provides a brief overview of CytoTable topics.
For a brief introduction on how to use CytoTable, please see the [tutorial](tutorial.md) page.

## Presets and Manual Overrides

```{eval-rst}
Various preset configurations are available for use within CytoTable which affect how data are read and produced under :data:`presets.config <cytotable.presets.config>`.
These presets are intended to assist with common data source expectations.
By default, CytoTable will use the "cellprofiler_csv" preset.
Please note that these presets may not capture all possible outcomes.
Use manual overrides within :mod:`convert() <cytotable.convert.convert>` as needed.
```

## Data Sources

```{mermaid}
flowchart LR
    images[("Image\nfile(s)")]:::outlined --> image-tools[Image Analysis Tools]:::outlined
    image-tools --> measurements[("Measurement\nfile(s)")]:::green
    measurements --> CytoTable:::green

    classDef outlined fill:#fff,stroke:#333
    classDef green fill:#97F0B4,stroke:#333
```

Data sources for CytoTable are measurement data created from other cell biology image analysis tools.
These measurement data are the focus of the data source content which follows.

### Data Source Locations

```{eval-rst}
Data sources may be provided to CytoTable using local filepaths or remote object-storage filepaths (for example, AWS S3, GCP Cloud Storage, Azure Storage).
We use `cloudpathlib <https://cloudpathlib.drivendata.org/~latest/>`_  under the hood to reference files in a unified way, whether they're local or remote.
```

#### Cloud Data Sources

CytoTable uses [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/) to access cloud-based data sources.
CytoTable supports:

- [Amazon S3](https://en.wikipedia.org/wiki/Amazon_S3): `s3://bucket_name/object_name`
- [Google Cloud Storage](https://en.wikipedia.org/wiki/Google_Cloud_Storage): `gc://bucket_name/object_name`
- [Azure Blob Storage](https://en.wikipedia.org/wiki/Microsoft_Azure#Storage_services): `az://container_name/blob_name`

##### Cloud Service Configuration and Authentication

```{eval-rst}
Remote object storage paths which require authentication or other specialized configuration may use cloudpathlib client arguments (`S3Client <https://cloudpathlib.drivendata.org/~latest/api-reference/s3client/>`_, `AzureBlobClient <https://cloudpathlib.drivendata.org/~latest/api-reference/azblobclient/>`_, `GSClient <https://cloudpathlib.drivendata.org/~latest/api-reference/gsclient/>`_) and :code:`convert(..., **kwargs)` (:mod:`convert() <cytotable.convert.convert>`).

For example, remote AWS S3 paths which are public-facing and do not require authentication (like, or similar to, :code:`aws s3 ... --no-sign-request`) may be used via :code:`convert(..., no_sign_request=True)` (:mod:`convert() <cytotable.convert.convert>`).
```

Each cloud service provider may have different requirements for authentication (there is no fully unified API for these).
Please see the [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/) client documentation for more information on which arguments may be used for configuration with specific cloud providers (for example, [`S3Client`](https://cloudpathlib.drivendata.org/stable/api-reference/s3client/), [`GSClient`](https://cloudpathlib.drivendata.org/stable/api-reference/gsclient/), or [`AzureBlobClient`](https://cloudpathlib.drivendata.org/stable/api-reference/azblobclient/)).

##### Cloud Service File Type Parsing Differences

Data sources retrieved from cloud services are not all treated the same due to technical constraints.
See below for a description of how each file type is treated for a better understanding of expectations.

__Comma-separated values (.csv)__:

CytoTable reads cloud-based CSV files directly.

__SQLite Databases (.sqlite)__:

CytoTable downloads cloud-based SQLite databases locally before other CytoTable processing.
This is necessary to account for differences in how [SQLite's virtual file system (VFS)](https://www.sqlite.org/vfs.html) operates in context with cloud service object storage.

Note: Large SQLite files stored in the cloud may benefit from explicit local cache specification through a special keyword argument (`**kwarg`) passed through CytoTable to `cloudpathlib` called `local_cache_dir`. See [the cloudpathlib documentation on caching](https://cloudpathlib.drivendata.org/~latest/caching/#keeping-the-cache-around).
This argument helps ensure constraints surrounding temporary local file storage locations do not impede the ability to download or work with the data (for example, file size limitations and periodic deletions outside of CytoTable might be encountered within default OS temporary file storage locations).

```{eval-rst}
A quick example of how this argument is used: :code:`convert(..., local_cache_dir="non_temporary_directory", ...)` (:mod:`convert() <cytotable.convert.convert>`).
```

Future work to enable direct SQLite data access from cloud locations for CytoTable will be documented within GitHub issue [CytoTable/#70](https://github.com/cytomining/CytoTable/issues/70).

### Data Source Types

Data source compatibility for CytoTable is focused (but not explicitly limited to) the following.

#### CellProfiler Data Sources

- __Comma-separated values (.csv)__: "A comma-separated values (CSV) file is a delimited text file that uses a comma to separate values." ([reference](https://en.wikipedia.org/wiki/Comma-separated_values))
  CellProfiler CSV data sources generally follow the format provided as output by [CellProfiler ExportToSpreadsheet](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/ExportToSpreadsheet.html).

```{eval-rst}
  * **Manual specification:** CSV data source types may be manually specified by using :code:`convert(..., source_datatype="csv", ...)` (:mod:`convert() <cytotable.convert.convert>`).
  * **Preset specification:** CSV data sources from CellProfiler may use the configuration preset :code:`convert(..., preset="cellprofiler_csv", ...)` (:mod:`convert() <cytotable.convert.convert>`).
```

- __SQLite Databases (.sqlite)__: "SQLite database files are commonly used as containers to transfer rich content between systems and as a long-term archival format for data." ([reference](https://sqlite.org/index.html))
  CellProfiler SQLite database sources may follow a format provided as output by [CellProfiler ExportToDatabase](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/ExportToDatabase.html) or [cytominer-database](https://github.com/cytomining/cytominer-database).

```{eval-rst}
  * **Manual specification:** SQLite data source types may be manually specified by using :code:`convert(..., source_datatype="sqlite", ...)` (:mod:`convert() <cytotable.convert.convert>`).
  * **Preset specification:** SQLite data sources from CellProfiler may use the configuration preset :code:`convert(..., preset="cellprofiler_sqlite", ...)` (:mod:`convert() <cytotable.convert.convert>`).
```

#### IN Carta Data Sources

- __Comma-separated values (.csv)__: [Molecular Devices IN Carta](https://www.moleculardevices.com/products/cellular-imaging-systems/high-content-analysis/in-carta-image-analysis-software) software provides output data in CSV format.

```{eval-rst}
  * **Manual specification:** CSV data source types may be manually specified by using :code:`convert(..., source_datatype="csv", ...)` (:mod:`convert() <cytotable.convert.convert>`).
  * **Preset specification:** CSV data sources from CellProfiler may use the configuration preset :code:`convert(..., preset="in-carta", ...)` (:mod:`convert() <cytotable.convert.convert>`).
```

## Data Destinations

### Data Destination Locations

```{eval-rst}
Converted data destinations are may be provided to CytoTable using only local filepaths (in contrast to data sources, which may also be remote).
Specify the converted data destination using the  :code:`convert(..., dest_path="<a local filepath>")` (:mod:`convert() <cytotable.convert.convert>`).
```

### Data Destination Types

- __Apache Parquet (.parquet)__: "Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.
  It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk." ([reference](https://parquet.apache.org/))

```{eval-rst}
  Parquet data destination type may be specified by using :code:`convert(..., dest_datatype="parquet", ...)` (:mod:`convert() <cytotable.convert.convert>`).
```

## Data Transformations

CytoTable performs various types of data transformations.
This section help define terminology and expectations surrounding the use of this terminology.
CytoTable might use one or all of these depending on user configuration.

### Data Chunking

<table>
<tr><th>Original</th><th>Changes</th></tr>
<tr>
<td>

"Data source"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td></tr>
<tr><td>2</td><td>b</td><td>0.02</td></tr>
</table>

</td>
<td>

"Chunk 1"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td></tr>

</table>

"Chunk 2"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>2</td><td>b</td><td>0.02</td></tr>
</table>

</td>
</tr>
</table>

_Example of data chunking performed on a simple table of data._

```{eval-rst}
Data chunking within CytoTable involves slicing data sources into "chunks" of rows which all contain the same columns and have a lower number of rows than the original data source.
CytoTable uses data chunking through the ``chunk_size`` argument value (:code:`convert(..., chunk_size=1000, ...)` (:mod:`convert() <cytotable.convert.convert>`)) to reduce the memory footprint of operations on subsets of data.
CytoTable may be used to create chunked data output by disabling concatenation and joins, e.g. :code:`convert(..., concat=False,join=False, ...)` (:mod:`convert() <cytotable.convert.convert>`).
Parquet "datasets" are an abstraction which may be used to read CytoTable output data chunks which are not concatenated or joined (for example, see `PyArrow documentation <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetDataset.html>`_ or `Pandas documentation <https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html>`_ on using source paths which are directories).
```

### Data Concatenations

<table>
<tr><th>Original</th><th>Changes</th></tr>
<tr>
<td>

"Chunk 1"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td></tr>

</table>

"Chunk 2"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>2</td><td>b</td><td>0.02</td></tr>
</table>

</td>
<td>

"Concatenated data"

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td></tr>
<tr><td>2</td><td>b</td><td>0.02</td></tr>
</table>

</td>
</tr>
</table>

_Example of data concatenation performed on simple tables of similar data "chunks"._

Data concatenation within CytoTable involves bringing two or more data "chunks" with the same columns together as a unified dataset.
Just as chunking slices data apart, concatenation brings them together.
Data concatenation within CytoTable typically occurs using a [ParquetWriter](https://arrow.apache.org/docs/python/generated/pyarrow.parquet.ParquetWriter.html) to assist with composing a single file from many individual files.

### Data Joins

<table>
<tr><th>Original</th><th>Changes</th></tr>
<tr>
<td>

"Table 1" (notice __Col_C__)

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td></tr>

</table>

"Table 2" (notice __Col_Z__)

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_Z</th></tr>
<tr><td>1</td><td>a</td><td>2024-01-01</td></tr>
</table>

</td>
<td>

"Joined data" (as Table 1 <a href="https://en.wikipedia.org/wiki/Join_(SQL)#Left_outer_join">left-joined</a> with Table 2)

<table>
<tr><th>Col_A</th><th>Col_B</th><th>Col_C</th><th>Col_Z</th></tr>
<tr><td>1</td><td>a</td><td>0.01</td><td>2024-01-01</td></tr>
</table>

</td>
</tr>
<tr >
<td colspan="2" style="text-align:center;font-weight:bold;">
Join Specification in SQL
</td>
</tr>
<tr >
<td colspan="2">

```sql
SELECT *
FROM Table_1
LEFT JOIN Table_2 ON
Table_1.Col_A = Table_2.Col_A;
```

</td>
</tr>
</table>

_Example of a data join performed on simple example tables._

```{eval-rst}
Data joins within CytoTable involve bringing one or more data sources together with differing columns as a new dataset.
The word "join" here is interpreted through `SQL-based terminology on joins <https://en.wikipedia.org/wiki/Join_(SQL)>`_.
Joins may be specified in CytoTable using `DuckDB-style SQL <https://duckdb.org/docs/sql/introduction.html>`_ through :code:`convert(..., joins="SELECT * FROM ... JOIN ...", ...)` (:mod:`convert() <cytotable.convert.convert>`).
Also see CytoTable's presets found here: :data:`presets.config <cytotable.presets.config>` or via `GitHub source code for presets.config <https://github.com/cytomining/CytoTable/blob/main/cytotable/presets.py>`_.
```

Note: data software outside of CytoTable sometimes makes use of the term "merge" to describe capabilities which are similar to join (for ex. [`pandas.DataFrame.merge`](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.merge.html).
Within CytoTable, we opt to describe these operations with "join" to avoid confusion with software development alongside the technologies used (for example, [DuckDB SQL](https://duckdb.org/docs/archive/0.9.2/sql/introduction) includes no `MERGE` keyword).
