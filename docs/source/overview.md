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

#### Data Source Locations in the Cloud

Cloud-based data sources used through CytoTable leverage [cloudpathlib](https://cloudpathlib.drivendata.org/~latest/).
Examples of paths to these sources leverage specialized prefixes and may be found below.

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

CSV files stored on cloud services are read directly from the cloud service.

__SQLite Databases (.sqlite)__:

SQLite databases stored on cloud services are downloaded locally before other CytoTable work is performed.
This is done to account for differences in how [SQLite's virtual file system (VFS)](https://www.sqlite.org/vfs.html) operates in context with cloud service object storage.
Large SQLite files stored in the cloud may benefit from explicit local cache specification through a special keyword argument (`**kwarg`) passed through CytoTable to [`cloudpathlib`: `local_cache_dir`](https://cloudpathlib.drivendata.org/~latest/caching/#keeping-the-cache-around).
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
