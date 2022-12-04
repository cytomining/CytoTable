
# Overview

This page provides a brief overview of pycytominer-transform topics.

## Data Sources

### Data Source Locations

```{eval-rst}
Data sources may be provided to pycytominer-transform using local filepaths or remote object-storage filepaths (for example, AWS S3, GCP Cloud Storage, Azure Storage).
We use `cloudpathlib <https://cloudpathlib.drivendata.org/~latest/>`_  under the hood to reference files in a unified way, whether they're local or remote.

Remote object storage paths which require authentication or other specialized configuration may use cloudpathlib client arguments (`S3Client <https://cloudpathlib.drivendata.org/~latest/api-reference/s3client/>`_, `AzureBlobClient <https://cloudpathlib.drivendata.org/~latest/api-reference/azblobclient/>`_, `GSClient <https://cloudpathlib.drivendata.org/~latest/api-reference/gsclient/>`_) and :code:`convert(..., **kwargs)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
For example, remote AWS S3 paths which are public-facing and do not require authentication (like, or similar to, :code:`aws s3 ... --no-sign-request`) may be used via :code:`convert(..., no_sign_request=True)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

### Data Source Types

- __Comma-separated values (.csv)__: "A comma-separated values (CSV) file is a delimited text file that uses a comma to separate values." ([reference](https://en.wikipedia.org/wiki/Comma-separated_values))
  CSV data sources generally follow the format provided as output by [CellProfiler ExportToSpreadsheet](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/ExportToSpreadsheet.html).

```{eval-rst}
  * **Manual specification:** CSV data source types may be manually specified by using :code:`convert(..., source_datatype="csv", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
  * **Preset specification:** CSV data sources from CellProfiler may use the configuration preset :code:`convert(..., preset="cellprofiler_csv", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

- __SQLite Databases (.sqlite)__: "SQLite database files are commonly used as containers to transfer rich content between systems and as a long-term archival format for data." ([reference](https://sqlite.org/index.html))
  SQLite database sources may follow a format provided as output by [CellProfiler ExportToDatabase](https://cellprofiler-manual.s3.amazonaws.com/CPmanual/ExportToDatabase.html) or [cytominer-database](https://github.com/cytomining/cytominer-database).

```{eval-rst}
  * **Manual specification:** SQLite data source types may be manually specified by using :code:`convert(..., source_datatype="sqlite", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
  * **Preset specification:** SQLite data sources from CellProfiler may use the configuration preset :code:`convert(..., preset="cellprofiler_sqlite", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

## Data Destinations

### Data Destination Locations

```{eval-rst}
Converted data destinations are may be provided to pycytominer-transform using only local filepaths (in contrast to data sources, which may also be remote).
Specify the converted data destination using the  :code:`convert(..., dest_path="<a local filepath>")` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```

### Data Destination Types

- __Apache Parquet (.parquet)__: "Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.
  It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk." ([reference](https://parquet.apache.org/))

```{eval-rst}
  Parquet data destination type may be specified by using :code:`convert(..., dest_datatype="parquet", ...)` (:mod:`convert() <pycytominer_transform.convert.convert>`).
```
