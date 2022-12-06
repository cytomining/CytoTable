# Data Architecture

Documentation covering data architecture for pyctyominer-transform.

## Sources

Data sources for pyctyominer-transform are measurement data created from other cell biology image analysis tools.

See below for a brief overview of these sources and data types.

- [CellProfiler](https://github.com/CellProfiler/CellProfiler) generates [CSV](https://en.wikipedia.org/wiki/Comma-separated_values) and [SQLite](https://www.sqlite.org/) databases data.
- [Cytominer-database](https://github.com/cytomining/cytominer-database) generates [SQLite](https://www.sqlite.org/) databases (which includes table data based on CellProfiler CSV's mentioned above).
- [DeepProfiler](https://github.com/cytomining/DeepProfiler) generates [NPZ](https://numpy.org/doc/stable/reference/routines.io.html?highlight=npz%20format#numpy-binary-files-npy-npz) data.

## Data structure

### Compartments and Images

Data are organized into tables of generally two categories:

- __Images__: Image tables generally include data about the image itself, including metadata.
- __Compartments__: Compartment data tables (such as Cytoplasm, Cells, Nuclei, Actin, or others) includes measurements specific to an aspect or part of cells found within an image.

### Identifying or Key Values

Identifying or key values for image and compartment tables may include the following:

- __ImageNumber__: Provides specificity on what image is being referenced (there may be many).
- __ObjectNumber__: Provides specificity for a specific compartment object within an ImageNumber.
- __Parent_Cells__: Provides a related Cell compartment ObjectNumber.
- __Parent_Nuclei__: Provides a related Nuclei compartment ObjectNumber.

### Image Data Relationships

```{mermaid}
erDiagram
    Image ||--o{ Cytoplasm : includes
    Image ||--o{ Cells : includes
    Image ||--o{ Nuclei : includes
    Image ||--o{ Others : includes
```

The above diagram shows an example of image data relationships found within the data that is used by pycytominer-transform.
Namely: Each image may include zero or many compartment objects (Cytoplasm, Cells, Nuclei, etc)objects.
