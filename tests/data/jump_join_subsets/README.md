# JUMP join-inference subsets

Small, offline CSV fixtures derived from public
[Cell Painting Gallery](https://github.com/broadinstitute/cellpainting-gallery)
exports. Dataset descriptions and associated publications are listed in the
[JUMP dataset catalog](https://github.com/jump-cellpainting/datasets).

## Selection and provenance

These fixtures retain every row from seven selected site/well export groups.
Only columns were removed: the image/object identifiers, every original
`Parent_*` column, two measurements per compartment, and plate/well/site
metadata remain. No identifiers or measurement values were edited or
renumbered. The 28 retained CSVs total 492,041 bytes; the full-width source
CSVs total 274,659,483 bytes. No microscopy images or full plates were downloaded.

`provenance.json` records the exact public source URLs, original file sizes and
SHA-256 checksums, retained columns and row counts, and fixture checksums.
The catalog identifies the contributing datasets as `cpg0016-jump`,
`cpg0000-jump-pilot`, and `cpg0002-jump-scope`.

## Full-width verification

The original, unprojected CSV subsets were also tested locally on November 7,
2026\. See `full_width_validation.json` for the results, environment, source
selection, and tested code revision.

| Subset                | Plate          | Images/sites | Cytoplasm rows | Output columns |
| --------------------- | -------------- | -----------: | -------------: | -------------: |
| JUMP source 1         | UL000109       |            2 |            427 |          5,886 |
| JUMP source 3         | BR5867a3       |            9 |          1,359 |          4,910 |
| JUMP source 4         | BR00117035     |            2 |            292 |          7,876 |
| JUMP-scope, Nikon 10x | BR00117060a10x |            1 |          2,266 |          3,811 |
| JUMP-pilot            | BR00116992     |            1 |            172 |          7,385 |

Each subset was converted with `preset=None`, `joins="infer"`, generated table
numbers, and `chunk_size=100` to exercise multiple pages. An independent,
explicit SQL control used cytoplasm-to-cell/nucleus parent references and
image/table keys in `LEFT JOIN ... ON` clauses, not inference-generated SQL.
All output column names and values matched after sorting on
`TableNumber`, `ImageNumber`, and `ObjectNumber`. Paired NaN values were treated
as equal without conflating them with nulls.

A separate Python dictionary lookup from the original CSVs checked all 4,516
cytoplasm identities and 13,548 compartment-area values, independently of both
SQL paths. There were no missing, extra, or duplicated output rows. The
full-width comparison covered 21,391,750 output values across 15 images/sites.

## Offline regression tests

Run from the repository root:

```sh
uv run --frozen --group dev pytest tests/test_jump_join_inference.py -q
```

The tests require no downloads. They verify each dataset separately, combine
all five plates to exercise real overlapping image/object identifiers, check
area and center-X values using independent raw-CSV parent lookups for every
row, and validate the retained fixture checksums and row counts.

These results support the selected CSV layouts, not every JUMP dataset or
condition. No additional native SQLite exports were validated. Column-subset
regressions are intentionally small and do not replace full-width checks.
