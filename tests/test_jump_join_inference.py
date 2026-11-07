"""Offline join-inference checks against column subsets of public JUMP exports."""

# pylint: disable=unused-argument

import csv
import hashlib
import json
import pathlib
import shutil
from typing import Optional

import pytest
from pyarrow import parquet

from cytotable.convert import convert

DATA = pathlib.Path(__file__).parent / "data" / "jump_join_subsets"
GROUPS = ("jump-source1", "jump-source3", "jump-source4", "jump-scope", "jump-pilot")
FEATURES = ("AreaShape_Area", "AreaShape_Center_X")


def _expected_rows(source: pathlib.Path) -> dict:
    """Look up parents directly in the original CSV values, independently of SQL."""
    expected = {}
    for image_path in sorted(source.rglob("Image.csv")):
        tables = {}
        for name in ("Image", "Cells", "Nuclei", "Cytoplasm"):
            with image_path.with_name(f"{name}.csv").open(encoding="utf-8") as stream:
                tables[name] = list(csv.DictReader(stream))
        images = {int(row["ImageNumber"]): row for row in tables["Image"]}
        parents = {}
        for name in ("Cells", "Nuclei"):
            parents[name] = {
                (int(row["ImageNumber"]), int(row["ObjectNumber"])): row
                for row in tables[name]
            }
            assert len(parents[name]) == len(tables[name])
        for cytoplasm in tables["Cytoplasm"]:
            number = int(cytoplasm["ImageNumber"])
            image = images[number]
            key = (
                image["Metadata_Plate"],
                image["Metadata_Well"],
                int(image["Metadata_Site"]),
                number,
                int(cytoplasm["ObjectNumber"]),
            )
            assert key not in expected
            values: dict[str, Optional[float]] = {
                f"Cytoplasm_{feature}": float(cytoplasm[feature])
                for feature in FEATURES
            }
            for name in ("Cells", "Nuclei"):
                parent = parents[name].get((number, int(cytoplasm[f"Parent_{name}"])))
                values.update(
                    {
                        f"{name}_{feature}": float(parent[feature]) if parent else None
                        for feature in FEATURES
                    }
                )
            expected[key] = values
    return expected


def _assert_inferred_rows(source: pathlib.Path, dest: pathlib.Path) -> None:
    """Require every original cytoplasm row and its actual parent measurements."""
    expected = _expected_rows(source)
    result = parquet.read_table(
        convert(
            source_path=str(source),
            dest_path=str(dest),
            dest_datatype="parquet",
            source_datatype="csv",
            preset=None,
            joins="infer",
            metadata=["image"],
            compartments=["cells", "nuclei", "cytoplasm"],
            chunk_size=100,
            add_tablenumber=True,
        )
    )
    key_columns = [
        "Image_Metadata_Plate",
        "Image_Metadata_Well",
        "Image_Metadata_Site",
        "Metadata_ImageNumber",
        "Metadata_ObjectNumber",
    ]
    feature_columns = [
        f"{name}_{feature}"
        for name in ("Cytoplasm", "Cells", "Nuclei")
        for feature in FEATURES
    ]
    seen = set()
    for row in result.select(key_columns + feature_columns).to_pylist():
        key = tuple(row[name] for name in key_columns)
        assert key not in seen
        seen.add(key)
        for name, value in expected[key].items():
            if value is None:
                assert row[name] is None
            else:
                assert row[name] == pytest.approx(value, rel=1e-12, abs=1e-12)
    assert seen == set(expected)
    assert result.num_rows == len(expected)


@pytest.mark.parametrize("group", GROUPS)
def test_jump_subset_inference(
    load_parsl_default: None, tmp_path: pathlib.Path, group: str
):
    """Check different JUMP laboratories, a pilot plate, and a microscope variant."""
    _assert_inferred_rows(DATA / group, tmp_path / "inferred.parquet")


def test_jump_subset_inference_across_sources(
    load_parsl_default: None, tmp_path: pathlib.Path
):
    """Keep real overlapping image/object numbers separated across five plates."""
    source = tmp_path / "source"
    for group in GROUPS:
        shutil.copytree(DATA / group, source / group)
    expected = _expected_rows(source)
    # Confirm this actually exercises collisions, unlike disjoint image ranges.
    unscoped_keys = [(key[3], key[4]) for key in expected]
    assert len(set(unscoped_keys)) < len(unscoped_keys)
    assert len({key[0] for key in expected}) == len(GROUPS)
    _assert_inferred_rows(source, tmp_path / "inferred.parquet")


def test_jump_subset_provenance():
    """Check retained bytes and row counts against the recorded fixture manifest."""
    provenance = json.loads((DATA / "provenance.json").read_text(encoding="utf-8"))
    recorded = set()
    for entry in provenance["files"]:
        path = DATA / entry["path"]
        assert entry["path"] not in recorded
        recorded.add(entry["path"])
        assert hashlib.sha256(path.read_bytes()).hexdigest() == entry["fixture_sha256"]
        with path.open(encoding="utf-8") as stream:
            reader = csv.DictReader(stream)
            assert reader.fieldnames == entry["columns"]
            assert sum(1 for _ in reader) == entry["rows"]
    assert recorded == {str(path.relative_to(DATA)) for path in DATA.rglob("*.csv")}
