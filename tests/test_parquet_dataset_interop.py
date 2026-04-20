from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq

from cityjson_test_interop.fixtures import (
    building_geometry_fixture,
    mixed_cityobjects_fixture,
    write_parquet_dataset,
)
from cityjson_test_interop.rust_tools import read_dataset_json, write_dataset_fixture


def test_pyarrow_reads_rust_native_parquet_dataset(tmp_path: Path) -> None:
    dataset_path = tmp_path / "rust-dataset"
    write_dataset_fixture(dataset_path)

    cityobjects = pq.read_table(
        dataset_path / "tables" / "cityobjects.parquet",
        columns=["cityobject_id", "object_type"],
    )
    geometries = pq.read_table(dataset_path / "tables" / "geometries.parquet")

    assert cityobjects.column("object_type").to_pylist() == ["Building", "BuildingPart"]
    assert cityobjects.column("cityobject_id").to_pylist() == [
        "rust-building-1",
        "rust-part-1",
    ]
    assert geometries.column("geometry_type").to_pylist() == ["MultiSurface"]


def test_pyarrow_full_scans_rust_cityobjects_dataset(tmp_path: Path) -> None:
    dataset_path = tmp_path / "rust-dataset-full-scan"
    write_dataset_fixture(dataset_path)

    table = pq.read_table(dataset_path / "tables" / "cityobjects.parquet")

    assert table.column("geographical_extent").to_pylist() == [None, None]


def test_rust_reads_pyarrow_native_parquet_dataset(tmp_path: Path) -> None:
    fixture = mixed_cityobjects_fixture()
    dataset_path = tmp_path / "pyarrow-dataset"
    write_parquet_dataset(dataset_path, fixture)

    table = pq.read_table(dataset_path / "tables" / "cityobjects.parquet")
    model = read_dataset_json(dataset_path)

    assert table.column("geographical_extent").to_pylist() == [None, None, None]
    assert set(model["CityObjects"]) == {"building-1", "part-1", "road-1"}
    assert model["CityObjects"]["building-1"]["children"] == ["part-1"]
    assert model["CityObjects"]["part-1"]["parents"] == ["building-1"]
    assert model["CityObjects"]["building-1"]["attributes"]["name"] == "Main Building"


def test_rust_reads_pyarrow_geometry_native_parquet_dataset(tmp_path: Path) -> None:
    fixture = building_geometry_fixture()
    dataset_path = tmp_path / "pyarrow-geometry-dataset"
    write_parquet_dataset(dataset_path, fixture)

    model = read_dataset_json(dataset_path)

    building = model["CityObjects"]["building-geometry"]
    assert building["type"] == "Building"
    assert building["geometry"][0]["type"] == "MultiSurface"
    assert building["geometry"][0]["boundaries"] == [[[0, 1, 2, 3, 0]]]


def test_duckdb_filters_and_projects_cityobjects(tmp_path: Path) -> None:
    fixture = mixed_cityobjects_fixture()
    dataset_path = tmp_path / "duckdb-dataset"
    write_parquet_dataset(dataset_path, fixture)
    cityobjects_path = dataset_path / "tables" / "cityobjects.parquet"

    rows = duckdb.sql(
        """
        SELECT cityobject_id
        FROM read_parquet(?)
        WHERE object_type = 'Building'
        ORDER BY cityobject_id
        """,
        params=[str(cityobjects_path)],
    ).fetchall()
    explain = duckdb.sql(
        """
        EXPLAIN SELECT cityobject_id
        FROM read_parquet(?)
        WHERE object_type = 'Building'
        """,
        params=[str(cityobjects_path)],
    ).fetchall()
    explain_text = "\n".join(str(row) for row in explain)

    assert rows == [("building-1",)]
    assert "object_type" in explain_text
    assert "cityobject_id" in explain_text


def test_polars_lazily_filters_and_projects_cityobjects(tmp_path: Path) -> None:
    fixture = mixed_cityobjects_fixture()
    dataset_path = tmp_path / "polars-dataset"
    write_parquet_dataset(dataset_path, fixture)
    cityobjects_path = dataset_path / "tables" / "cityobjects.parquet"

    lazy_frame = (
        pl.scan_parquet(cityobjects_path)
        .filter(pl.col("object_type") == "Building")
        .select("cityobject_id")
    )
    explain = lazy_frame.explain()
    result = lazy_frame.collect()

    assert result.to_dict(as_series=False) == {"cityobject_id": ["building-1"]}
    assert "Parquet" in explain
    assert "cityobject_id" in explain
