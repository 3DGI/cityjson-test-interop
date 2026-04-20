from __future__ import annotations

from pathlib import Path

from cityjson_test_interop.arrow_stream import read_stream, write_stream
from cityjson_test_interop.fixtures import (
    building_geometry_fixture,
    minimal_fixture,
    mixed_cityobjects_fixture,
)
from cityjson_test_interop.rust_tools import read_arrow_json, write_arrow_fixture


def test_pyarrow_reads_rust_arrow_stream(tmp_path: Path) -> None:
    stream_path = tmp_path / "rust-fixture.cjarrow"
    write_arrow_fixture(stream_path)

    stream = read_stream(stream_path)

    assert stream.header["package_version"] == "cityjson-arrow.package.v3alpha3"
    assert [table.name for table in stream.tables] == [
        "metadata",
        "vertices",
        "geometry_boundaries",
        "geometries",
        "cityobjects",
        "cityobject_children",
    ]

    cityobjects = _table_by_name(stream, "cityobjects")
    assert cityobjects.column("object_type").to_pylist() == ["Building", "BuildingPart"]
    assert cityobjects.column("cityobject_id").to_pylist() == [
        "rust-building-1",
        "rust-part-1",
    ]


def test_rust_reads_pyarrow_minimal_arrow_stream(tmp_path: Path) -> None:
    fixture = minimal_fixture()
    stream_path = tmp_path / "pyarrow-minimal.cjarrow"
    write_stream(
        stream_path,
        header=fixture.header,
        projection=fixture.projection,
        tables=fixture.tables,
    )

    model = read_arrow_json(stream_path)

    assert model["type"] == "CityJSON"
    assert model["version"] == "2.0"
    assert model["CityObjects"] == {}


def test_rust_reads_pyarrow_mixed_cityobjects_arrow_stream(tmp_path: Path) -> None:
    fixture = mixed_cityobjects_fixture()
    stream_path = tmp_path / "pyarrow-mixed.cjarrow"
    write_stream(
        stream_path,
        header=fixture.header,
        projection=fixture.projection,
        tables=fixture.tables,
    )

    model = read_arrow_json(stream_path)

    assert set(model["CityObjects"]) == {"building-1", "part-1", "road-1"}
    assert model["CityObjects"]["building-1"]["type"] == "Building"
    assert model["CityObjects"]["building-1"]["children"] == ["part-1"]
    assert model["CityObjects"]["part-1"]["parents"] == ["building-1"]
    assert model["CityObjects"]["building-1"]["attributes"]["name"] == "Main Building"
    assert model["CityObjects"]["building-1"]["attributes"]["height"] == 12.5


def test_rust_reads_pyarrow_geometry_arrow_stream(tmp_path: Path) -> None:
    fixture = building_geometry_fixture()
    stream_path = tmp_path / "pyarrow-geometry.cjarrow"
    write_stream(
        stream_path,
        header=fixture.header,
        projection=fixture.projection,
        tables=fixture.tables,
    )

    model = read_arrow_json(stream_path)

    building = model["CityObjects"]["building-geometry"]
    assert building["type"] == "Building"
    assert building["geometry"][0]["type"] == "MultiSurface"
    assert building["geometry"][0]["boundaries"] == [[[0, 1, 2, 3, 0]]]


def _table_by_name(stream, name: str):
    for stream_table in stream.tables:
        if stream_table.name == name:
            return stream_table.table
    raise AssertionError(f"stream does not contain {name}")
