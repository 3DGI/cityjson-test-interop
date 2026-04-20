from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pyarrow as pa
import pyarrow.parquet as pq

from cityjson_test_interop.arrow_stream import NamedTable, SCHEMA_ID

CITYJSON_VERSION: Final = "2.0"


@dataclass(frozen=True, slots=True)
class InteropFixture:
    citymodel_id: str
    header: dict[str, object]
    projection: dict[str, object]
    tables: list[NamedTable]


def minimal_fixture() -> InteropFixture:
    return _fixture(
        citymodel_id="interop-minimal",
        projection={},
        tables=[
            NamedTable("metadata", _metadata_table("interop-minimal")),
            NamedTable("vertices", _vertices_table([])),
            NamedTable("geometry_boundaries", _geometry_boundaries_table([])),
            NamedTable("geometries", _geometries_table([])),
            NamedTable("cityobjects", _cityobjects_table([])),
        ],
    )


def mixed_cityobjects_fixture() -> InteropFixture:
    projection = {
        "cityobject_attributes": {
            "fields": [
                {"name": "name", "value": "Utf8", "nullable": True},
                {"name": "height", "value": "Float64", "nullable": True},
            ]
        }
    }
    cityobjects = [
        {
            "cityobject_id": "building-1",
            "cityobject_ix": 0,
            "object_type": "Building",
            "name": "Main Building",
            "height": 12.5,
        },
        {
            "cityobject_id": "part-1",
            "cityobject_ix": 1,
            "object_type": "BuildingPart",
            "name": "Annex",
            "height": 3.25,
        },
        {
            "cityobject_id": "road-1",
            "cityobject_ix": 2,
            "object_type": "Road",
            "name": None,
            "height": None,
        },
    ]
    return _fixture(
        citymodel_id="interop-mixed",
        projection=projection,
        tables=[
            NamedTable("metadata", _metadata_table("interop-mixed")),
            NamedTable("vertices", _vertices_table([])),
            NamedTable("geometry_boundaries", _geometry_boundaries_table([])),
            NamedTable("geometries", _geometries_table([])),
            NamedTable("cityobjects", _cityobjects_table(cityobjects, with_attributes=True)),
            NamedTable(
                "cityobject_children",
                _cityobject_children_table(
                    [
                        {
                            "parent_cityobject_ix": 0,
                            "child_ordinal": 0,
                            "child_cityobject_ix": 1,
                        }
                    ]
                ),
            ),
        ],
    )


def building_geometry_fixture() -> InteropFixture:
    return _fixture(
        citymodel_id="interop-building-geometry",
        projection={},
        tables=[
            NamedTable("metadata", _metadata_table("interop-building-geometry")),
            NamedTable(
                "vertices",
                _vertices_table(
                    [
                        (0, 0.0, 0.0, 0.0),
                        (1, 1.0, 0.0, 0.0),
                        (2, 1.0, 1.0, 0.0),
                        (3, 0.0, 1.0, 0.0),
                    ]
                ),
            ),
            NamedTable(
                "geometry_boundaries",
                _geometry_boundaries_table(
                    [
                        {
                            "geometry_id": 0,
                            "vertex_indices": [0, 1, 2, 3, 0],
                            "line_offsets": None,
                            "ring_offsets": [0],
                            "surface_offsets": [0],
                            "shell_offsets": None,
                            "solid_offsets": None,
                        }
                    ]
                ),
            ),
            NamedTable(
                "geometries",
                _geometries_table(
                    [
                        {
                            "geometry_id": 0,
                            "cityobject_ix": 0,
                            "geometry_ordinal": 0,
                            "geometry_type": "MultiSurface",
                            "lod": "2.0",
                        }
                    ]
                ),
            ),
            NamedTable(
                "cityobjects",
                _cityobjects_table(
                    [
                        {
                            "cityobject_id": "building-geometry",
                            "cityobject_ix": 0,
                            "object_type": "Building",
                        }
                    ]
                ),
            ),
        ],
    )


def write_parquet_dataset(root: Path, fixture: InteropFixture) -> None:
    table_dir = root / "tables"
    table_dir.mkdir(parents=True, exist_ok=True)
    table_refs: list[dict[str, object]] = []
    for named_table in fixture.tables:
        relative_path = Path("tables") / f"{named_table.name}.parquet"
        parquet_table = _parquet_table(named_table.table)
        pq.write_table(parquet_table, root / relative_path, row_group_size=1)
        table_refs.append(
            {
                "name": named_table.name,
                "path": relative_path.as_posix(),
                "rows": named_table.table.num_rows,
            }
        )

    manifest = {
        "package_schema": SCHEMA_ID,
        "cityjson_version": CITYJSON_VERSION,
        "citymodel_id": fixture.citymodel_id,
        "projection": fixture.projection,
        "tables": table_refs,
    }
    (root / "manifest.json").write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )


def _parquet_table(table: pa.Table) -> pa.Table:
    columns = [
        _parquet_array(column.combine_chunks(), field)
        for column, field in zip(table.columns, table.schema)
    ]
    schema = pa.schema([_parquet_field(field) for field in table.schema])
    return pa.Table.from_arrays(columns, schema=schema)


def _parquet_field(field: pa.Field) -> pa.Field:
    return pa.field(
        field.name,
        _parquet_type(field.type),
        nullable=field.nullable,
        metadata=field.metadata,
    )


def _parquet_type(data_type: pa.DataType) -> pa.DataType:
    if pa.types.is_fixed_size_list(data_type):
        return pa.list_(_parquet_field(data_type.value_field))
    if pa.types.is_list(data_type):
        return pa.list_(_parquet_field(data_type.value_field))
    if pa.types.is_struct(data_type):
        return pa.struct([_parquet_field(field) for field in data_type])
    return data_type


def _parquet_array(array: pa.Array, field: pa.Field) -> pa.Array:
    data_type = field.type
    if pa.types.is_fixed_size_list(data_type):
        return _fixed_size_list_to_parquet_list(array, field)
    if pa.types.is_struct(data_type):
        struct_type = _parquet_type(data_type)
        return pa.StructArray.from_arrays(
            [
                _parquet_array(array.field(child_index), child_field)
                for child_index, child_field in enumerate(data_type)
            ],
            fields=list(struct_type),
            mask=array.is_null(),
        )
    return array


def _fixed_size_list_to_parquet_list(array: pa.Array, field: pa.Field) -> pa.Array:
    fixed_size_type = field.type
    values: list[list[float] | None] = []
    for row_index in range(len(array)):
        scalar = array[row_index]
        if not scalar.is_valid:
            values.append(None)
            continue
        row = scalar.as_py()
        if len(row) != fixed_size_type.list_size:
            msg = (
                f"{field.name} row {row_index} has {len(row)} values, "
                f"expected {fixed_size_type.list_size}"
            )
            raise ValueError(msg)
        values.append(row)
    return pa.array(values, type=_parquet_type(fixed_size_type))


def _fixture(
    *,
    citymodel_id: str,
    projection: dict[str, object],
    tables: list[NamedTable],
) -> InteropFixture:
    return InteropFixture(
        citymodel_id=citymodel_id,
        header={
            "package_version": SCHEMA_ID,
            "citymodel_id": citymodel_id,
            "cityjson_version": CITYJSON_VERSION,
        },
        projection=projection,
        tables=tables,
    )


def _metadata_table(citymodel_id: str) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("citymodel_id", pa.large_string(), nullable=False),
            pa.field("cityjson_version", pa.string(), nullable=False),
            pa.field("citymodel_kind", pa.string(), nullable=False),
            pa.field("feature_root_id", pa.large_string(), nullable=True),
            pa.field("identifier", pa.large_string(), nullable=True),
            pa.field("title", pa.large_string(), nullable=True),
            pa.field("reference_system", pa.large_string(), nullable=True),
            pa.field("geographical_extent", _fixed_f64_6_type(), nullable=True),
            pa.field("reference_date", pa.string(), nullable=True),
            pa.field("default_material_theme", pa.string(), nullable=True),
            pa.field("default_texture_theme", pa.string(), nullable=True),
            pa.field("point_of_contact", _point_of_contact_type(), nullable=True),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([citymodel_id], type=pa.large_string()),
            pa.array([CITYJSON_VERSION], type=pa.string()),
            pa.array(["CityJSON"], type=pa.string()),
            pa.array([None], type=pa.large_string()),
            pa.array([citymodel_id], type=pa.large_string()),
            pa.array([None], type=pa.large_string()),
            pa.array([None], type=pa.large_string()),
            pa.array([None], type=_fixed_f64_6_type()),
            pa.array([None], type=pa.string()),
            pa.array([None], type=pa.string()),
            pa.array([None], type=pa.string()),
            pa.array([None], type=_point_of_contact_type()),
        ],
        schema=schema,
    )


def _point_of_contact_type() -> pa.StructType:
    return pa.struct(
        [
            pa.field("contact_name", pa.large_string(), nullable=False),
            pa.field("email_address", pa.large_string(), nullable=False),
            pa.field("role", pa.string(), nullable=True),
            pa.field("website", pa.large_string(), nullable=True),
            pa.field("contact_type", pa.string(), nullable=True),
            pa.field("phone", pa.large_string(), nullable=True),
            pa.field("organization", pa.large_string(), nullable=True),
        ]
    )


def _fixed_f64_6_type() -> pa.FixedSizeListType:
    return pa.list_(pa.field("item", pa.float64(), nullable=False), 6)


def _list_u32_type() -> pa.ListType:
    return pa.list_(pa.field("item", pa.uint32(), nullable=False))


def _vertices_table(rows: list[tuple[int, float, float, float]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("vertex_id", pa.uint64(), nullable=False),
            pa.field("x", pa.float64(), nullable=False),
            pa.field("y", pa.float64(), nullable=False),
            pa.field("z", pa.float64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([row[0] for row in rows], type=pa.uint64()),
            pa.array([row[1] for row in rows], type=pa.float64()),
            pa.array([row[2] for row in rows], type=pa.float64()),
            pa.array([row[3] for row in rows], type=pa.float64()),
        ],
        schema=schema,
    )


def _geometry_boundaries_table(rows: list[dict[str, object]]) -> pa.Table:
    list_u32 = _list_u32_type()
    schema = pa.schema(
        [
            pa.field("geometry_id", pa.uint64(), nullable=False),
            pa.field("vertex_indices", list_u32, nullable=False),
            pa.field("line_offsets", list_u32, nullable=True),
            pa.field("ring_offsets", list_u32, nullable=True),
            pa.field("surface_offsets", list_u32, nullable=True),
            pa.field("shell_offsets", list_u32, nullable=True),
            pa.field("solid_offsets", list_u32, nullable=True),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([row["geometry_id"] for row in rows], type=pa.uint64()),
            pa.array([row["vertex_indices"] for row in rows], type=list_u32),
            pa.array([row["line_offsets"] for row in rows], type=list_u32),
            pa.array([row["ring_offsets"] for row in rows], type=list_u32),
            pa.array([row["surface_offsets"] for row in rows], type=list_u32),
            pa.array([row["shell_offsets"] for row in rows], type=list_u32),
            pa.array([row["solid_offsets"] for row in rows], type=list_u32),
        ],
        schema=schema,
    )


def _geometries_table(rows: list[dict[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("geometry_id", pa.uint64(), nullable=False),
            pa.field("cityobject_ix", pa.uint64(), nullable=False),
            pa.field("geometry_ordinal", pa.uint32(), nullable=False),
            pa.field("geometry_type", pa.string(), nullable=False),
            pa.field("lod", pa.string(), nullable=True),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([row["geometry_id"] for row in rows], type=pa.uint64()),
            pa.array([row["cityobject_ix"] for row in rows], type=pa.uint64()),
            pa.array([row["geometry_ordinal"] for row in rows], type=pa.uint32()),
            pa.array([row["geometry_type"] for row in rows], type=pa.string()),
            pa.array([row["lod"] for row in rows], type=pa.string()),
        ],
        schema=schema,
    )


def _cityobjects_table(
    rows: list[dict[str, object]],
    *,
    with_attributes: bool = False,
) -> pa.Table:
    fields = [
        pa.field("cityobject_id", pa.large_string(), nullable=False),
        pa.field("cityobject_ix", pa.uint64(), nullable=False),
        pa.field("object_type", pa.string(), nullable=False),
        pa.field("geographical_extent", _fixed_f64_6_type(), nullable=True),
    ]
    arrays = [
        pa.array([row["cityobject_id"] for row in rows], type=pa.large_string()),
        pa.array([row["cityobject_ix"] for row in rows], type=pa.uint64()),
        pa.array([row["object_type"] for row in rows], type=pa.string()),
        pa.array([None for _ in rows], type=_fixed_f64_6_type()),
    ]
    if with_attributes:
        attribute_type = pa.struct(
            [
                pa.field("name", pa.large_string(), nullable=True),
                pa.field("height", pa.float64(), nullable=True),
            ]
        )
        fields.append(pa.field("attributes", attribute_type, nullable=True))
        arrays.append(
            pa.array(
                [
                    {"name": row.get("name"), "height": row.get("height")}
                    for row in rows
                ],
                type=attribute_type,
            )
        )
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def _cityobject_children_table(rows: list[dict[str, object]]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field("parent_cityobject_ix", pa.uint64(), nullable=False),
            pa.field("child_ordinal", pa.uint32(), nullable=False),
            pa.field("child_cityobject_ix", pa.uint64(), nullable=False),
        ]
    )
    return pa.Table.from_arrays(
        [
            pa.array([row["parent_cityobject_ix"] for row in rows], type=pa.uint64()),
            pa.array([row["child_ordinal"] for row in rows], type=pa.uint32()),
            pa.array([row["child_cityobject_ix"] for row in rows], type=pa.uint64()),
        ],
        schema=schema,
    )
