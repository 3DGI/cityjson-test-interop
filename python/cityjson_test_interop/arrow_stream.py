from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pyarrow as pa

STREAM_MAGIC: Final = b"CITYJSON_ARROW_STREAM_V3\0"
STREAM_END_TAG: Final = 0xFF
SCHEMA_ID: Final = "cityjson-arrow.package.v3alpha3"

TABLE_TAGS: Final[dict[str, int]] = {
    "metadata": 0,
    "extensions": 2,
    "vertices": 3,
    "template_vertices": 4,
    "texture_vertices": 5,
    "semantics": 6,
    "semantic_children": 7,
    "materials": 8,
    "textures": 9,
    "template_geometry_boundaries": 10,
    "template_geometry_semantics": 11,
    "template_geometry_materials": 12,
    "template_geometry_ring_textures": 13,
    "template_geometries": 14,
    "geometry_boundaries": 15,
    "geometry_surface_semantics": 16,
    "geometry_point_semantics": 17,
    "geometry_linestring_semantics": 18,
    "geometry_surface_materials": 19,
    "geometry_ring_textures": 20,
    "geometry_instances": 21,
    "geometries": 22,
    "cityobjects": 23,
    "cityobject_children": 24,
}
TABLE_NAMES: Final = {tag: name for name, tag in TABLE_TAGS.items()}


@dataclass(frozen=True, slots=True)
class StreamTable:
    name: str
    rows: int
    table: pa.Table


@dataclass(frozen=True, slots=True)
class NamedTable:
    name: str
    table: pa.Table


@dataclass(frozen=True, slots=True)
class CityJsonArrowStream:
    header: dict[str, object]
    projection: dict[str, object]
    tables: list[StreamTable]


def write_stream(
    path: Path,
    *,
    header: dict[str, object],
    projection: dict[str, object],
    tables: list[NamedTable],
) -> None:
    prelude = {"header": header, "projection": projection}
    prelude_bytes = json.dumps(prelude, separators=(",", ":")).encode("utf-8")
    chunks = [
        STREAM_MAGIC,
        struct.pack("<Q", len(prelude_bytes)),
        prelude_bytes,
    ]

    for named_table in tables:
        if named_table.name not in TABLE_TAGS:
            raise ValueError(f"unknown canonical table {named_table.name!r}")
        chunks.append(
            struct.pack("<BQ", TABLE_TAGS[named_table.name], named_table.table.num_rows)
        )
        chunks.append(_ipc_stream_payload(named_table.table))

    chunks.append(bytes([STREAM_END_TAG]))
    path.write_bytes(b"".join(chunks))


def read_stream(path: Path) -> CityJsonArrowStream:
    data = path.read_bytes()
    if not data.startswith(STREAM_MAGIC):
        raise ValueError(f"{path} is not a CityJSON Arrow stream")

    offset = len(STREAM_MAGIC)
    prelude_len = struct.unpack_from("<Q", data, offset)[0]
    offset += 8
    prelude_end = offset + prelude_len
    prelude = json.loads(data[offset:prelude_end])
    offset = prelude_end

    tables: list[StreamTable] = []
    while offset < len(data):
        tag = data[offset]
        offset += 1
        if tag == STREAM_END_TAG:
            break
        if tag not in TABLE_NAMES:
            raise ValueError(f"unknown canonical table tag {tag}")
        rows = struct.unpack_from("<Q", data, offset)[0]
        offset += 8
        source = pa.BufferReader(data[offset:])
        table = pa.ipc.open_stream(source).read_all()
        consumed = source.tell()
        offset += consumed
        tables.append(StreamTable(name=TABLE_NAMES[tag], rows=rows, table=table))

    return CityJsonArrowStream(
        header=prelude["header"],
        projection=prelude["projection"],
        tables=tables,
    )


def _ipc_stream_payload(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()

