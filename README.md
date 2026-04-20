# cityjson-test-interop

Black-box interoperability tests for CityJSON Arrow and native CityJSON Parquet datasets.

This repository verifies that the published CityJSON Arrow/Parquet schemas are usable by
independent implementations. Tests compare semantic CityJSON meaning rather than
implementation-specific binary equality.

The suite intentionally lives outside `cityjson-arrow` and `cityjson-parquet` so that
readers and writers can be implemented against the published specs without relying on private
crate internals.

## Scope

- PyArrow reads Rust-produced CityJSON Arrow streams.
- Rust reads PyArrow-produced CityJSON Arrow streams.
- PyArrow reads Rust-produced native Parquet datasets.
- Rust reads PyArrow-produced native Parquet datasets.
- DuckDB and Polars query native Parquet table files with filtering and projection.

Native Parquet tests intentionally treat nullable fixed-size CityJSON Arrow fields
as Parquet lists with fixed-length semantic validation. PyArrow 23.0.1 cannot
full-scan nullable `FixedSizeList` Parquet columns, including files it writes itself.

The existing `.cityjson-parquet` single-file package is not the target for DuckDB/Polars tests;
that package is an Arrow IPC-backed container. Native Parquet interop uses the dataset API in
`cityjson-parquet`.

## Layout

- `python/cityjson_test_interop/` contains spec-based Python artifact writers and readers.
- `tests/` contains pytest scenarios.
- `rust-tools/` contains a small helper binary that calls public Rust APIs only.

## Requirements

- Python 3.12+
- `uv`
- Rust toolchain
- sibling checkouts:
  - `../cityjson-arrow`
  - `../cityjson-parquet`
  - `../cityjson-rs`
  - `../cityjson-json`

## Commands

```bash
uv sync
uv run pytest
```

or:

```bash
just ci
```
