# demo-simple-iceberg

This repo is a small, local Apache Iceberg demo for people who think in pandas dataframes, not data platform diagrams.

It creates one local Iceberg warehouse with two small tables and one saved view, and then shows how you can:

- list tables from a single point of access
- load Iceberg tables back into pandas
- join multiple tables in a familiar dataframe workflow
- inspect snapshot history for lightweight table versioning context

The tables are written as parquet-backed Iceberg tables inside a local warehouse directory named `demo_warehouse/`.
Table discovery is handled by a tiny filesystem registry, so there is no SQLite catalog and no SQLAlchemy dependency.

## Why Iceberg Here?

The useful idea is the catalog.

Instead of passing around loose parquet paths, you work through one catalog and ask for tables by name:

- `analytics.profiles`
- `analytics.images`
- `analytics.profile_image_view`

That gives you a simple mental model:

1. put related tables in one namespace
1. discover them from one place
1. read them into pandas whenever you want to do analysis

For a small team, that is often enough structure without introducing heavyweight infrastructure.

## Project Layout

- `src/demo_simple_iceberg/demo.py`: the single module that builds the warehouse, prints a catalog-like view, and shows a joined report
- `tests/test_demo.py`: focused tests for the module and CLI-style output
- `pyproject.toml`: dependencies plus the `poe demo` task

## Quick Start

Install the project and dev tools:

```bash
uv sync --group dev
```

Run the demo from the poethepoet task:

```bash
uv run poe demo
```

Run the tests:

```bash
uv run pytest
```

## What The Demo Prints

The module prints six blocks:

1. `Catalog Overview`
1. `Profiles Table`
1. `Images Table`
1. `Profile Image View`
1. `Images Data Files`
1. `Images Snapshot History`

The first block is the main Iceberg-oriented one. It shows a small catalog display with:

- namespace
- table name
- row count
- columns
- current snapshot id

That gives users a quick inventory of what is available before they start loading dataframes.

## Programmatic Use

You can also use the module from Python:

```python
from demo_simple_iceberg.demo import build_demo_outputs

outputs = build_demo_outputs()

catalog_df = outputs["catalog"]
profiles_df = outputs["profiles"]
images_df = outputs["images"]
joined_df = outputs["profile_image_view"]
snapshots_df = outputs["snapshots"]
```

Each value is returned as a pandas `DataFrame`, so the handoff back into normal notebook-style analysis stays simple.

## CytoTable-Style Access

If CytoTable writes a result bundle with a `warehouse/` directory, the easiest user-facing API is a few small functions:

```python
from demo_simple_iceberg.cytotable_access import describe, read, tables

tables("path/to/result")
describe("path/to/result")
profiles_df = read(warehouse="path/to/result", table="analytics.profiles")
joined_df = read(warehouse="path/to/result", table="analytics.profile_image_view")
```

That keeps the interface simple:

- `tables(...)` tells users what exists
- `describe(...)` gives a small Iceberg-aware summary
- `read(warehouse=..., table=...)` loads one table into a dataframe

## Iceberg Features Demonstrated

This example intentionally keeps the scope narrow.

- `Catalog listing`: discover multiple tables from one catalog instead of tracking raw parquet file paths
- `Table-backed parquet storage`: data lands as parquet files, but the catalog and Iceberg metadata give the tables structure
- `Arrow struct columns for images`: the `images` table stores an `ome_image` struct with shape, channel names, and pixels
- `Saved joins as lightweight views`: the `profile_image_view` entry resolves into a pandas join across Iceberg tables
- `Snapshot history`: inspect the current table history after appends
- `Pandas interoperability`: read from Iceberg and continue analysis with standard pandas merges and transformations

## Notes

- The demo recreates `demo_warehouse/` each time it runs so the output stays deterministic.
- This is a local example using `pyiceberg` table metadata plus a tiny filesystem registry for table discovery.
- The warehouse on disk contains Iceberg metadata files, parquet data files, and a small JSON registry file.
- It is designed for learning and small-project ergonomics, not production hardening.
