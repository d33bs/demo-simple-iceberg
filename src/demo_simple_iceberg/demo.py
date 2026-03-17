"""Small local Apache Iceberg demo for pandas-style workflows."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
from ome_arrow import OMEArrow
from ome_arrow.meta import OME_ARROW_STRUCT
from pyiceberg.schema import Schema
from pyiceberg.types import DoubleType, LongType, NestedField, StringType

from demo_simple_iceberg.cytotable_access import (
    NAMESPACE,
    TinyCatalog,
    create_join_view,
    describe,
    object_id,
    read,
)

ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_WAREHOUSE = ROOT_DIR / "demo_warehouse"
DEMO_ROW_COUNT = 3
ICEBERG_OME_ARROW_STRUCT = pa.struct(
    [field for field in OME_ARROW_STRUCT if field.name != "masks"]
)
IMAGE_IDS = [object_id(f"image-{index:03d}") for index in range(1, DEMO_ROW_COUNT + 1)]

TABLES = {
    "profiles": pd.DataFrame(
        [
            {
                "profile_id": "P-001",
                "image_id": IMAGE_IDS[0],
                "well_id": "A01",
                "cell_count": 182,
                "mean_intensity": 0.82,
            },
            {
                "profile_id": "P-002",
                "image_id": IMAGE_IDS[1],
                "well_id": "A02",
                "cell_count": 205,
                "mean_intensity": 0.67,
            },
            {
                "profile_id": "P-003",
                "image_id": IMAGE_IDS[2],
                "well_id": "B01",
                "cell_count": 163,
                "mean_intensity": 0.91,
            },
        ]
    ),
}


def iceberg_schema(*fields: tuple[str, Any]) -> Schema:
    return Schema(
        *(
            NestedField(i, name, dtype, required=False)
            for i, (name, dtype) in enumerate(fields, start=1)
        )
    )


def arrow_schema(*fields: tuple[str, Any]) -> pa.Schema:
    return pa.schema([pa.field(name, dtype) for name, dtype in fields])


ICEBERG_SCHEMAS = {
    "profiles": iceberg_schema(
        ("profile_id", StringType()),
        ("image_id", StringType()),
        ("well_id", StringType()),
        ("cell_count", LongType()),
        ("mean_intensity", DoubleType()),
    ),
}

ARROW_SCHEMAS = {
    "profiles": arrow_schema(
        ("profile_id", pa.string()),
        ("image_id", pa.string()),
        ("well_id", pa.string()),
        ("cell_count", pa.int64()),
        ("mean_intensity", pa.float64()),
    ),
    "images": arrow_schema(
        ("image_id", pa.string()),
        ("ome_arrow", ICEBERG_OME_ARROW_STRUCT),
    ),
}

ICEBERG_SCHEMAS["images"] = ARROW_SCHEMAS["images"]


def image_table() -> pa.Table:
    """Build a tiny Arrow table using ome-arrow payloads projected for Iceberg."""

    def as_iceberg_row(pixels: list[list[int]]) -> dict[str, object]:
        row = OMEArrow(
            np.array([[[pixels]]], dtype="uint16"),
            image_type="image",
        ).data.as_py()
        return {key: value for key, value in row.items() if key != "masks"}

    ome_rows = [
        as_iceberg_row([[0, 32], [64, 255]]),
        as_iceberg_row([[8, 24], [96, 180]]),
        as_iceberg_row([[12, 48], [128, 220]]),
    ]
    return pa.table(
        {
            "image_id": pa.array(IMAGE_IDS),
            "ome_arrow": pa.array(ome_rows, type=ICEBERG_OME_ARROW_STRUCT),
        },
        schema=ARROW_SCHEMAS["images"],
    )


def build_demo_warehouse(warehouse_root: Path = DEFAULT_WAREHOUSE) -> TinyCatalog:
    if warehouse_root.exists():
        shutil.rmtree(warehouse_root)
    catalog = TinyCatalog(warehouse_root)
    catalog.create_namespace(NAMESPACE)
    for name, frame in {**TABLES, "images": image_table()}.items():
        table = catalog.create_table(
            (NAMESPACE, name),
            ICEBERG_SCHEMAS[name],
        )
        arrow_table = (
            frame
            if isinstance(frame, pa.Table)
            else pa.Table.from_pandas(
                frame, schema=ARROW_SCHEMAS[name], preserve_index=False
            )
        )
        table.append(arrow_table)
    create_join_view(
        warehouse_root,
        name="profile_image_view",
        base="profiles",
        joins=[
            {"source": "images", "on": ["image_id"]},
        ],
    )
    return catalog


def read_table(catalog: TinyCatalog, name: str) -> pd.DataFrame:
    return catalog.load_table((NAMESPACE, name)).scan().to_arrow().to_pandas()


def inspect_table(catalog: TinyCatalog, name: str) -> dict[str, pd.DataFrame]:
    table = catalog.load_table((NAMESPACE, name))
    files = table.inspect.files().to_pandas()
    manifests = table.inspect.manifests().to_pandas()
    snapshots = table.inspect.snapshots().to_pandas()
    return {
        "data": table.scan().to_arrow().to_pandas(),
        "files": files.loc[
            :, ["file_path", "file_format", "record_count", "file_size_in_bytes"]
        ],
        "manifests": manifests.loc[
            :, ["path", "added_snapshot_id", "added_data_files_count"]
        ],
        "snapshots": snapshots.loc[
            :, ["snapshot_id", "parent_id", "operation", "committed_at"]
        ],
        "summary": pd.DataFrame(
            [
                {
                    "namespace": NAMESPACE,
                    "table": name,
                    "rows": int(files["record_count"].sum()),
                    "data_files": len(files),
                    "manifest_files": len(manifests),
                    "snapshot_id": table.current_snapshot().snapshot_id,
                }
            ]
        ),
    }


def image_summary(frame: pd.DataFrame) -> pd.DataFrame:
    """Build a compact display for the nested ome-arrow image payload."""
    return frame.assign(
        image_type=lambda df: df["ome_arrow"].map(lambda value: value["image_type"]),
        size_yx=lambda df: df["ome_arrow"].map(
            lambda value: (
                value["pixels_meta"]["size_y"],
                value["pixels_meta"]["size_x"],
            )
        ),
        plane_count=lambda df: df["ome_arrow"].map(lambda value: len(value["planes"])),
    )[["image_id", "image_type", "size_yx", "plane_count"]]


def build_demo_outputs(
    warehouse_root: Path = DEFAULT_WAREHOUSE,
) -> dict[str, pd.DataFrame]:
    catalog = build_demo_warehouse(warehouse_root)
    profiles = inspect_table(catalog, "profiles")
    images = inspect_table(catalog, "images")
    catalog_df = describe(warehouse_root, include_views=True)
    if "table" in catalog_df.columns:
        split_names = catalog_df["table"].str.split(".", n=1, expand=True)
        catalog_df = catalog_df.assign(namespace=split_names[0], table=split_names[1])[
            ["namespace", "table", "kind", "rows", "data_files", "snapshot_id"]
        ]
    joined = read(warehouse_root, "profile_image_view")
    view_summary = joined.assign(
        image_type=lambda df: df["ome_arrow"].map(lambda value: value["image_type"]),
        size_yx=lambda df: df["ome_arrow"].map(
            lambda value: (
                value["pixels_meta"]["size_y"],
                value["pixels_meta"]["size_x"],
            )
        ),
    )[
        [
            "profile_id",
            "image_id",
            "well_id",
            "cell_count",
            "mean_intensity",
            "image_type",
            "size_yx",
        ]
    ]
    return {
        "catalog": catalog_df,
        "profiles": profiles["data"],
        "images": image_summary(images["data"]),
        "profile_image_view": view_summary,
        "files": images["files"],
        "manifests": images["manifests"],
        "snapshots": images["snapshots"],
    }


def _block(title: str, frame: pd.DataFrame) -> str:
    return f"{title}\n{'=' * len(title)}\n{frame.to_string(index=False)}"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-root", type=Path, default=DEFAULT_WAREHOUSE)
    args = parser.parse_args(argv)
    outputs = build_demo_outputs(args.warehouse_root)
    print(
        "\n\n".join(
            [
                _block("Catalog Overview", outputs["catalog"]),
                _block("Profiles Table", outputs["profiles"]),
                _block("Images Table", outputs["images"]),
                _block("Profile Image View", outputs["profile_image_view"]),
                _block("Images Data Files", outputs["files"]),
                _block("Images Manifests", outputs["manifests"]),
                _block("Images Snapshot History", outputs["snapshots"]),
            ]
        )
    )


if __name__ == "__main__":
    main()
