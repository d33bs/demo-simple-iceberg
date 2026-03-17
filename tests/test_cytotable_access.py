"""Tests for the CytoTable-style access helpers."""

from pathlib import Path

import pyarrow as pa

from demo_simple_iceberg.cytotable_access import (
    TinyCatalog,
    create_view,
    describe,
    object_id,
    read,
    tables,
)
from demo_simple_iceberg.demo import (
    ARROW_SCHEMAS,
    DEMO_ROW_COUNT,
    ICEBERG_SCHEMAS,
    TABLES,
    build_demo_warehouse,
)

OBJECT_ID_LENGTH = 40


def test_result_helpers_make_warehouse_easy_to_use(tmp_path: Path) -> None:
    """Users should be able to list and read tables from a result folder."""
    warehouse = tmp_path / "warehouse"
    build_demo_warehouse(warehouse)

    assert tables(warehouse, include_views=False) == [
        "analytics.images",
        "analytics.profiles",
    ]
    assert "analytics.profile_image_view" in tables(warehouse)

    profiles = read(warehouse=warehouse, table="profiles")
    assert profiles.shape == (3, 5)

    summary = describe(warehouse=warehouse)
    assert summary["kind"].tolist() == ["table", "view", "table"]
    assert summary["rows"].tolist() == [3, 3, 3]


def test_custom_views_can_join_tables(tmp_path: Path) -> None:
    """Saved views should read like easy joined tables."""
    warehouse = tmp_path / "warehouse"
    build_demo_warehouse(warehouse)
    create_view(
        warehouse=warehouse,
        name="profiles_with_images",
        sources=["profiles", "images"],
        join_keys=["image_id"],
    )

    assert "analytics.profiles_with_images" in tables(warehouse)
    joined = read(warehouse=warehouse, table="profiles_with_images")
    assert "ome_arrow" in joined.columns
    assert joined.shape[0] == DEMO_ROW_COUNT


def test_access_helpers_support_custom_namespace_and_registry(tmp_path: Path) -> None:
    """The reusable access layer should not depend on demo defaults."""
    warehouse = tmp_path / "custom_warehouse"
    catalog = TinyCatalog(
        warehouse,
        default_namespace="experiment",
        registry_file="cytotable_registry.json",
    )
    catalog.create_namespace("experiment")
    table = catalog.create_table(
        ("experiment", "profiles"), ICEBERG_SCHEMAS["profiles"]
    )
    table.append(
        pa.Table.from_pandas(
            TABLES["profiles"],
            schema=ARROW_SCHEMAS["profiles"],
            preserve_index=False,
        )
    )

    assert tables(
        warehouse,
        include_views=False,
        default_namespace="experiment",
        registry_file="cytotable_registry.json",
    ) == ["experiment.profiles"]
    frame = read(
        warehouse,
        table="profiles",
        default_namespace="experiment",
        registry_file="cytotable_registry.json",
    )
    assert frame.shape == (DEMO_ROW_COUNT, 5)
    summary = describe(
        warehouse,
        default_namespace="experiment",
        registry_file="cytotable_registry.json",
    )
    assert summary["table"].tolist() == ["experiment.profiles"]


def test_object_id_uses_prefixed_uuid_strings() -> None:
    """Object IDs should be string-friendly and UUID-shaped."""
    generated = object_id("image-001")
    assert generated.startswith("obj-")
    assert generated == object_id("image-001")
    assert len(generated) == OBJECT_ID_LENGTH
