"""Tests for the CytoTable-style access helpers."""

from pathlib import Path

from demo_simple_iceberg.cytotable_access import create_view, describe, read, tables
from demo_simple_iceberg.demo import DEMO_ROW_COUNT, build_demo_warehouse


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
    assert "ome_image" in joined.columns
    assert joined.shape[0] == DEMO_ROW_COUNT
