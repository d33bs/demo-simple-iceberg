"""Tests for the Iceberg demo module."""

from pathlib import Path

from demo_simple_iceberg.demo import IMAGE_IDS, build_demo_outputs, main


def test_build_demo_outputs(tmp_path: Path) -> None:
    """The demo should expose a small catalog and joined dataframe views."""
    outputs = build_demo_outputs(warehouse_root=tmp_path / "warehouse")

    catalog = outputs["catalog"]
    assert catalog["table"].tolist() == ["images", "profile_image_view", "profiles"]
    assert catalog["kind"].tolist() == ["table", "view", "table"]
    assert catalog["rows"].tolist() == [3, 3, 3]

    joined = outputs["profile_image_view"]
    assert joined.shape == (3, 7)
    assert joined["image_id"].tolist() == IMAGE_IDS
    assert "image_type" in joined.columns


def test_cli_main_prints_sections(tmp_path: Path, capsys: object) -> None:
    """The module entrypoint should print the expected sections."""
    main(["--warehouse-root", str(tmp_path / "warehouse")])
    captured = capsys.readouterr()

    assert "Catalog Overview" in captured.out
    assert "Images Snapshot History" in captured.out
