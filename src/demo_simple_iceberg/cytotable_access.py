"""Small helper functions for CytoTable-style Iceberg result bundles."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from pyiceberg.catalog import Catalog, MetastoreCatalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.table.update import TableRequirement, TableUpdate
from pyiceberg.typedef import EMPTY_DICT

NAMESPACE = "analytics"
REGISTRY_FILE = "catalog.json"


class TinyCatalog(MetastoreCatalog):
    """Tiny filesystem-backed catalog for local Iceberg result bundles."""

    def __init__(self, warehouse_root: Path):
        self.registry_path = warehouse_root / REGISTRY_FILE
        warehouse_root.mkdir(parents=True, exist_ok=True)
        super().__init__("local", warehouse=warehouse_root.resolve().as_uri())

    def _read_registry(self) -> dict[str, object]:
        if not self.registry_path.exists():
            return {"namespaces": [NAMESPACE], "tables": {}, "views": {}}
        registry = json.loads(self.registry_path.read_text())
        registry.setdefault("views", {})
        return registry

    def _write_registry(self, registry: dict[str, object]) -> None:
        self.registry_path.write_text(json.dumps(registry, indent=2, sort_keys=True))

    def create_namespace(self, namespace, properties=EMPTY_DICT) -> None:
        registry = self._read_registry()
        names = set(registry["namespaces"])
        names.add(Catalog.namespace_to_string(namespace))
        registry["namespaces"] = sorted(names)
        self._write_registry(registry)

    def load_namespace_properties(self, namespace) -> dict[str, str]:
        name = Catalog.namespace_to_string(namespace)
        if name not in self._read_registry()["namespaces"]:
            raise NoSuchNamespaceError(name)
        return {}

    def list_namespaces(self, namespace=()) -> list[tuple[str, ...]]:
        return [tuple(name.split(".")) for name in self._read_registry()["namespaces"]]

    def list_tables(self, namespace) -> list[tuple[str, ...]]:
        prefix = f"{Catalog.namespace_to_string(namespace)}."
        return [tuple(name.split(".")) for name in sorted(self._read_registry()["tables"]) if name.startswith(prefix)]

    def load_table(self, identifier) -> Table:
        name = ".".join(Catalog.identifier_to_tuple(identifier))
        metadata_location = self._read_registry()["tables"].get(name)
        if metadata_location is None:
            raise NoSuchTableError(name)
        io = self._load_file_io(location=metadata_location)
        metadata = FromInputFile.table_metadata(io.new_input(metadata_location))
        return Table(Catalog.identifier_to_tuple(identifier), metadata, metadata_location, io, self)

    def register_table(self, identifier, metadata_location: str) -> Table:
        registry = self._read_registry()
        registry["tables"][".".join(Catalog.identifier_to_tuple(identifier))] = metadata_location
        self._write_registry(registry)
        return self.load_table(identifier)

    def commit_table(
        self, table: Table, requirements: tuple[TableRequirement, ...], updates: tuple[TableUpdate, ...]
    ) -> CommitTableResponse:
        identifier = Catalog.identifier_to_tuple(table.name())
        try:
            current = self.load_table(identifier)
        except NoSuchTableError:
            current = None
        staged = self._update_and_stage_table(current, identifier, requirements, updates)
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        registry = self._read_registry()
        registry["tables"][".".join(identifier)] = staged.metadata_location
        self._write_registry(registry)
        return CommitTableResponse(metadata=staged.metadata, metadata_location=staged.metadata_location)

    def create_table(
        self, identifier, schema, location=None, partition_spec=None, sort_order=UNSORTED_SORT_ORDER, properties=EMPTY_DICT
    ) -> Table:
        if partition_spec is None:
            from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC

            partition_spec = UNPARTITIONED_PARTITION_SPEC
        return self.create_table_transaction(
            identifier, schema, location, partition_spec, sort_order, properties
        ).commit_transaction()

    def table_exists(self, identifier) -> bool:
        return ".".join(Catalog.identifier_to_tuple(identifier)) in self._read_registry()["tables"]

    def view_exists(self, identifier) -> bool:
        return ".".join(Catalog.identifier_to_tuple(identifier)) in self._read_registry()["views"]

    def list_views(self, namespace) -> list[tuple[str, ...]]:
        prefix = f"{Catalog.namespace_to_string(namespace)}."
        return [tuple(name.split(".")) for name in sorted(self._read_registry()["views"]) if name.startswith(prefix)]

    def drop_view(self, identifier) -> None:
        raise NotImplementedError

    def drop_table(self, identifier) -> None:
        raise NotImplementedError

    def rename_table(self, from_identifier, to_identifier) -> Table:
        raise NotImplementedError

    def drop_namespace(self, namespace) -> None:
        raise NotImplementedError

    def update_namespace_properties(self, namespace, removals=None, updates=EMPTY_DICT):
        raise NotImplementedError


def catalog(warehouse: str | Path) -> TinyCatalog:
    """Open a local result bundle and return a tiny catalog object."""
    root = Path(warehouse)
    warehouse = root if (root / REGISTRY_FILE).exists() else root / "warehouse"
    return TinyCatalog(warehouse)


def create_view(
    warehouse: str | Path,
    name: str,
    sources: list[str],
    join_keys: list[str],
    how: str = "left",
) -> None:
    """Create a simple saved join view inside the registry."""
    if len(sources) < 2:
        raise ValueError("A view needs at least two sources.")
    bundle = catalog(warehouse)
    identifier = ".".join(name.split(".")) if "." in name else f"{NAMESPACE}.{name}"
    qualified_sources = [source if "." in source else f"{NAMESPACE}.{source}" for source in sources]
    joins = [{"source": source, "on": join_keys, "how": how} for source in qualified_sources[1:]]
    registry = bundle._read_registry()
    registry["views"][identifier] = {
        "kind": "pandas_merge",
        "base": qualified_sources[0],
        "joins": joins,
    }
    bundle._write_registry(registry)


def create_join_view(warehouse: str | Path, name: str, base: str, joins: list[dict[str, object]]) -> None:
    """Create a saved view with explicit join steps."""
    bundle = catalog(warehouse)
    qualify = lambda value: value if "." in value else f"{NAMESPACE}.{value}"
    registry = bundle._read_registry()
    registry["views"][qualify(name)] = {
        "kind": "pandas_merge",
        "base": qualify(base),
        "joins": [
            {"source": qualify(str(join["source"])), "on": list(join["on"]), "how": join.get("how", "left")}
            for join in joins
        ],
    }
    bundle._write_registry(registry)


def _read_view(bundle: TinyCatalog, name: str) -> pd.DataFrame:
    spec = bundle._read_registry()["views"][name]
    result = bundle.load_table(tuple(spec["base"].split("."))).scan().to_arrow().to_pandas()
    for join in spec["joins"]:
        frame = bundle.load_table(tuple(join["source"].split("."))).scan().to_arrow().to_pandas()
        result = result.merge(frame, on=join["on"], how=join["how"])
    return result


def tables(warehouse: str | Path, include_views: bool = True) -> list[str]:
    """List fully qualified table names in a result bundle."""
    bundle = catalog(warehouse)
    names = [".".join(identifier) for namespace in bundle.list_namespaces() for identifier in bundle.list_tables(namespace)]
    if include_views:
        names.extend(".".join(identifier) for namespace in bundle.list_namespaces() for identifier in bundle.list_views(namespace))
    return sorted(names)


def read(warehouse: str | Path, table: str) -> pd.DataFrame:
    """Read one Iceberg table into pandas using a short name or full name."""
    bundle = catalog(warehouse)
    name = table if "." in table else f"{NAMESPACE}.{table}"
    if bundle.view_exists(tuple(name.split("."))):
        return _read_view(bundle, name)
    return bundle.load_table(tuple(name.split("."))).scan().to_arrow().to_pandas()


def describe(warehouse: str | Path, include_views: bool = True) -> pd.DataFrame:
    """Return a small summary for each table in the bundle."""
    bundle = catalog(warehouse)
    rows: list[dict[str, object]] = []
    for namespace in bundle.list_namespaces():
        for identifier in bundle.list_tables(namespace):
            table = bundle.load_table(identifier)
            files = table.inspect.files().to_pandas()
            rows.append(
                {
                    "table": ".".join(identifier),
                    "rows": int(files["record_count"].sum()),
                    "data_files": len(files),
                    "snapshot_id": table.current_snapshot().snapshot_id,
                    "kind": "table",
                }
            )
        if include_views:
            for identifier in bundle.list_views(namespace):
                view_name = ".".join(identifier)
                view_frame = _read_view(bundle, view_name)
                rows.append(
                    {
                        "table": view_name,
                        "rows": len(view_frame),
                        "data_files": 0,
                        "snapshot_id": None,
                        "kind": "view",
                    }
                )
    return pd.DataFrame(rows).sort_values("table").reset_index(drop=True)


load_catalog = catalog
list_tables = tables
read_pandas = read
describe_result = describe
