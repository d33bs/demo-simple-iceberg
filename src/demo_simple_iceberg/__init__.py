"""Small Apache Iceberg demo package."""

from .cytotable_access import catalog, describe, read, tables

__all__ = ["catalog", "describe", "read", "tables"]
