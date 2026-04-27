"""
Lake storage abstraction — backend-agnostic I/O for the medallion pipeline.

Usage:
    from pipeline.storage import get_store

    store = get_store()          # reads LAKE_BACKEND env (default: "local")
    store.write_bronze("bnp", run_id, html_bytes)
    store.write_silver(run_id, silver_table)
    store.write_manifest("silver", run_id, silver_manifest)
    latest = store.get_latest_run_id("silver")
"""

from ._local import LocalStore
from ._protocol import BaseLakeStore, get_store

__all__ = [
    "BaseLakeStore",
    "LocalStore",
    "get_store",
]
