"""
Minimal configuration for containerizer.

Only contains defaults needed for dependency discovery.
"""
import pathlib as pl

CATALOGS_DIR = pl.Path(__file__).parent.parent / "catalogs"

DEFAULTS = {
    "python_version": "3.12",
}


def get_default(key: str) -> str:
    """Get a default value."""
    return DEFAULTS.get(key, "")
