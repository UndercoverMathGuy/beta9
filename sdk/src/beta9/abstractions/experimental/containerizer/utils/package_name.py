"""
Import name to package name resolution.

This module provides utilities for mapping Python import names to their corresponding
PyPI package names. Many packages have import names that differ from their PyPI names
(e.g., `import cv2` comes from `opencv-python`, `from PIL import Image` comes from `Pillow`).

The resolution is fully offline - no network calls are made. It uses:
    1. importlib.metadata - For packages installed in the current environment
    2. Static import map - For common import != package mappings
    3. Direct match - Assumes import name == package name as fallback

Example:
    ```python
    from containerizer.utils.package_name import resolve_import_to_package

    # Resolve PIL to Pillow
    pkg_name, version, is_resolved, method = resolve_import_to_package("PIL")
    print(pkg_name)  # "Pillow"

    # Get version if installed
    from containerizer.utils.package_name import get_installed_package_version
    version = get_installed_package_version("requests")
    print(version)  # "2.31.0"
    ```
"""
import functools
from importlib.metadata import distributions, packages_distributions
from typing import Dict, Optional, List, Tuple


# Static mapping for common imports that differ from package names.
# This works even when packages aren't installed locally.
STATIC_IMPORT_MAP: Dict[str, Optional[str]] = {
    # Image/Vision
    "pil": "Pillow",
    "cv2": "opencv-python",
    "skimage": "scikit-image",
    
    # ML/Data Science
    "sklearn": "scikit-learn",
    "tf": "tensorflow",
    
    # Web/Parsing
    "bs4": "beautifulsoup4",
    "lxml": "lxml",
    
    # Config/Serialization
    "yaml": "PyYAML",
    "toml": "toml",
    "msgpack": "msgpack",
    "ujson": "ujson",
    "rapidjson": "python-rapidjson",
    "orjson": "orjson",
    
    # Date/Time
    "dateutil": "python-dateutil",
    "arrow": "arrow",
    "pendulum": "pendulum",
    
    # Environment/Config
    "dotenv": "python-dotenv",
    "decouple": "python-decouple",
    
    # Auth/Crypto
    "jwt": "PyJWT",
    "jose": "python-jose",
    "nacl": "PyNaCl",
    "OpenSSL": "pyOpenSSL",
    "crypto": "pycryptodome",
    "Crypto": "pycryptodome",
    
    # Database
    "psycopg2": "psycopg2-binary",
    "MySQLdb": "mysqlclient",
    "pymysql": "PyMySQL",
    "redis": "redis",
    "pymongo": "pymongo",
    
    # Documents
    "docx": "python-docx",
    "pptx": "python-pptx",
    "fitz": "PyMuPDF",
    "PyPDF2": "PyPDF2",
    "pypdf": "pypdf",
    
    # System/Hardware
    "serial": "pyserial",
    "usb": "pyusb",
    "magic": "python-magic",
    "psutil": "psutil",
    
    # GUI
    "wx": "wxPython",
    "gi": "PyGObject",
    "cairo": "pycairo",
    "tkinter": None,  # stdlib
    
    # Network
    "socks": "PySocks",
    "dns": "dnspython",
    "websocket": "websocket-client",
    "websockets": "websockets",
    
    # Compression
    "lz4": "lz4",
    "zstd": "zstandard",
    "snappy": "python-snappy",
    "brotli": "Brotli",
    
    # CLI/Terminal
    "colorama": "colorama",
    "termcolor": "termcolor",
    "rich": "rich",
    "click": "click",
    "typer": "typer",
    "fire": "fire",
    "tqdm": "tqdm",
    
    # Logging
    "loguru": "loguru",
    "structlog": "structlog",
    
    # Testing
    "pytest": "pytest",
    "mock": "mock",
    "faker": "Faker",
    "hypothesis": "hypothesis",
    
    # Async
    "aiohttp": "aiohttp",
    "httpx": "httpx",
    "aiofiles": "aiofiles",
    "anyio": "anyio",
    "trio": "trio",
    
    # Data validation
    "attr": "attrs",
    "attrs": "attrs",
    "pydantic": "pydantic",
    "marshmallow": "marshmallow",
    
    # Templating
    "jinja2": "Jinja2",
    "mako": "Mako",
    
    # Markdown/Docs
    "markdown": "Markdown",
    "pygments": "Pygments",
    
    #TODO - Improve Google package mapping
    # Google (note: "google" is ambiguous - could be many packages)
    # Only map specific submodules, not the bare "google" import
    "googleapiclient": "google-api-python-client",
    "google_auth": "google-auth",
    "google.auth": "google-auth",
    "google.cloud": "google-cloud-core",
    
    # AWS
    "boto3": "boto3",
    "botocore": "botocore",
    
    # HuggingFace
    "transformers": "transformers",
    "datasets": "datasets",
    "huggingface_hub": "huggingface-hub",
    "tokenizers": "tokenizers",
    "accelerate": "accelerate",
    "diffusers": "diffusers",
    
    # Audio
    "soundfile": "soundfile",
    "librosa": "librosa",
    "pydub": "pydub",
    "pyaudio": "PyAudio",
    
    # Video
    "moviepy": "moviepy",
    "imageio": "imageio",
    "imageio_ffmpeg": "imageio-ffmpeg",
    
    # Geo
    "shapely": "shapely",
    "fiona": "fiona",
    "pyproj": "pyproj",
    "geopandas": "geopandas",
    "rasterio": "rasterio",
    
    # Multipart/Forms
    "multipart": "python-multipart",
}


@functools.lru_cache(maxsize=16)
def _build_import_to_package_map() -> Dict[str, List[str]]:
    """
    Build mapping from import names to package names using packages_distributions.

    Uses importlib.metadata to discover which packages provide which import names
    in the current Python environment.

    Returns:
        Dict[str, List[str]]: Mapping of lowercase import name to list of package names
            that provide that import.
    """
    try:
        return {k.lower(): v for k, v in packages_distributions().items()}
    except Exception:
        return {}


@functools.lru_cache(maxsize=16)
def _build_package_metadata_map() -> Dict[str, Dict]:
    """
    Build mapping of package names to their metadata.

    Iterates through all installed distributions and extracts name and version
    information for quick lookup.

    Returns:
        Dict[str, Dict]: Mapping of lowercase package name to metadata dict
            containing 'name' (original casing) and 'version'.
    """
    pkg_map = {}
    try:
        for dist in distributions():
            name = dist.metadata.get("Name", "").lower()
            version = dist.metadata.get("Version", "")
            if name:
                pkg_map[name] = {
                    "name": dist.metadata.get("Name"),  # Original casing
                    "version": version,
                }
    except Exception:
        pass
    return pkg_map


def get_installed_package_version(package_name: str) -> Optional[str]:
    """
    Get the installed version of a package from importlib.metadata.

    Parameters:
        package_name (str):
            The package name to look up (case-insensitive).

    Returns:
        Optional[str]: Version string if the package is installed, None otherwise.

    Example:
        ```python
        version = get_installed_package_version("requests")
        print(version)  # "2.31.0"
        ```
    """
    pkg_meta = _build_package_metadata_map()
    meta = pkg_meta.get(package_name.lower(), {})
    return meta.get("version")


def resolve_import_to_package(import_name: str) -> Tuple[str, Optional[str], bool, str]:
    """
    Resolve an import name to its PyPI package name.

    Many Python packages have import names that differ from their PyPI package names.
    This function attempts to resolve the correct package name using multiple strategies,
    all without making network calls.

    Resolution order:
        1. importlib.metadata - Check installed packages for the import
        2. Static import map - Use hardcoded mappings for common packages
        3. Direct match - Check if import name matches an installed package
        4. Hyphenated variant - Try converting underscores to hyphens

    Parameters:
        import_name (str):
            The import name to resolve (e.g., "PIL", "cv2", "sklearn").

    Returns:
        Tuple[str, Optional[str], bool, str]: A tuple containing:
            - package_name (str): The resolved PyPI package name
            - version (Optional[str]): Version string if known, None otherwise
            - is_resolved (bool): True if confidently resolved, False if guessed
            - resolution_method (str): How it was resolved ("importlib.metadata",
              "static_map", "direct_match", or "assumed")

    Example:
        ```python
        pkg, version, resolved, method = resolve_import_to_package("PIL")
        print(pkg)  # "Pillow"
        print(method)  # "static_map"

        pkg, version, resolved, method = resolve_import_to_package("requests")
        print(pkg)  # "requests"
        print(version)  # "2.31.0" (if installed)
        ```
    """
    import_lower = import_name.lower()
    pkg_meta = _build_package_metadata_map()

    # 1. Try importlib.metadata first (installed packages)
    import_map = _build_import_to_package_map()
    if import_lower in import_map:
        pkg_names = import_map[import_lower]
        if pkg_names:
            pkg_name = pkg_names[0]
            meta = pkg_meta.get(pkg_name.lower(), {})
            return meta.get("name", pkg_name), meta.get("version"), True, "importlib.metadata"
    
    # 2. Try static import map (works without installation)
    if import_lower in STATIC_IMPORT_MAP:
        pkg_name = STATIC_IMPORT_MAP[import_lower]
        if pkg_name is None:  # Explicitly marked as stdlib
            return import_name, None, True, "stdlib"
        # Try to get version if installed
        meta = pkg_meta.get(pkg_name.lower(), {})
        return meta.get("name", pkg_name), meta.get("version"), True, "static_map"
    
    # 3. Try direct match on package name (import name == package name)
    meta = pkg_meta.get(import_lower, {})
    if meta:
        return meta.get("name", import_name), meta.get("version"), True, "direct_match"
    
    # 4. Try hyphenated variant as common convention
    hyphenated = import_name.replace("_", "-")
    if hyphenated != import_name:
        meta = pkg_meta.get(hyphenated.lower(), {})
        if meta:
            return meta.get("name", hyphenated), meta.get("version"), True, "direct_match"
    
    # Return import name as package name - not confidently resolved
    return import_name, None, False, "assumed"


def get_import_override(import_name: str) -> str:
    """
    Get the package name for an import.

    Convenience function that returns just the package name without version
    or resolution metadata.

    Parameters:
        import_name (str):
            The import name to resolve.

    Returns:
        str: The PyPI package name (may be same as import_name if no mapping exists).
    """
    pkg_name, _, _, _ = resolve_import_to_package(import_name)
    return pkg_name


def get_static_package_name(import_name: str) -> Optional[str]:
    """
    Get package name from static map only (no metadata lookup).

    Useful when you want to check if an import has a known mapping without
    querying the installed packages.

    Parameters:
        import_name (str):
            The import name to look up.

    Returns:
        Optional[str]: Package name if found in static map, None otherwise.
    """
    return STATIC_IMPORT_MAP.get(import_name.lower())
