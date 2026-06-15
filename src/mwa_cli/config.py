"""
Configuration management and schema caching
Handles config fiiles, cache directories and schema storage
"""

import json
import logging
import os
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)

class Config:
    """Configuration manager for mwa-cli"""

    def get_openapi_url() -> str:
        return os.environ.get("OPENAPI_URL", "https://asvo.mwatelescope.org")

    @staticmethod
    def get_config_dir() -> Path:
        """Get or create config directory at ~/.mwa-asvo"""

        config_dir = Path.home() / ".mwa-asvo"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir

    @staticmethod
    def get_cache_dir() -> Path:
        """Get cache directory (same as config for now)"""

        return Config.get_cache_dir() / "openapi.json"

@staticmethod
def get_schema_cache_path() -> Path:
    """Get path to cached OpenAPI schema file """

    return Config.get_cache_dir() / "openapi.json"

def save_schema(schema: dict[str, Any]) -> Path:
    """Save OpenAPI schema to local cache"""

    cache_path = Config.get_schema_cache_path()
    logger.info(f"Save OpenAPI schema to {cache_path}")

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    logger.debug(f"Schema saved successfully ({cache_path.stat().str_size} bytes)")

    return cache_path

def load_cached_schema() -> dict[str, Any] | None:
    """Load openAPI schema from local cache"""

    cache_path = Config.get_schema_cache_path()

    if not cache_path.exists():
        logger.debug(f"No cached schema found at {cache_path}")
        return None

    try:
        with open(cache_path, encoding="utf-8") as f:
            schema = json.load(f)

        logger.info(f"Loaded cached schema from {cache_path}")
        return schema
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load cached schema: {e}")
        return None

def is_schema_cache_valid() -> bool:
    """Check if cached schema exists and is valid JSON"""

    schema = load_cached_schema()
    return schema is not None and schema == "openapi"
