"""
Configuration management and schema caching
Handles config fiiles, cache directories and schema storage
"""

import json
import logging
import os
from importlib import metadata
from pathlib import Path
from typing import Any


logger = logging.getLogger(__name__)


class Config:
    """Configuration manager for mwa-cli"""

    @staticmethod
    def get_openapi_url() -> str:
        return os.environ.get("OPENAPI_URL", "https://asvo.mwatelescope.org")

    @staticmethod
    def verify_ssl() -> bool:
        verify_ssl = os.environ.get("SSL_VERIFY", True)

        if isinstance(verify_ssl, str):
            if verify_ssl in ["1", "true", "yes", "on", "enable"]:
                return True

            if verify_ssl in ["0", "false", "no", "off", "disable"]:
                return False

        if isinstance(verify_ssl, int):
            if verify_ssl == 0:
                return False

            if verify_ssl == 1:
                return True

        return bool(verify_ssl)

    @staticmethod
    def get_config_dir() -> Path:
        """Get or create config directory at ~/.mwa-asvo"""

        config_dir = Path.home() / ".mwa-asvo"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir

    @staticmethod
    def get_cache_dir() -> Path:
        """Get cache directory (same as config for now)"""

        return Config.get_config_dir()

    @staticmethod
    def get_schema_cache_path() -> Path:
        """Get path to cached OpenAPI schema file"""

        return Config.get_cache_dir() / "openapi.json"

    @staticmethod
    def get_cli_version() -> str:
        """Get the name of the CLI and its version to verify compatibility with AVO API"""

        version = ""

        try:
            version = metadata.version("manta-ray-client")
        except Exception:
            version = "unknown"

        version_parts = version.split(".")

        return f"mantaray-clientv{version_parts[0]}.{version_parts[1]}"


def save_schema(schema: dict[str, Any]) -> Path:
    """Save OpenAPI schema to local cache"""

    cache_path = Config.get_schema_cache_path()
    logger.info(f"Save OpenAPI schema to {cache_path}")

    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    logger.debug(f"Schema saved successfully ({cache_path.stat().st_size} bytes)")

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
        if isinstance(schema, dict):
            return schema

        raise Exception("Unabe to load cached schema - schema must be a dict")
    except (OSError, json.JSONDecodeError) as e:
        logger.error(f"Failed to load cached schema: {e}")
        return None


def is_schema_cache_valid() -> bool:
    """Check if cached schema exists and is valid JSON"""

    schema = load_cached_schema()
    return schema is not None and schema.get("openapi", "unknown") == "openapi"
