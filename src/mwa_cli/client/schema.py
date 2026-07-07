"""
OpenAPI schema fetching and management
Downloads schemas from FastAPI server for local model generation
"""

import logging
from typing import Any

import httpx

from mwa_cli.config import Config


logger = logging.getLogger(__name__)


async def fetch_openapi_schema(api_url: str, timeout: float = 30.0) -> dict[str, Any]:
    """Fetch OpenAPI schema from FastAPI server"""

    schema_url = f"{api_url.rstrip('/')}/openapi.json"
    logger.info(f"Fetching OpenAPI schema from {schema_url}")

    async with httpx.AsyncClient(timeout=timeout, verify=Config.verify_ssl()) as client:
        try:
            response = await client.get(schema_url)
            response.raise_for_status()

            schema = response.json()

            if "openapi" not in schema:
                raise ValueError("Invalid OpenAPI schema: missing 'openapi' field")

            if "info" not in schema:
                raise ValueError("Invalid OpenAPI schema: missing 'info' field")

            if not isinstance(schema, dict):
                raise ValueError("Invalid OpenAPI schema: 'schema' must be a dictionary")

            logger.info(
                f"Successfully fetched schema: {schema['info'].get('title', 'Unknown')} "
                f"v{schema['info'].get('version', 'Unknown')}"
            )

            return schema

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error fetching schema: {e.response.status_code}")
            raise
        except httpx.ConnectError:
            logger.error(f"Connection error: Cannot reach {schema_url}")
            raise
        except httpx.TimeoutException:
            logger.error(f"Timeout fetching schema from {schema_url}")
            raise
        except ValueError as e:
            logger.error(f"Invalid json response: {e}")
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred while fetching schema: {e}")
            raise


async def check_schema_version(api_url: str) -> str:
    """Check API version from OpenAPI schema"""

    schema = await fetch_openapi_schema(api_url)
    version = schema.get("info", {}).get("version", "unknown")

    if isinstance(version, str):
        return version

    return "unknown"
