"""
Schema management commands
Fetch OpenAPI schemas and generate Pydantic models
"""

import asyncio
import json
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import typer
from rich.console import Console

from mwa_cli.client.schema import fetch_openapi_schema
from mwa_cli.config import Config, load_cached_schema, save_schema


app = typer.Typer()
console = Console()
logger = logging.getLogger(__name__)


def run_codegen(schema: dict[str, Any], output_path: Path) -> bool:
    """Run datamodel-code-generator to create Pydantic models"""

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, encoding="utf-8"
    ) as tmp:
        json.dump(schema, tmp, indent=2)
        tmp_path = Path(tmp.name)

    try:
        cmd = [
            "datamodel-codegen",
            "--input",
            str(tmp_path),
            "--output",
            str(output_path),
            "--input-file-type",
            "openapi",
            "--output-model-type",
            "pydantic_v2.BaseModel",
            "--use-standard-collections",
            "--use-schema-description",
            "--use-field-description",
            "--field-constraints",
            "--snake-case-field",
        ]

        logger.debug(f"Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        if result.returncode != 0:
            logger.error(f"Code generation failed: {result.stderr}")
            return False

        logger.info(f"Generated models written to {output_path}")
        return True
    finally:
        tmp_path.unlink(missing_ok=True)


@app.command(name="update")
def update_schema_command(
    api_url: str | None = typer.Option(
        None,
        "--api-url",
        help="API base URL (default: from config or https://asvo.mwatelescope.org)",
    ),
    force: bool = typer.Option(False, "--force", help="Force fetch even if cache exists"),
) -> bool:
    """Update OpenAPI schema and regenerate Pydantic models"""

    if api_url is None:
        api_url = Config.get_openapi_url()

    console.print(f"[bold]Fetching OpenAPI schema from:[/bold] {api_url}")

    try:
        # check if we should use cache
        if not force:
            cached = load_cached_schema()
            if cached:
                console.print("[yellow]Cached schema found. Use --force to fetch latest[/yellow]")
                schema = cached
            else:
                schema = asyncio.run(fetch_openapi_schema(api_url))
                save_schema(schema)
        else:
            schema = asyncio.run(fetch_openapi_schema(api_url))
            save_schema(schema)

        info = schema.get("info", {})
        console.print(f"[green][+[/green] Schema: {info.get('title', 'Unknown')}")
        console.print(f"[green]+[/green] Version: {info.get('version', 'Unknown')}")

        console.print("\n[bold]Generating Pydantic models...[/bold]")

        models_path = Path(__file__).parent.parent / "models" / "generated.py"
        success = run_codegen(schema, models_path)

        if success:
            console.print(f"[green]+[/green] Models generated: {models_path}")

            if models_path.exists():
                content = models_path.read_text()
                class_count = content.count("class ")
                console.print(f"[green]+[/green] Generated {class_count} model classes")

            console.print("\n[bold green]Schema update completed![/bold green]")
            return True
        else:
            console.print("[bold red]X Model generation failed[/bold red]")
            return False

    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {e}")
        logger.exception("Schema update failed")
        return False


@app.command(name="info")
def schema_info() -> None:
    """Show information about cached OpenAPI schema"""

    schema = load_cached_schema()
    if schema is None:
        console.print("[yellow]No cached schema found.[/yellow]")
        console.print("Run [bold]mwa-cli schema update[/bold] to fetch schema")
        return

    info = schema.get("info", {})

    console.print("[bold]Cached Schema Information[/bold]\n")
    console.print(f"Title {info.get('title', 'Unknown')}")
    console.print(f"Version: {info.get('version', 'Unknown')}")
    console.print(f"Description: {info.get('description', 'N/A')}")

    # count endpoints
    paths = schema.get("paths", {})
    console.print(f"\nEndpoints: {len(paths)}")

    # count schemas
    schemas = schema.get("components", {}).get("schemas", {})
    console.print(f"Schemas: {len(schemas)}")

    cache_path = Config.get_schema_cache_path()
    console.print(f"\nCache location: {cache_path}")
