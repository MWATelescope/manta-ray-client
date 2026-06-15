"""
Main CLI entry point for mwa-cli
Configures Typer application with command groups and global options
"""

import logging

import typer
from rich.console import Console

from mwa_cli import __version__
from mwa_cli.commands import auth, jobs, schema, search


# Global state for verbose/debug flags (passed via context)
class GlobalState:
    """Global state shared across commands"""

    verbose: bool = False
    debug: bool = False
    output_format: str = "table"


app = typer.Typer(
    name="mwa-cli",
    help="Modern CLI for MWA ASVO - Search observations, submit jobs, monitor processing",
    add_completion=False,
    no_args_is_help=True,
    rich_markup_mode="rich",
)

console = Console()
state = GlobalState()

def version_callback(value: bool) -> None:
    if value:
        console.print(f"[bold]mwa-cli[/bold] version {__version__}")
        raise typer.Exit(0)

@app.callback()
def main(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable verbose output"),
    debug: bool = typer.Option(False, "--debug", help="Enable debug logging"),
    output: str = typer.Option(
        "table",
        "--output",
        "-o",
        help="Output format: table, json, csv",
    ),
    version: bool | None = typer.Option(
        None,
        "--version",
        "-v",
        callback=version_callback,
        help="Show version and exit",
        is_eager=True,
    ),
) -> None:
    """MWA ASVO CLI - Modern command-line interface for radio astronomy data access"""

    # Store global flags in state
    state.verbose = verbose
    state.debug = debug
    state.output_format = output

    # configure logging based on flags
    if debug:
        logging.basicConfig(
            level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    elif verbose:
        logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# Import and register command groups
# The imports happen after the app is created to avoid circular imports
def register_commands() -> None:
    """Register all command groups"""

    try:
        app.add_typer(auth.app, name="auth", help="Authentication commands")
        app.add_typer(jobs.app, name="jobs", help="Job management commands")
        app.add_typer(search.app, name="search", help="observation search commands")
        app.add_typer(schema.app, name="schema", help="Generate pydantic types based on openapi schemas")

    except ImportError:
        # todo
        pass

# entry point for console script

def app_entry() -> None:
    """Entry point for CLI"""

    register_commands()
    app()

if __name__ == "__main__":
    app_entry()
