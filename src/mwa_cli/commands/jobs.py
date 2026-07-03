"""Job management commands"""

import asyncio
import json
import logging
import os
from typing import Any

import typer
from rich.console import Console
from rich.table import Table

from mwa_cli.client.base import BaseClient
from mwa_cli.client.errors import APIError
from mwa_cli.client.jobs import JobsClient
from mwa_cli.config import Config
from mwa_cli.models.generated import JobsByUserRequest


app = typer.Typer()
console = Console()
logger = logging.getLogger(__name__)

JOB_TYPES = [
    "conversion",
    "vis_download",
    "meta_download",
    "voltage",
    "cancel",
    "beamformer",
    "imaging",
]


async def do_list(api_url: str, params: JobsByUserRequest) -> dict[str, Any]:
    """Get user's jobs list"""

    async with BaseClient(api_url, verify=Config().verify_ssl()) as base_client:
        jobs_client = JobsClient(base_client)
        return await jobs_client.list_jobs(params)


async def do_show(
    api_url: str, job_id: int = typer.Argument(..., help="Job Id")
) -> dict[str, Any]:
    """Show detailed info about a job"""

    async with BaseClient(api_url, verify=Config().verify_ssl()) as base_client:
        jobs_client = JobsClient(base_client)
        return await jobs_client.get_job(job_id)


@app.command(name="list")
def list_jobs(
    api_url: str | None = typer.Option(None, "--api-url", help="The base URL of the MWA ASVO API"),
    state: str | None = typer.Option(
        None, "--state", help="Filter by state: queued, processing, completed, error, cancelled"
    ),
    job_type: str | None = typer.Option(
        None,
        "--type",
        help="Filter by type: conversion, vis_download, meta_download, voltage, beamformer, imaging",
    ),
    limit: int = typer.Option(50, "--limit", help="Maximum results"),
    offset: int = typer.Option(0, "--offset", help="Pagination offset"),
) -> None:
    """
    List your jobs with optional filters
    """

    if api_url is None:
        api_url = os.environ.get("MWA_ASVO_HOST")

    if api_url is None:
        api_url = "https://asvo.mwatelescope.org"

    try:
        mapped_type = None
        if job_type:
            mapped_type = JOB_TYPES.index(job_type)

        params = JobsByUserRequest(
            job_state=state, job_type=mapped_type, limit=limit, offset=offset
        )
        result = asyncio.run(do_list(api_url, params))
        jobs = result["jobs"]

        if not jobs:
            console.print("[yellow]No jobs found[/yellow]")
            return

        table = Table(title="Your jobs")
        table.add_column("Job ID", style="cyan")
        table.add_column("Type", style="blue")
        table.add_column("State", style="green")
        table.add_column("Created", style="yellow")

        for job in jobs:
            state_str = job.get("job_state", "unknown")
            state_display = f"[blue]{state_str}[/blue]"

            if state_str == "completed":
                state_display = f"[green]{state_str}[/green]"

            if state_str == "error":
                state_display = f"[red]{state_str}[/red]"

            if state_str == "cancelled":
                state_display = f"[white]{state_str}[/white]"

            table.add_row(str(job.get("id", "id")), job_type, state_display, job.get("created", ""))

        console.print(table)

    except APIError as e:
        console.print(f"[red]x[/red] {e.message}")
        raise typer.Exit(1)  # noqa: B904

    except Exception as e:
        console.print(f"[bold red]x[/bold red] Unable to list jobs: {e}")
        raise typer.Exit(1)  # noqa: B904


@app.command(name="show")
def show_job(
    api_url: str | None = typer.Option(None, "--api-url", help="The base URL of the MWA ASVO API"),
    job_id: int = typer.Argument(..., help="Job ID"),
) -> None:
    """Show detailed information about a job"""

    try:
        if api_url is None:
            api_url = os.environ.get("MWA_ASVO_HOST")

        if api_url is None:
            api_url = "https://asvo.mwatelescope.org"

        job: Any = asyncio.run(do_show(api_url=api_url, job_id=job_id))

        console.print(f"\n[bold]Job {job['id']}[/bold]\n")

        console.print(f"Type:    {JOB_TYPES[job.get('job_type', 1)]}")
        console.print(f"State:   {job.get('job_state', 'unknown')}")
        console.print(f"Created: {job.get('created', 'N/A')}")

        if job.get("completed"):
            console.print(f"Completed: {job['completed']}")

        if job.get("job_params"):
            console.print("\n[bold]Parameters:[/bold]")
            console.print(json.dumps(job["job_params"], indent=2))

        if job.get("error_text"):
            console.print("\n[bold red]Error:[/bold red]")
            console.print(job["error_text"])

        if job.get("results"):
            console.print("\n[bold green]Results:[/bold green]")
            console.print(job["results"])

    except APIError as e:
        console.print(f"[red]x[/red] {e.message}")
        raise typer.Exit(1)  # noqa: B904

    except Exception as e:
        console.print(f"[bold red]x[/bold red] Unable to fetch job info: {e}")
        raise typer.Exit(1)  # noqa: B904
