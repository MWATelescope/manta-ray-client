"""Job management commands"""

import typer


app = typer.Typer()


@app.command()
def list_jobs() -> None:
    """List jobs"""
    typer.echo("Jobs list command - to be implemented")
