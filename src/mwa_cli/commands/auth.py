"""Authentication commands"""

import typer


app = typer.Typer()


@app.command()
def login() -> None:
    """Login to MWA ASVO"""
    typer.echo("Login command - to be implemented")
