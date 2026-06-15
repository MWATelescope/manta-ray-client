"""Search commands"""

import typer


app = typer.Typer()


@app.command()
def observations() -> None:
    """Search observations"""

    typer.echo("Search command - to be implemented")
