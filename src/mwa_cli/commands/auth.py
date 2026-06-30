"""
Authentication commands: login, logout, status
Handles JWT authentication with MWA ASVO API
"""

import asyncio
import logging
import os
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import typer
from rich.console import Console
from rich.prompt import Prompt

from mwa_cli.auth_store import TokenData, TokenStore
from mwa_cli.config import Config
from mwa_cli.utils.dates import format_expiry


app = typer.Typer()
console = Console()
logger = logging.getLogger(__name__)


async def do_login(api_key: str, api_url: str = "https://asvo.mwatelescope.org") -> dict[str, Any]:
    """Perform login request to ASVO server"""

    login_url = f"{api_url.rstrip('/')}/api/v2/api_login"
    logger.info(f"Logging in to {login_url}")
    verify = Config.verify_ssl()

    async with httpx.AsyncClient(verify=verify) as client:
        response = await client.post(
            login_url,
            json={"login": Config.get_cli_version(), "password": api_key},
            follow_redirects=True,
        )

        response.raise_for_status()

        access_token = response.cookies.get("mwa_access_token")
        refresh_token = response.cookies.get("mwa_refresh_token")

        if not access_token or not refresh_token:
            raise ValueError("Login succeeded but tokens not found in response cookies")

        try:
            body = response.json()
            user_data = body.get("user", {})
        except Exception:
            user_data = {"id": 0, "login": "n/a", "email": ""}

        now = datetime.now(UTC)
        access_expiry = now + timedelta(minutes=15)
        refresh_expiry = now + timedelta(days=7)

        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "access_expires_at": access_expiry.isoformat() + "Z",
            "refresh_expires_at": refresh_expiry.isoformat() + "Z",
            "user": {
                "id": user_data.get("id", 0),
                "login": user_data.get("login", "unknown"),
                "email": user_data.get("email", ""),
            },
        }


@app.command(name="login")
def login(
    api_url: str | None = typer.Option(None, "--api-url", help="Base API URL"),
    api_key: str | None = typer.Option(None, "--api-key", help="API key for authentication"),
) -> None:
    """Login to MWA ASVO"""

    if not api_key:
        api_key = os.environ.get("MWA_ASVO_API_KEY", None)
        if not api_key:
            api_key = Prompt.ask("API key", password=True)

    if not api_key:
        console.print("[red]Error:[/red] Password cannot be empty")
        raise typer.Exit(1)

    # Determine API URL
    if api_url is None:
        api_url = os.environ.get("MWA_ASVO_HOST")

    if api_url is None:
        api_url = "https://asvo.mwatelescope.org"

    try:
        console.print(f"[dim]Authenticating with {api_url}...[/dim]")
        result = asyncio.run(do_login(api_key, api_url))

        token_store = TokenStore()
        user = result["user"]

        token_data = TokenData(
            access_token=result["access_token"],
            refresh_token=result["refresh_token"],
            access_expires_at=result["access_expires_at"],
            refresh_expires_at=result["refresh_expires_at"],
            user_id=user["id"],
            user_login=user["login"],
            user_email=user["email"],
        )

        token_store.save(token_data)

        console.print(f"[bold green]+[/bold green] Logged in as [bold]{user['login']}[/bold]")
        console.print(f"Email {user['email']}")
        console.print(f"Token expires: {result['access_expires_at']}")

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 401:
            console.print("[bold red]-[/bold red] Authentication failed: invalid credentials")
        else:
            console.print(f"[bold red]-[/bold red] HTTP error: {e.response.status_code}")
        raise typer.Exit(1)  # noqa: B904
    except httpx.ConnectError as e:
        console.print(f"[bold red]-[/bold red] Cannot connect to {api_url}")
        logger.error(e)
        raise typer.Exit(1)  # noqa: B904
    except Exception as e:
        console.print(f"[bold red]-[/bold red] Login failed: {e}")
        logger.exception("Login error")
        raise typer.Exit(1)  # noqa: B904


@app.command(name="status")
def status() -> None:
    """
    Show current authentication status and token information
    Displays logged-in user, email and token expiry times
    """

    token_store = TokenStore()
    token_data = token_store.load()

    if token_data is None:
        console.print("[yellow]Not logged in[/yellow]")
        console.print("\nRun [bold]mwa-cli auth login[/bold] to authenticate")
        return

    console.print("[bold]Authentication Status[/bold]\n")

    console.print(f"User: {token_data.user_login}")
    console.print(f"Email: {token_data.user_email}")
    console.print(f"ID: {token_data.user_id}\n")

    console.print(f"Access token: {format_expiry(token_data.access_expires_at)}")
    console.print(f"Refresh token: {format_expiry(token_data.refresh_expires_at)}")

    # check if tokens are expired
    if token_data.is_access_token_expired():
        console.print("\n[yellow]!![/yellow] Access token has expired")

        if not token_data.is_refresh_token_expired():
            console.print("Token will be refreshed automatically on next API call")
        else:
            console.print("[red]Refresh token also expired - please login again[/red]")
            console.print("Run: [bold]mwa-cli auth login[/bold]")


@app.command(name="logout")
def logout(
    api_url: str | None = typer.Option(
        None, "--api-url", help="API base URL (default: from config)"
    ),
) -> None:
    """Logout and clear stored authentication tokens"""

    token_store = TokenStore()
    token_data = token_store.load()

    if token_data is None:
        console.print("[yellow]Not currently logged in[/yellow]")
        return

    if api_url is None:
        api_url = "https://asvo.mwatelescope.org"

    try:
        logout_url = f"{api_url.rstrip('/')}/api/auth/logout"
        response = httpx.get(
            logout_url,
            cookies={"access_token": token_data.access_token},
            timeout=0.5,
            verify=True,
        )

        if response.status_code == 200:
            logger.debug("Server logout successful")

    except Exception as e:
        # Don't fail logout if server request fails
        logger.debug(f"Server logout failed (not critical): {e}")

    # Clear local tokens (this is the important part)
    token_store.clear()

    console.print("[bold green]✓[/bold green] Logged out successfully")
    console.print(f"User {token_data.user_login} logged out")
