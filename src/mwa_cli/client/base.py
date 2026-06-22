"""
Base HTTP client with authentication middleware.
Handles automatic token injection, refresh, retries and error parsing
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import TracebackType
from typing import Any

import httpx
from rich.console import Console

from mwa_cli.auth_store import TokenData, TokenStore
from mwa_cli.client.errors import APIError, parse_api_error


logger = logging.getLogger(__name__)
console = Console()

class BaseClient:
    """Async client with authentication and retry logic"""

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        max_retries: int = 3,
        verbose: bool = False
    ):
        """Initialize HTTP client"""

        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.verbose = verbose

        self.token_store = TokenStore()
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "BaseClient":
        """Context manager entry"""

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True
        )

        return self

    async def __aexit__(
            self,
            exc_type: type[BaseException] | None,
            exc_val: BaseException | None,
            exc_tb: TracebackType | None
        ) -> None:
        """Context manager exit"""

        if self._client:
            await self._client.aclose()

    def _get_auth_cookies(self) -> dict[str, str]:
        """Get authentication cookies from token store"""

        token_data = self.token_store.load()
        if token_data is None:
            return {}

        if token_data.is_access_token_expired():
            logger.debug("Access token expired, will be refreshed on request")

        return {"access_token": token_data.access_token}

    async def _refresh_tokens(self) -> bool:
        """
        Refresh access token using refresh token

        Returns: True if refresh succeeded, False otherwise
        """

        token_data = self.token_store.load()
        if token_data is None or token_data.is_refresh_token_expired():
            logger.error("Cannot refresh: no valid refresh token")
            return False

        try:
            logger.info("Refreshing access token...")
            refresh_url = f"{self.base_url}/api/auth/refresh"

            if not self._client:
                logger.error("Unable to refresh tokens: base httpx client not found")
                return False

            response = await self._client.post(
                refresh_url,
                cookies={"refresh_token": token_data.refresh_token}
            )

            # extract new tokens from response cookies
            new_access = response.cookies.get("access_token")
            new_refresh = response.cookies.get("refresh_token")

            if not new_access:
                logger.error("Refresh response missing access_token")
                return False

            # Update stored tokens
            now = datetime.now(timezone.utc)

            updated_data = TokenData(
                access_token=new_access,
                refresh_token=new_refresh or token_data.refresh_token,
                access_expires_at=(now + timedelta(minutes=15)).isoformat() + "Z",
                refresh_expires_at=token_data.refresh_expires_at,
                user_id=token_data.user_id,
                user_login=token_data.user_login,
                user_email=token_data.user_email
            )

            self.token_store.save(updated_data)
            logger.info("Access token refreshed successfully")

            return True

        except Exception as e:
            logging.error(f"Token refresh failed: {e}")
            return False

    async def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Make HTTP request with authentication and retry logic"""

        if not self._client:
            raise RuntimeError("Client not initialized. Use 'async with' context manager")

        # Inject authentication cookies
        cookies = kwargs.get("cookies", {})
        cookies.update(self._get_auth_cookies())
        kwargs["cookies"] = cookies

        url = path if path.startswith("http") else f"{self.base_url}{path}"

        if self.verbose:
            logger.info(f"{method} {url}")
            if "json" in kwargs:
                logger.debug(f"Request body: {kwargs['json']}")

        # Retry look
        last_error = None

        for attempt in range(self.max_retries):
            try:
                response = await self._client.request(method, url, **kwargs)

                if self.verbose:
                    logger.info(f"Response: {response.status_code}")

                # Handle 401 Unauthorized - try token refresh
                if response.status_code == 401:
                    logger.warning("Received 401 Unauthorized, attempting token refresh")

                    if await self._refresh_tokens():
                        # Retry with new token
                        cookies.update(self._get_auth_cookies())
                        kwargs["cookies"] = cookies

                        response = await self._client.request(method, url, **kwargs)
                    else:
                        # Refresh failed, user needs to re-login
                        console.print("[red]Authentication expired. Please login again:[/red]")
                        console.print(" [bold]mwa-cli auth login[/bold]")

                        raise APIError(
                            status_code=401,
                            message="Authentication expired. Please run 'mwa-cli auth login'",
                            error_code=None
                        )

                # raise for other HTTP errors
                response.raise_for_status()

                return response

            except httpx.HTTPStatusError as e:
                error = parse_api_error(e.response)
                raise error from e

            except (httpx.ConnectError, httpx.TimeoutException) as e:
                last_error = e
                logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}")

                if attempt < self.max_retries - 1:
                    # exponential backoff
                    await asyncio.sleep(2 ** attempt)
                    continue
                else:
                    raise APIError(
                        status_code=0,
                        message=f"Connection failed after {self.max_retries} attempts: {e}",
                        error_code=None,
                        field_errors=None
                    ) from e

        # should not reach here
        if last_error:
            raise last_error

        raise APIError(
            status_code=0,
            message="An unknown error occurred",
            error_code=None,
            field_errors=None
        )

    async def get(self, path: str, **kwargs: Any) -> httpx.Response:
        """GET request"""
        return await self._request("GET", path, **kwargs)


    async def post(self, path: str, **kwargs: Any) -> httpx.Response:
        """POST request"""
        return await self._request("POST", path, **kwargs)

    async def delete(self, path: str, **kwargs: Any) -> httpx.Response:
        """DELETE request"""
        return await self._request("DELETE", path, **kwargs)

    async def put(self, path: str, **kwargs: Any) -> httpx.Response:
        """PUT request"""
        return await self._request("PUT", path, **kwargs)
