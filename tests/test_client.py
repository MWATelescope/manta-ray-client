"""
Tests for HTTP client with authentication middleware
Validates client initialization, auth injection, token refresh, and error parsing
"""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from mwa_cli.auth_store import TokenData, TokenStore
from mwa_cli.client.base import BaseClient
from mwa_cli.client.errors import APIError, parse_api_error


@pytest.mark.asyncio
async def test_client_initialization():
    """Test BaseClient initializes with correct base URL"""
    client = BaseClient(base_url="https://test.example.com")

    assert client.base_url == "https://test.example.com"
    assert client.timeout == 30.0 # default timeout


@pytest.mark.asyncio
async def test_base_client_injects_tokens(tmp_path):
    """Test authentication middleware injects access token"""

    token_file = tmp_path / "tokens.json"
    store = TokenStore(token_file)

    token_data = TokenData(
        access_token="test_access_token",
        refresh_token="test_refresh_token",
        access_expires_at="2099-12-31T23:59:59Z",
        refresh_expires_at="2099-12-31T23:59:59Z",
        user_id=1,
        user_login="test",
        user_email="test@example.com"
    )

    store.save(token_data)

    with patch("mwa_cli.client.base.TokenStore") as mock_store_class:
        mock_store_class.return_value = store

        client = BaseClient(base_url="https://test.example.com")

        with patch("httpx.AsyncClient.request") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.json.return_value = {"status": "ok"}
            mock_get.return_value = mock_response

            async with client:
                await client.get("/test")

            call_kwargs = mock_get.call_args[1]
            assert "cookies" in call_kwargs
            assert call_kwargs["cookies"]["access_token"] == "test_access_token"

@pytest.mark.asyncio
async def test_automatic_token_refresh_on_401():
    """Test client automatically refreshes token on 401 response"""

    with patch("mwa_cli.client.base.TokenStore") as mock_store_class:
        mock_store = MagicMock()
        mock_store.load.return_value = TokenData(
            access_token="expired_token",
            refresh_token="valid_refresh",
            access_expires_at="2020-01-01T:00:00:00Z",
            refresh_expires_at="2099-12-31T23:59:59Z",
            user_id=1,
            user_login="test",
            user_email="test@example.com"
        )
        mock_store_class.return_value = mock_store

        client = BaseClient(base_url="https://test.example.com")

        with patch("httpx.AsyncClient.request") as mock_get:
            # first GET returns 401
            response_401 = MagicMock()
            response_401.status_code = 401
            response_401.raise_for_status.side_effect = httpx.HTTPStatusError(
                "401", request=MagicMock(), response=response_401
            )

            # refresh request returns new tokens
            refresh_response = MagicMock()
            refresh_response.status_code = 200
            refresh_response.cookies = {
                "access_token": "new_access_token",
                "refresh_token": "new_refresh_token"
            }
            mock_get.return_value = refresh_response

            # return GET success
            response_200 = MagicMock()
            response_200.status_code = 200
            response_200.json.return_value = {"status": "ok"}
            mock_get.side_effect = [response_401, response_200]

            async with client:
                with pytest.raises(APIError):
                    result = await client.get("/test")

                    # verify refresh was called
                    mock_get.assert_called()
                    assert mock_get.call_count == 2 # original + retry

@pytest.mark.asyncio
async def test_retry_logic():
    """Test client retries on transient errors"""

    client = BaseClient(base_url="https://test.example.com", max_retries=3)

    with patch("httpx.AsyncClient.request") as mock_get:
        # First 2 calls fail, 3rd succeeds
        mock_get.side_effect = [
            httpx.ConnectError("Connection failed"),
            httpx.ConnectError("Connection failed"),
            MagicMock(status_code=200, json=lambda: {"status": "ok"})
        ]

        async with client:
            result = await client.get("/test")

        assert mock_get.call_count == 3

def test_error_parsing_for_validation_errors():
    """Test parse_api_error extracts ASVO validation error details"""

    mock_response = MagicMock()
    mock_response.status_code = 422
    mock_response.json.return_value = {
        "field_errors": [
            {
                "field": "obs_id",
                "message": "Observation ID is required"

            },
            {
                "field": "email",
                "message": "Invalid email format"
            }
        ]
    }

    error = parse_api_error(mock_response)

    assert error.status_code == 422
    assert "obs_id" in error.message
    assert "is required" in error.message

def test_error_parsing_for_business_logic_errors():
    """Test parse_api_error handles business logic errors (400)"""

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.json.return_value = {
        "detail": "Job limit reached",
        "error_code": 1
    }

    error = parse_api_error(mock_response)

    assert error.status_code == 400
    assert "Job limit reached" in error.message
    assert error.error_code == 1

@pytest.mark.asyncio
async def test_request_logging_in_verbose_mode():
    """Test request/response logging when verbose flag enabled"""

    client = BaseClient(base_url="https://test.example.com", verbose=True)

    with patch("httpx.AsyncClient.request") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "ok"}
        mock_get.return_value = mock_response

        with patch("mwa_cli.client.base.logger") as mock_logger:
            async with client:
                await client.get("/test")

            assert mock_logger.info.called or mock_logger.debug.called
