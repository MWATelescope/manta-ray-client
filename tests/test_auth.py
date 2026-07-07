"""
Tests for JWT authentication and token management.
Validates login, token storage, security and logout flows
"""

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from mwa_cli.auth_store import TokenData, TokenStore
from mwa_cli.commands.auth import app


@pytest.fixture
def tmp_token_file(tmp_path):
    """Create temporary token file for testing"""

    token_file = tmp_path / "tokens.json"
    return token_file

def test_login_with_api_key():
    """Test login command with api_key"""

    runner = CliRunner()

    with patch("mwa_cli.commands.auth.do_login") as mock_login:
        mock_login.return_value = {
            "access_token": "test_access_token",
            "refresh_token": "test_refresh_token",
            "access_expires_at": "2026-06-18T14:00:00Z",
            "refresh_expires_at": "2026-06-25T14:00:00Z",
            "user": {"id": 1, "login": "testuser", "email": "test@example.com"}
        }

        # simulate api_key input
        result = runner.invoke(
            app,
            ["login", "--api-url", "https://example.com"],
            input="123-test-api-key-456"
        )

        assert result.exit_code == 0
        assert "logged in as testuser" in result.stdout.lower() or "success" in result.stdout.lower()

def test_token_storage_creates_file(tmp_token_file):
    """Test that TokenStore creates tokens.json"""

    store = TokenStore(tmp_token_file)
    token_data = TokenData(
        access_token="test_access",
        refresh_token="test_refresh",
        access_expires_at="2026-06-18T14:00:00Z",
        refresh_expires_at="2026-06-25T14:00:00Z",
        user_id=1,
        user_login="testuser",
        user_email="test@example.com"
    )

    store.save(token_data)

    assert tmp_token_file.exists()
    assert tmp_token_file.is_file()

def test_file_permissions_set_to_0600(tmp_token_file):
    """Test that token file has 0600 permissions (user read/write only)"""

    store = TokenStore(tmp_token_file)
    token_data = TokenData(
        access_token="test_access",
        refresh_token="test_refresh",
        access_expires_at="2026-06-18T14:00:00Z",
        refresh_expires_at="2026-06-25T14:00:00Z",
        user_id=1,
        user_login="testuser",
        user_email="test@example.com"
    )

    store.save(token_data)

    # check file permissions (0600 = 384 decimal)
    stat = tmp_token_file.stat()
    mode = stat.st_mode & 0o777 # get permission bits

    assert mode == 0o600, f"Expected 0o600, got {oct(mode)}"


def test_token_loading_from_storage(tmp_token_file):
    """Test loading tokens from TokenStore"""
    store = TokenStore(tmp_token_file)

    original = TokenData(
        access_token="access_123",
        refresh_token="refresh_456",
        access_expires_at="2026-06-18T14:00:00Z",
        refresh_expires_at="2026-06-25T14:00:00Z",
        user_id=42,
        user_login="johndoe",
        user_email="john@example.com"
    )
    store.save(original)

    loaded = store.load()

    assert loaded is not None
    assert loaded.access_token == "access_123"
    assert loaded.refresh_token == "refresh_456"
    assert loaded.user_id == 42
    assert loaded.user_login == "johndoe"

def test_logout_clears_tokens(tmp_token_file):
    """Test that logout command clears token storage"""

    store = TokenStore(tmp_token_file)
    token_data = TokenData(
        access_token="test",
        refresh_token="test",
        access_expires_at="2026-05-21T14:00:00Z",
        refresh_expires_at="2026-05-28T14:00:00Z",
        user_id=1,
        user_login="test",
        user_email="test@example.com"
    )

    store.save(token_data)
    store.clear()

    assert not tmp_token_file.exists() or tmp_token_file.stat().st_size == 0

def test_status_command_shows_user_info():
    """Test status command shows user and token info"""

    runner = CliRunner()

    with patch("mwa_cli.commands.auth.TokenStore") as mock_store_class:
        mock_store = MagicMock()
        mock_store.load.return_value = TokenData(
            access_token="test",
            refresh_token="test",
            access_expires_at="2026-05-21T14:00:00Z",
            refresh_expires_at="2026-05-28T14:00:00Z",
            user_id=1,
            user_login="johnwick",
            user_email="john.wick@curtin.edu.au"
        )
        mock_store_class.return_value = mock_store

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "johnwick" in result.stdout or "john.wick" in result.stdout

def test_status_handles_not_logged_in():
    """Test status command when user is not logged in"""

    runner = CliRunner()

    with patch("mwa_cli.commands.auth.TokenStore") as mock_store_class:
        mock_store = MagicMock()
        mock_store.load.return_value = None
        mock_store_class.return_value = mock_store

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "not logged in" in result.stdout.lower() or "not authenticated" in result.stdout.lower()
