"""
Secure token storage for JWT authentication
Store access and refresh tokens in ~/mwa-asvo/tokens.json with 0600 permissions.
"""

import json
import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path


logger = logging.getLogger(__name__)


@dataclass
class TokenData:
    """JWT token data with user information"""

    access_token: str
    refresh_token: str
    access_expires_at: str
    refresh_expires_at: str
    user_id: str
    user_login: str
    user_email: str

    def is_access_token_expired(self) -> bool:
        """Check if access token has expired"""

        try:
            expiry = datetime.fromisoformat(self.access_expires_at.replace("Z", ""))
            return datetime.now(expiry.tzinfo) >= expiry
        except (ValueError, AttributeError):
            logger.warning("Invalid access token expiry timestamp")
            return True

    def is_refresh_token_expired(self) -> bool:
        """Check if refresh token has expired"""

        try:
            expiry = datetime.fromisoformat(self.refresh_expires_at.replace("Z", ""))
            return datetime.now(expiry.tzinfo) >= expiry
        except (ValueError, AttributeError):
            logger.warning("Invalid refresh token expiry timestamp")
            return True


class TokenStore:
    """Manages secure storage of JWT tokens"""

    def __init__(self, token_file: Path | None = None):
        """Initialize token store"""

        if token_file is None:
            config_dir = Path.home() / ".mwa-asvo"
            config_dir.mkdir(parents=True, exist_ok=True)
            token_file = config_dir / "tokens.json"

        self.token_file = token_file

    def save(self, token_data: TokenData) -> None:
        """Save tokens to file with secure permissions"""

        data = asdict(token_data)

        with open(self.token_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

        os.chmod(self.token_file, 0o600)
        logger.info(f"Tokens saved to {self.token_file} with 0600 permissions")

    def load(self) -> TokenData | None:
        """Load tokens from file"""

        if not self.token_file.exists():
            logger.debug(f"Token file not found: {self.token_file}")
            return None

        try:
            with open(self.token_file, encoding="utf-8") as f:
                data = json.load(f)

            required = [
                "access_token",
                "refresh_token",
                "access_expires_at",
                "refresh_expires_at",
                "user_id",
                "user_login",
                "user_email",
            ]

            if not all(key in data for key in required):
                logger.error("Token file missing required fields")
                return None

            token_data = TokenData(**data)
            logger.debug(f"Tokens loaded for user: {token_data.user_login}")

            return token_data
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"Failed to load tokens: {e}")
            return None

    def clear(self) -> None:
        """Delete token file (logout)"""

        if self.token_file.exists():
            self.token_file.unlink()
            logger.info(f"Token file deleted: {self.token_file}")
        else:
            logger.debug("Token file already cleared")

    def get_access_token(self) -> str | None:
        """Get current refresh token if valid"""

        token_data = self.load()

        if token_data is None:
            return None

        if token_data.is_access_token_expired():
            logger.debug("Access token has expired")
            return None

        return token_data.access_token

    def get_refresh_token(self) -> str | None:
        """Get current refresh token if valid"""

        token_data = self.load()
        if token_data is None:
            return None

        if token_data.is_refresh_token_expired():
            logger.debug("Refresh token has expired")
            return None

        return token_data.refresh_token
