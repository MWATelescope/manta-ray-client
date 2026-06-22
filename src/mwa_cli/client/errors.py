"""
API error parsing and user-friendly error messages
Extracts FastAPI ValidationError details and business logic errors
"""

import logging
from dataclasses import dataclass
from typing import Any

import httpx


logger = logging.getLogger(__name__)

@dataclass
class APIError(Exception):
    """Structured API error with user-friendly message"""

    def __init__(
        self,
        status_code: int,
        message: str,
        error_code: int | None = None,
        field_errors: list[dict[str, Any]] | None = None
    ):
        super().__init__(message)

        self.message = message
        self.error_code = error_code
        self.status_code = status_code
        self.field_errors = field_errors

    def to_dict(self) -> dict[str, Any]:
        """Convert exception to standard error response schema"""

        response: dict[str, Any] = {
            "error_code": self.error_code,
            "message": self.message,
        }

        if self.status_code:
            response["status_code"] = self.status_code

        if self.field_errors and len(self.field_errors) > 0:
            response["field_errors"] = self.field_errors

        return response

    def __str__(self) -> str:
        return self.message

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"error_code={self.error_code}, "
            f"message='{self.message}', "
            f"status_code={self.status_code})"
        )

def parse_api_error(response: httpx.Response) -> APIError:
    """
    Parse ASVO API error response into user-friendly APIError

    Handles:
    - 422 Validation errors (pydantic ValidationError)
    - 400 Business logic errors with error_code
    - 401 Authentication errors
    - 403 Permission denied
    - 404 Not found
    - 500 Server errors

    Returns: APIError with formatted message
    """

    status_code = response.status_code

    # try to parse JSON response body
    try:
        body = response.json()
    except Exception:
        body = {}

    # handle different error types
    if status_code == 422:
        return _parse_validation_error(body)

    # business logic error
    elif status_code == 400:
        return _parse_business_error(body)

    elif status_code == 401:
        return APIError(
            status_code=401,
            message="Authentication required. Please run: mwa-cli auth login"
        )

    elif status_code == 403:
        return APIError(
            status_code=403,
            message="Permission denied. You don't have access to this resource."
        )

    elif status_code == 404:
        detail = body.get("detail", "Resource not found")
        return APIError(
            status_code=404,
            message=f"Not found: {detail}"
        )

    elif status_code == 500:
        return APIError(
            status_code=status_code,
            message=f"Server error ({status_code}). Please try again later or contact support."
        )

    else:
        # Generic error
        detail = body.get("detail", f"HTTP {status_code} error")
        return APIError(
            status_code=status_code,
            message=str(detail),
        )

def _parse_validation_error(body: dict[str, Any]) -> APIError:
    """Parse 422 Validation Error"""

    errors = body.get("field_errors", [])

    if not isinstance(errors, list):
        return APIError(
            status_code=422,
            message=f"Validation error: {errors}"
        )

    field_errors = []
    error_messages = []

    for error in errors:
        field = error.get('field', "")
        msg = error.get('message', 'validation error')

        field_name = field if field else "unknown"

        field_errors.append({
            "field": field_name,
            "message": msg,
            "type": error.get("type", "")
        })

        error_messages.append(f"  - {field_name}: {msg}")

    message = "Validation errors:\n" + "\n".join(error_messages)

    return APIError(
        status_code=422,
        message=message,
        field_errors=field_errors
    )

def _parse_business_error(body: dict[str, Any]) -> APIError:
    """Parse business logic error (400 with error_code)"""

    detail = body.get("detail", "Bad request")
    error_code = body.get("error_code")

    suggestions = {
        0: "\n -> An unknown error occurred.",
        1: "\n -> Wait for existing jobs to complete or cancel some jobs",
        2: "\n -> Check observation ID is valid: mwa-cli search --obs-id <id>",
        3: "\n -> Verify job parameters match observation requirements"
    }

    code = 0
    if isinstance(error_code, int):
        code = int(error_code)

    suggestion = suggestions.get(code, 0)
    message = f"{detail}{suggestion}"

    return APIError(
        status_code=400,
        message=message,
        error_code=error_code
    )
