"""Jobs client for submission and monitoring"""

import logging
from typing import Any

from mwa_cli.client.base import BaseClient
from mwa_cli.models.generated import JobsByUserRequest


logger = logging.getLogger(__name__)


class JobsClient:
    """Client for job operations"""

    def __init__(self, base_client: BaseClient):
        self.client = base_client

    async def list_jobs(self, params: JobsByUserRequest) -> dict[str, Any]:
        """List user jobs with filters"""

        request_params = params.model_dump(mode="json")
        response = await self.client.post("/api/v2/get_jobs", json=request_params)
        result: dict[str, Any] = response.json()

        return result

    async def get_job(self, job_id: int) -> dict[str, Any]:
        """Get job info based on the provided id"""

        response = await self.client.post(f"/api/v2/jobs/{job_id}")
        result: dict[str, Any] = response.json()

        return result
