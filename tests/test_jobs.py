from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from mwa_cli.client.errors import APIError
from mwa_cli.commands.jobs import app


@pytest.fixture
def mock_jobs_list():
    return {
        "jobs": [
            {"id": 12345, "job_type": 0, "job_state": "queued", "created": "2026-06-30T12:00:00Z"},
            {"id": 12346, "job_type": 1, "job_state": "processing", "created": "2026-06-30T12:05:00Z"}
        ],
        "total_count": 2
    }

@pytest.fixture
def mock_job_details():
    return {
        "id": 12345,
        "job_type": 0,
        "job_state": "completed",
        "job_params": {"obs_id": 1234567890, "delivery": "acacia"},
        "created": "2026-06-30T12:00:00Z",
        "completed": "2026-06-30T12:05:00Z"
    }

def test_jobs_list_no_filters(mock_jobs_list):
    """Test jobs list with no filters"""

    runner = CliRunner()

    with patch("mwa_cli.client.jobs.JobsClient.list_jobs") as mock_list:
        mock_list.return_value = mock_jobs_list
        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        assert "12345" in result.stdout

def test_jobs_list_filtered_by_state(mock_jobs_list):
    """Test jobs list with state filter"""

    runner = CliRunner()

    with patch("mwa_cli.client.jobs.JobsClient.list_jobs") as mock_list:
        mock_list.return_value = mock_jobs_list
        result = runner.invoke(app, ["list", "--state", "queued"])

        assert result.exit_code == 0

def test_jobs_show_display_details(mock_job_details):
    """Test jobs show display full job details"""

    runner = CliRunner()

    with patch("mwa_cli.client.jobs.JobsClient.get_job") as mock_post:
        mock_post.return_value = mock_job_details
        result = runner.invoke(app, ["show", "12345"])

        assert result.exit_code == 0
        assert "12345" in result.stdout
        assert "conversion" in result.stdout

def test_404_error_handling():
    """Test handling of job not found (404)"""

    runner = CliRunner()

    with patch("mwa_cli.client.jobs.JobsClient.get_job") as mock_post:
        mock_post.side_effect = APIError(status_code=404, message="Job not found")
        result = runner.invoke(app, ["show", "99999"])

        assert "not found" in result.stdout.lower()
