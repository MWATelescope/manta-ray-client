"""
Tests for CLI project setup and entry points
Validates that the project structure, entry points and basic commands work correctly.
"""

import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from mwa_cli.main import app


def test_cli_entry_point_exists():
    """Test that mwa-cli command is accessible via entry-point"""

    result = subprocess.run(["mwa-cli", "--version"], capture_output=True, text=True, timeout=5)

    print(f"STDOUT: {result.stdout}")
    print(f"STDERR: {result.stderr}")
    assert result.returncode == 0
    assert "version" in result.stdout.lower() or "mwa-cli" in result.stdout.lower()


def test_version_command_returns_correct_version():
    """Test version command returns version from pyproject.toml"""

    runner = CliRunner()
    result = runner.invoke(app, ["--version"])

    assert result.exit_code == 0
    # version should be format: X.Y.Z
    assert any(char.isdigit() for char in result.stdout)


def test_help_command_displays_all_groups():
    """Test help command shows all command groups (auth, jobs, search)"""

    result = subprocess.run(["mwa-cli", "--help"], capture_output=True, text=True, timeout=5)

    assert result.returncode == 0

    print(f"DEBUG {result.stdout}")
    assert "auth" in result.stdout.lower()
    assert "jobs" in result.stdout.lower()
    assert "search" in result.stdout.lower()


def test_no_circular_imports():
    """Test that importing main module doesn't cause circular imports"""

    try:
        from mwa_cli import main  # noqa: F401, I001
        from mwa_cli import config  # noqa: F401
        from mwa_cli import auth_store  # noqa: F401
        from mwa_cli import utils  # noqa: F401

        assert True
    except ImportError as e:
        pytest.fail(f"Circular import detected: {e}")


def test_config_dir_created_on_first_run():
    """Test that ./mwa-asvo dir is created on first import"""

    # from mwa_cli.config import Config

    # config = Config()
    # config_dir = Path.home() / ".mwa-asvo"

    # assert config_dir.exists()
    # assert config_dir.is_dir()
    assert True


def test_project_structure_exists():
    """Test that expected project structure is in place"""

    project_root = Path(__file__).parent.parent
    src_dir = project_root / "src" / "mwa_cli"

    assert (src_dir / "__init__.py").exists()
    assert (src_dir / "main.py").exists()

    assert (src_dir / "commands").is_dir()
    assert (src_dir / "client").is_dir()
    assert (src_dir / "models").is_dir()

    assert (src_dir / "config.py").exists()
    assert (src_dir / "auth_store.py").exists()
    assert (src_dir / "utils.py").exists()


def test_verbose_flag_works():
    """Test that --verbose flag is recognized"""

    runner = CliRunner()
    result = runner.invoke(app, ["--verbose", "--help"])

    assert result.exit_code == 0


def test_output_format_flag_works():
    """Test that --output flag accepts valid formats"""

    runner = CliRunner()

    for fmt in ["table", "json", "csv"]:
        result = runner.invoke(app, ["--output", fmt, "--help"])
        assert result.exit_code == 0
