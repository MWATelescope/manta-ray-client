# mwa-cli

Modern CLI tool for the MWA ASVO platform.

## Installation

```bash
pip install -e .
```

## Usage

```bash
# Authenticate
mwa-cli login --username your_username

# Search observations
mwa-cli search --obs-id 1234567890

# Submit a job
mwa-cli jobs submit conversion --obs-id 1234567890

# List jobs
mwa-cli jobs list

# Check status
mwa-cli status
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type check
mypy src/mwa_cli

# Lint and format
ruff check src/ tests/
ruff format src/ tests/
```