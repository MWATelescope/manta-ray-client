FROM python:3.10-slim

# Install system packages (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /app

# Copy only dependency files first (for caching)
COPY ./pyproject.toml ./uv.lock ./

# Install dependencies (This creates /app/.venv)
RUN uv sync --frozen --no-install-project

COPY . .

# Final sync to install the 'mantaray' package and create the 'mwa_client' script
# Using --no-editable ensures the script is baked in properly
RUN uv sync --frozen --no-editable

# Add the virtual environment to the PATH
ENV PATH="/app/.venv/bin:$PATH"

# Default to bash, but now mwa_client is in the PATH
ENTRYPOINT ["/bin/bash", "-c"]
CMD ["mwa_client"]
