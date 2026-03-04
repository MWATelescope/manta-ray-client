FROM python:3.10

# Install system packages (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl libxml2-dev libxmlsec1-dev libxmlsec1-openssl \
    libssl-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/* \
    && apt-get autoremove \
    && apt-get clean

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# --- Dependency caching layer ---
# Copy only dependency files first (for caching)
COPY ./pyproject.toml ./uv.lock ./

# Pre-install dependencies — this layer is cached unless pyproject/lock changes
RUN uv sync --frozen --no-install-project

COPY . /manta-ray-client

RUN cd manta-ray-client \
    && uv sync \
    && uv run --locked setup.py install 

ENTRYPOINT /bin/bash
