FROM python:3.10-slim

# Install essential system dependencies (curl for container healthchecks)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Leverage Docker layer caching: copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code into container
COPY . .

# Unbuffered environment for immediate realtime logging to Prefect HQ
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Default command: run Prefect Worker listening to the specified work pool
CMD ["sh", "-c", "prefect worker start --pool \"${PREFECT_WORK_POOL_NAME:-default-agent-pool}\""]
