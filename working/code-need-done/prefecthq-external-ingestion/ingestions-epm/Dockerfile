FROM python:3.7.9-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Docker layer caching: copy requirements first
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code into container
COPY . .

# Unbuffered environment for immediate realtime logging to Prefect HQ
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Run prefect Worker listening  to the specifield work pool
CMD ["sh", "-c", "prefect worker start --pool \"${PREFECT_WORK_POOL_NAME:-default-agent-pool}\""]
