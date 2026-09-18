# ==============================================================================
# EPM Lightweight Crawler (Zero-Spark, Zero-JVM Pure Python Image)
# Base Image: prefecthq/prefect:3-python3.12 (~150MB overhead vs ~2.5GB Spark)
# ==============================================================================
FROM prefecthq/prefect:3-python3.12

# Prevent Python from writing bytecode and enable unbuffered standard I/O
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app:/app/epm

WORKDIR /app

# Install minimal OS dependencies for network & TLS
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install lightweight Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application files
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/entrypoint.sh && \
    chmod +x /usr/local/bin/entrypoint.sh

COPY epm/ /app/epm/
COPY epm_crawler_flow.py /app/epm_crawler_flow.py
COPY .env.epm /app/.env.epm

# Create persistent state, buffer, and audit mount points
RUN mkdir -p /app/state /app/data/buffer /app/data/audit

VOLUME ["/app/state", "/app/data"]

EXPOSE 4200

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["serve"]
