FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        ca-certificates \
        cron \
        git \
    && rm -rf /var/lib/apt/lists/*

# Runtime for dbt build + S3 upload + OpenMetadata dbt ingestion helpers.
COPY dbt_projects/requirements.txt /tmp/dbt-requirements.txt
RUN pip install --no-cache-dir -r /tmp/dbt-requirements.txt

WORKDIR /workspace/metadata-agent
COPY metadata-agent/scripts/entrypoint.sh /workspace/metadata-agent/scripts/entrypoint.sh
COPY metadata-agent/scripts/run-once.sh /workspace/metadata-agent/scripts/run-once.sh
COPY metadata-agent/python /workspace/metadata-agent/python
RUN chmod +x /workspace/metadata-agent/scripts/entrypoint.sh /workspace/metadata-agent/scripts/run-once.sh

ENTRYPOINT ["/workspace/metadata-agent/scripts/entrypoint.sh"]
