FROM node:20-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       python3 \
       python3-pip \
       python3-venv \
       ca-certificates \
       bash \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir dbt-core dbt-trino
RUN npm install -g @lightdash/cli@latest

WORKDIR /workspace
COPY . /workspace

RUN chmod +x /workspace/scripts/setup-lightdash-projects.sh
ENTRYPOINT ["/workspace/scripts/setup-lightdash-projects.sh"]
