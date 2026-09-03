# =============================================================================
# Fleet Data Platform — Makefile
# =============================================================================
# Tự động hóa toàn bộ thao tác phát triển, kiểm thử, và vận hành từ Terminal.
#
# Usage (gõ trên Terminator / Terminal):
#   make help          — Hiển thị toàn bộ lệnh có sẵn
#   make up            — Bật hạ tầng local sandbox (Docker Compose)
#   make down          — Tắt hạ tầng local sandbox
#   make test          — Chạy toàn bộ unit tests
#   make run-scd2      — Chạy job SCD2 Customer trên môi trường local
#   make config env=prod  — In ra cấu hình production để kiểm tra
# =============================================================================

.PHONY: help up down clean status test lint config run-scd2 run-agg smoke-test topics

# Biến mặc định
ENV ?= local
COMPOSE_FILE = docker/docker-compose.local.yaml
PYTHON = python

# ─────────────────────────────────────────────────────────────────────────────
# HELP — Hiển thị menu lệnh
# ─────────────────────────────────────────────────────────────────────────────
help: ## Hiển thị toàn bộ lệnh có sẵn
	@echo ""
	@echo "╔═══════════════════════════════════════════════════════════════╗"
	@echo "║  Fleet Data Platform — Terminal Command Center               ║"
	@echo "╚═══════════════════════════════════════════════════════════════╝"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""

# ─────────────────────────────────────────────────────────────────────────────
# INFRASTRUCTURE — Quản lý Local Sandbox
# ─────────────────────────────────────────────────────────────────────────────
up: ## Bật toàn bộ hạ tầng local sandbox (Postgres, Redis, Redpanda, MinIO)
	@echo "🚀 Starting Fleet Local Sandbox..."
	docker compose -f $(COMPOSE_FILE) up -d
	@echo ""
	@echo "✅ Local Sandbox is UP. Services:"
	@echo "  • PostgreSQL  : localhost:5432  (user: fleet_app / pass: local_dev_pass)"
	@echo "  • Redis       : localhost:6379"
	@echo "  • Redpanda    : localhost:19092 (Kafka protocol)"
	@echo "  • Redpanda UI : http://localhost:8180"
	@echo "  • MinIO API   : http://localhost:9000 (minioadmin/minioadmin)"
	@echo "  • MinIO UI    : http://localhost:9001"

down: ## Tắt hạ tầng local sandbox (giữ dữ liệu)
	@echo "⏹ Stopping Fleet Local Sandbox..."
	docker compose -f $(COMPOSE_FILE) down

clean: ## Tắt và XÓA TOÀN BỘ dữ liệu local sandbox (volumes)
	@echo "🗑 Destroying Fleet Local Sandbox (including data)..."
	docker compose -f $(COMPOSE_FILE) down -v
	rm -rf data/warehouse data/checkpoints

status: ## Kiểm tra trạng thái tất cả containers
	@echo "📊 Fleet Local Sandbox — Container Status:"
	docker compose -f $(COMPOSE_FILE) ps

# ─────────────────────────────────────────────────────────────────────────────
# SMOKE TEST — Kiểm tra kết nối tới hạ tầng local
# ─────────────────────────────────────────────────────────────────────────────
smoke-test: ## Kiểm tra kết nối tới PostgreSQL, Redis, Redpanda, MinIO
	@echo "🔍 Running smoke tests against local infrastructure..."
	@echo ""
	@echo "━━━ [1/4] PostgreSQL ━━━"
	@docker exec fleet-postgres-local pg_isready -U fleet_app -d fleet_oltp && \
		echo "  ✓ PostgreSQL is healthy" || echo "  ✗ PostgreSQL FAILED"
	@echo ""
	@echo "━━━ [2/4] Redis ━━━"
	@docker exec fleet-redis-local redis-cli ping | grep -q PONG && \
		echo "  ✓ Redis PONG" || echo "  ✗ Redis FAILED"
	@echo ""
	@echo "━━━ [3/4] Redpanda (Kafka) ━━━"
	@docker exec fleet-redpanda-local rpk cluster health 2>/dev/null && \
		echo "  ✓ Redpanda cluster healthy" || echo "  ✗ Redpanda FAILED"
	@echo ""
	@echo "━━━ [4/4] MinIO (S3) ━━━"
	@docker exec fleet-minio-local mc ls local 2>/dev/null && \
		echo "  ✓ MinIO accessible" || echo "  ⚠ MinIO init may still be running"
	@echo ""
	@echo "✅ Smoke test completed."

# ─────────────────────────────────────────────────────────────────────────────
# KAFKA TOPICS — Tạo các Kafka Topics trên Redpanda local
# ─────────────────────────────────────────────────────────────────────────────
topics: ## Tạo Kafka topics (truck-telemetry, repair-request) trên Redpanda local
	@echo "📨 Creating Kafka topics on local Redpanda..."
	docker exec fleet-redpanda-local rpk topic create truck-telemetry --partitions 3 --replicas 1 2>/dev/null || true
	docker exec fleet-redpanda-local rpk topic create repair-request --partitions 3 --replicas 1 2>/dev/null || true
	docker exec fleet-redpanda-local rpk topic list
	@echo "✅ Kafka topics ready."

# ─────────────────────────────────────────────────────────────────────────────
# TESTING — Kiểm thử
# ─────────────────────────────────────────────────────────────────────────────
test: ## Chạy toàn bộ unit tests (pytest)
	@echo "🧪 Running tests..."
	APP_ENV=local $(PYTHON) -m pytest tests/ -v --tb=short

lint: ## Kiểm tra code style (flake8 + sqlfluff)
	@echo "🔎 Running linters..."
	$(PYTHON) -m flake8 src/ --max-line-length=120 --ignore=E501,W503
	@echo "✅ Lint passed."

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — Kiểm tra cấu hình
# ─────────────────────────────────────────────────────────────────────────────
config: ## In ra cấu hình đang áp dụng (Usage: make config env=prod)
	@echo "⚙ Dumping configuration for environment: $(ENV)"
	APP_ENV=$(ENV) $(PYTHON) -m src.core.config

# ─────────────────────────────────────────────────────────────────────────────
# ETL JOBS — Chạy PySpark Jobs trên môi trường local
# ─────────────────────────────────────────────────────────────────────────────
run-scd2: ## Chạy SCD2 Customer Dimension job (Usage: make run-scd2 env=local)
	@echo "⚡ Running SCD2 Customer Dimension job [env=$(ENV)]..."
	APP_ENV=$(ENV) $(PYTHON) -m src.jobs.scd2_customer_job

run-agg: ## Chạy Batch DWH Aggregation job (Usage: make run-agg env=local)
	@echo "⚡ Running Batch DWH Aggregation job [env=$(ENV)]..."
	APP_ENV=$(ENV) $(PYTHON) -m src.jobs.batch_dwh_job --granularity monthly
