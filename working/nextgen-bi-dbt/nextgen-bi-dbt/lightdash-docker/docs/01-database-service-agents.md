# Tài liệu mô tả công dụng các Agent cho 1 Database Service

## 1. Mục tiêu
Tài liệu này mô tả vai trò của các Agent khi vận hành metadata cho một database service (ví dụ: `crm-gold`), giúp team:
- Hiểu mỗi Agent dùng để làm gì
- Biết khi nào cần chạy Agent nào
- Tránh chạy sai thứ tự dẫn đến metadata cập nhật không đầy đủ

## 2. Phạm vi
Áp dụng cho một service database trong OpenMetadata/Lightdash stack, ví dụ:
- Service: `crm-gold`
- Catalog: `hive`
- Schema: `bi_silver`, `bi_gold`

## 3. Tổng quan các Agent

### 3.1 Metadata Agent (Schema Ingestion)
- Công dụng: Đồng bộ danh sách table/view/column và kiểu dữ liệu từ database vào metadata catalog.
- Đầu vào: Kết nối database service + quyền đọc metadata.
- Đầu ra: Entity table/column được tạo hoặc cập nhật trong catalog.
- Khi chạy: Bắt buộc chạy đầu tiên, và chạy định kỳ (15-60 phút) để bắt thay đổi cấu trúc.
- Dấu hiệu lỗi thường gặp: Table mới có trong DB nhưng không thấy trên UI.
- Công cụ triển khai khuyến nghị:
	- OpenMetadata Ingestion Workflow (metadata CLI)
	- Airflow DAG hoặc CronJob trên Kubernetes
	- Docker Compose job runner cho môi trường local/dev
- Tự code nhanh:
	- Viết script Python gọi OpenMetadata REST API để upsert database/schema/table/column.
	- Lấy schema từ `information_schema` của Trino/Hive, map về payload OpenMetadata.
	- Chạy theo lịch bằng cron, lưu checkpoint `last_success_time` để giảm tải.

### 3.2 DBT Agent (DBT Ingestion)
- Công dụng: Đồng bộ mô tả model/column, owner, tags, test result từ DBT artifact (`manifest.json`, `catalog.json`, `run_results.json`).
- Đầu vào: Artifact DBT + serviceName khớp với database service.
- Đầu ra: Description, owner, tags, lineage logic cấp model được cập nhật.
- Khi chạy: Sau Metadata Agent.
- Dấu hiệu lỗi thường gặp: Log báo updated nhưng UI không đổi do FQN không khớp hoặc mapping model bị sai.
- Công cụ triển khai khuyến nghị:
	- `metadata ingest` với source type `dbt`
	- Tích hợp trong CI/CD sau bước `dbt build` và `dbt docs generate`
	- Orchestrator: Airflow, Dagster, GitHub Actions, GitLab CI
- Tự code nhanh:
	- Đọc `manifest.json`, `catalog.json`, `run_results.json`.
	- Chuẩn hóa FQN theo `service.database.schema.table` trước khi ingest.
	- Gọi API patch cho table/column description, tags, owner.
	- Nếu model là ephemeral nhưng vẫn cần update metadata, ép materialization trong artifact dùng cho ingest.

### 3.3 Usage Agent
- Công dụng: Thu thập thống kê truy cập table/column (query count, user usage).
- Đầu vào: Query log/audit log của engine.
- Đầu ra: Số liệu mức độ sử dụng để ưu tiên governance.
- Khi chạy: Theo ngày hoặc theo giờ tùy tài nguyên.
- Dấu hiệu lỗi thường gặp: Tất cả bảng đều usage = 0 do chưa cấp quyền đọc log.
- Công cụ triển khai khuyến nghị:
	- OpenMetadata Usage ingestion connector
	- Query log pipeline từ Trino/Hive (S3, Kafka, Elasticsearch)
	- Batch ETL bằng Spark/Flink nếu log lớn
- Tự code nhanh:
	- Parse query logs, trích xuất table/column được truy vấn theo user.
	- Tổng hợp theo cửa sổ thời gian (1h/24h), rồi push usage về OpenMetadata API.
	- Áp dụng chuẩn hóa tên bảng để tránh lệch FQN.

### 3.4 Profiler Agent
- Công dụng: Tính thống kê dữ liệu (null rate, distinct count, min/max, distribution) theo bảng/cột.
- Đầu vào: Quyền đọc data.
- Đầu ra: Profile metric phục vụ cảnh báo chất lượng dữ liệu.
- Khi chạy: Theo ngày/tuần, nên chạy ngoài giờ cao điểm.
- Dấu hiệu lỗi thường gặp: Chạy lâu, tốn tài nguyên do profile quá nhiều bảng lớn.
- Công cụ triển khai khuyến nghị:
	- OpenMetadata Profiler workflow
	- Spark job cho bảng lớn
	- Kubernetes CronJob để tách lịch profile theo domain
- Tự code nhanh:
	- Tạo danh sách bảng ưu tiên profile (top usage, bảng critical).
	- Chạy SQL thống kê (`count`, `count distinct`, `min/max`, `null rate`) theo cột.
	- Ghi kết quả về bảng metric nội bộ hoặc đẩy trực tiếp vào OpenMetadata API.

### 3.5 Data Quality Agent (Test/Suite)
- Công dụng: Chạy bộ rule chất lượng dữ liệu (null, unique, range, freshness...).
- Đầu vào: Rule test + kết nối service.
- Đầu ra: Kết quả pass/fail, cảnh báo theo bảng/cột.
- Khi chạy: Theo lịch nghiệp vụ (hằng ngày trước giờ báo cáo).
- Dấu hiệu lỗi thường gặp: Rule đúng cấu trúc nhưng sai ngữ cảnh nghiệp vụ.
- Công cụ triển khai khuyến nghị:
	- DBT tests (generic + custom)
	- Great Expectations hoặc Soda Core
	- OpenMetadata Test Suite/Test Case API
- Tự code nhanh:
	- Viết bộ rule dạng YAML hoặc SQL assertion.
	- Chạy batch test, chuẩn hóa output về format pass/fail.
	- Đẩy kết quả lên OpenMetadata và gửi cảnh báo qua Slack/Email.

### 3.6 Lineage Agent
- Công dụng: Xây dựng dòng chảy dữ liệu giữa source -> model -> dashboard.
- Đầu vào: SQL parser, DBT metadata, hoặc query history.
- Đầu ra: Graph lineage phục vụ impact analysis.
- Khi chạy: Sau Metadata + DBT Agent.
- Dấu hiệu lỗi thường gặp: Không trace được lineage khi SQL quá động hoặc dùng dynamic SQL.
- Công cụ triển khai khuyến nghị:
	- DBT lineage từ manifest
	- OpenLineage + Marquez
	- SQL lineage parser (sqlglot, openmetadata-sqllineage)
- Tự code nhanh:
	- Parse AST từ SQL để lấy upstream/downstream table.
	- Merge với lineage từ DBT để tăng độ phủ.
	- Gọi API tạo cạnh lineage theo cặp source -> target.

## 3.7 Khung triển khai chuẩn cho từng Agent
- Runtime:
	- Local/dev: Docker Compose + shell script
	- Staging/prod: Airflow hoặc Kubernetes CronJob
- Logging:
	- Ghi log chuẩn JSON, tối thiểu gồm `job_name`, `service`, `status`, `duration`, `error`
- Retry:
	- Retry 3 lần, exponential backoff (30s, 60s, 120s)
- Idempotent:
	- Job phải chạy lại an toàn, không tạo dữ liệu trùng
- Secrets:
	- Dùng secret manager hoặc biến môi trường CI/CD, không hard-code token trong repo

## 4. Thứ tự chạy khuyến nghị cho 1 service database
1. Metadata Agent
2. DBT Agent
3. Lineage Agent
4. Usage Agent
5. Profiler Agent
6. Data Quality Agent

Ghi chú:
- Nếu metadata schema chưa có, DBT Agent có thể không update được description đúng đối tượng.
- Nếu serviceName/FQN sai, log vẫn có thể báo updated artifact nhưng entity trên UI không đổi.

## 5. Lịch chạy đề xuất
- Metadata Agent: mỗi 30 phút
- DBT Agent: sau mỗi lần deploy DBT, tối thiểu 2-4 lần/ngày
- Lineage Agent: 2-4 lần/ngày
- Usage Agent: mỗi 24h
- Profiler Agent: mỗi đêm (00:00-05:00)
- Data Quality Agent: theo SLA report (ví dụ 06:00 hằng ngày)

## 6. Checklist vận hành nhanh khi "description không cập nhật"
1. Kiểm tra `serviceName` trong DBT ingestion có đúng service đang xem trên UI.
2. Kiểm tra FQN table (service.database.schema.table) có khớp 100%.
3. Kiểm tra artifact DBT có description tại model/column.
4. Kiểm tra metadata entity đã tồn tại trước khi DBT Agent chạy.
5. Chạy lại Metadata Agent -> DBT Agent theo thứ tự.
6. Hard refresh UI và kiểm tra `updatedAt`, `updatedBy`.

## 7. RACI đề xuất
- Data Platform: sở hữu Metadata Agent, Usage Agent, Profiler Agent
- Analytics Engineering: sở hữu DBT Agent, Lineage Agent
- Data Governance/BI Owner: sở hữu Data Quality Agent và quy tắc đặt mô tả

## 8. Mẫu cấu hình tối thiểu cho 1 service
- Service Name: `crm-gold`
- Database: `hive`
- Schema: `bi_silver`
- Environment: `local`/`prod`
- Owners: team-analytics, team-data-platform

## 9. Khuyến nghị triển khai riêng cho Trino/Hive

### 9.1 Kiến trúc runtime đề xuất
- Mục tiêu: Tách ingestion metadata với truy vấn nghiệp vụ để không ảnh hưởng hiệu năng BI.
- Cách làm:
	- Dùng 1 worker riêng cho các Agent (Airflow worker hoặc Kubernetes namespace riêng).
	- Dùng account kỹ thuật riêng cho ingestion, chỉ cấp quyền cần thiết.
	- Dùng SSL/TLS và timeout rõ ràng cho Trino connector.
	- Giới hạn concurrency cho profiler và data quality để tránh tải lớn.

### 9.2 Quyền tối thiểu trên Trino/Hive
- Metadata Agent:
	- Quyền đọc metadata trên `information_schema`.
	- Quyền `SHOW TABLES`, `SHOW COLUMNS` trong các schema mục tiêu.
- DBT Agent:
	- Không cần quyền ghi DB nếu chỉ ingest artifact.
	- Cần quyền đọc artifact lưu trên local/S3/MinIO.
- Usage Agent:
	- Quyền đọc query log/event log (tùy cách lưu log của Trino).
- Profiler/Data Quality:
	- Quyền `SELECT` trên bảng mục tiêu.
	- Nên cấm ghi hoặc DDL trong account profiler.

### 9.3 Lịch chạy khuyến nghị cho Trino/Hive
- Metadata Agent: mỗi 30 phút.
- DBT Agent: chạy ngay sau pipeline DBT thành công.
- Usage Agent: mỗi 24h hoặc mỗi 6h nếu cần near real-time.
- Profiler Agent: chạy theo lô ngoài giờ cao điểm, chia nhỏ theo schema.
- Data Quality Agent: theo SLA báo cáo, ưu tiên bảng critical trước.

### 9.4 Mẫu triển khai bằng Airflow (ý tưởng DAG)
- DAG 1: metadata_ingestion_trino_hive
	- Task 1: check_service_health
	- Task 2: run_metadata_agent
- DAG 2: dbt_and_metadata_sync
	- Task 1: dbt_build
	- Task 2: dbt_docs_generate
	- Task 3: normalize_artifact_fqn_materialization
	- Task 4: run_dbt_agent
- DAG 3: governance_daily
	- Task 1: run_usage_agent
	- Task 2: run_profiler_agent
	- Task 3: run_data_quality_agent

## 10. Mẫu code tự triển khai cho Trino/Hive

### 10.1 Đọc metadata schema từ Trino (Python)

		import trino

		conn = trino.dbapi.connect(
				host="10.0.247.157",
				port=8443,
				user="ingestion_bot",
				http_scheme="https",
				catalog="hive",
				schema="information_schema",
				auth=trino.auth.BasicAuthentication("ingestion_bot", "***"),
		)

		cur = conn.cursor()
		cur.execute(
				"""
				SELECT table_schema, table_name, column_name, data_type
				FROM hive.information_schema.columns
				WHERE table_schema IN ('bi_silver', 'bi_gold')
				"""
		)
		rows = cur.fetchall()

### 10.2 Chuẩn hóa FQN trước khi gọi OpenMetadata API

		def build_fqn(service: str, database: str, schema: str, table: str) -> str:
				return f"{service}.{database}.{schema}.{table}".lower()

		fqn = build_fqn("crm-gold", "hive", "bi_silver", "crm_deals")

### 10.3 Patch description cột qua OpenMetadata API

		import requests

		host = "http://localhost:8036/api"
		jwt = "${OM_JWT}"
		fqn = "crm-gold.hive.bi_silver.crm_deals"

		table = requests.get(
				f"{host}/v1/tables/name/{fqn}?fields=columns",
				headers={"Authorization": f"Bearer {jwt}"},
				timeout=30,
		).json()

		for col in table.get("columns", []):
				if col["name"] == "deal_id" and not col.get("description"):
						col["description"] = "Mã định danh duy nhất của giao dịch."

		requests.put(
				f"{host}/v1/tables",
				headers={"Authorization": f"Bearer {jwt}", "Content-Type": "application/json"},
				json=table,
				timeout=30,
		)

### 10.4 SQL profiler tối giản cho Trino/Hive

		SELECT
			COUNT(*) AS row_count,
			SUM(CASE WHEN deal_id IS NULL THEN 1 ELSE 0 END) AS deal_id_null_count,
			CAST(SUM(CASE WHEN deal_id IS NULL THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(*), 0) AS deal_id_null_rate,
			APPROX_DISTINCT(deal_id) AS deal_id_distinct
		FROM hive.bi_silver.crm_deals;

### 10.5 Data quality rule kiểu SQL assertion

		-- Rule: deal_id không được null
		SELECT COUNT(*) AS violations
		FROM hive.bi_silver.crm_deals
		WHERE deal_id IS NULL;

### 10.6 Shell job mẫu cho DBT Agent (artifact-only)

		#!/usr/bin/env bash
		set -euo pipefail

		export OM_API_HOSTPORT="http://localhost:8036/api"
		export OM_AUTH_PROVIDER="openmetadata"
		export OM_JWT="${OM_JWT}"

		# normalize artifact trước khi ingest (FQN + materialized)
		python scripts/normalize_dbt_artifacts.py \
			--manifest dbt_projects/crm/target/manifest.json \
			--catalog dbt_projects/crm/target/catalog.json \
			--service crm-gold \
			--database hive \
			--schema bi_silver

		metadata ingest -c dbt_projects/crm/ingestion/dbt_ingest.yaml

## 11. Bảo mật và vận hành
- Không commit JWT, password, access key vào repository.
- Dùng secret manager hoặc biến môi trường ở CI/CD.
- Đặt TTL ngắn cho token bot, xoay vòng định kỳ.
- Mask toàn bộ secret trong log và alert.
- Với file `.env.openmetadata.local`, chỉ dùng cho local; bản production nên tách secret ra khỏi file mã nguồn.

## 12. Chạy nhanh Lineage Agent cho PySpark ETL
- Script sẵn dùng:
	- `lightdash-docker/scripts/lineage_agent_trino_hive.py`
- File cấu hình mẫu:
	- `lightdash-docker/scripts/lineage_agent_config.example.yaml`

Quy trình chạy:
1. Export token:

			 export OM_JWT="<your_openmetadata_jwt>"

2. Sửa file config theo đúng bảng raw/silver/gold của bạn.
	- Nếu toàn bộ flow cùng một service, có thể khai báo `service` ở root config.
	- Nếu flow đi qua nhiều service (ví dụ `crm-raw -> crm-silver -> crm-gold`), khai báo `from.service` và `to.service` cho từng edge.
3. Chạy script:

			 python3 lightdash-docker/scripts/lineage_agent_trino_hive.py \
				 --config lightdash-docker/scripts/lineage_agent_config.example.yaml

Kết quả mong đợi:
- Mỗi edge lineage sẽ được upsert qua OpenMetadata API `PUT /v1/lineage`.
- Chạy lại an toàn, không tạo cạnh trùng.

---

Tài liệu này có thể dùng làm template cho các service khác (finance-gold, cx-gold, hr-gold) bằng cách đổi lại service/database/schema và SLA.