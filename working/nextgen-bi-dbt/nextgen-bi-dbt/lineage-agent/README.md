# Lineage Agent

Folder này chứa công cụ cập nhật lineage vào OpenMetadata cho luồng ETL (đặc biệt phù hợp Trino/Hive + PySpark).

## Files
- `lineage_agent_trino_hive.py`: script upsert edge lineage qua API `PUT /v1/lineage`
- `lineage_agent_config.example.yaml`: config mẫu raw -> silver -> gold, hỗ trợ cross-service
- `.env.lineage.example`: env mẫu cho `OM_API_HOSTPORT`, `OM_JWT`
- `pyspark_lineage_hook.py`: module để pipeline PySpark tự sinh config edges sau mỗi job
- `examples/pyspark_job_lineage_example.py`: ví dụ tích hợp hook trong job PySpark

## Run
1. Tạo file env:

```bash
cp lineage/.env.lineage.example lineage/.env.lineage
```

2. Cập nhật `OM_API_HOSTPORT` và `OM_JWT` trong `lineage/.env.lineage`.

3. Sửa config theo bảng thực tế.

4. Chạy script:

```bash
python3 lineage/lineage_agent_trino_hive.py \
  --config lineage/lineage_agent_config.example.yaml
```

Tùy chọn chỉ định env file khác:

```bash
python3 lineage/lineage_agent_trino_hive.py \
  --config lineage/lineage_agent_config.example.yaml \
  --env-file dbt_projects/.env.openmetadata.local
```

## Notes
- Có thể đặt `service` ở root làm mặc định.
- Nếu flow đi qua nhiều service, khai báo rõ `from.service` và `to.service` trên từng edge.
- Script idempotent: chạy lại không tạo cạnh trùng.
- Script tự load env theo thứ tự: `--env-file` -> `lineage/.env.lineage` -> `.env`.

## Tích hợp vào pipeline PySpark hiện tại
Trong mỗi job ETL, sau bước `write` thành công:

1. Tạo collector:

```python
collector = LineageCollector(host_port="http://localhost:8036/api", jwt_token_env="OM_JWT")
```

2. Ghi lại edge raw -> silver hoặc silver -> gold:

```python
collector.add_edge(
  source=TableNode.from_table_name("crm-raw", "hive.bi_raw.crm_deals_raw"),
  target=TableNode.from_table_name("crm-silver", "hive.bi_silver.crm_deals"),
  description="PySpark ETL raw to silver",
)
```

3. Kết thúc job thì sync lineage:

```python
config_path = "lineage/generated/lineage_edges.yaml"
collector.write_config(config_path)
collector.run_agent(config_path, agent_script_path="lineage/lineage_agent_trino_hive.py")
```

Gợi ý production:
- Chỉ gọi `run_agent` khi toàn bộ ETL job thành công.
- Với DAG nhiều task, gom edges theo từng task rồi sync một lần ở task cuối.
