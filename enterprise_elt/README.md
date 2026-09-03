# Enterprise Production ELT Package (`enterprise_elt`)

Package xử lý luồng dữ liệu chuẩn **Production-Grade Cloud-Native Lakehouse** đáp ứng toàn bộ các yêu cầu:
- **Orchestration**: Prefect 3.0
- **Data Engine**: Apache Spark 2.3.2 (Tối ưu hóa bộ nhớ, chống double scan, chạy được trên cụm legacy)
- **Data Quality & DLQ**: Rule Engine phân luồng Single-Pass, cô lập bản ghi lỗi vào Dead Letter Queue
- **Multi-Target Sink**: MinIO Raw Backup + Trino Dev Sandbox + Trino Prod Cleaned (Upsert qua Left Anti Join & unionByName)
- **Concurrency & Versioning**: Deduplicate theo Window và điều phối Dimension chạy trước Fact

---

## 1. Cấu trúc Module

```text
enterprise_elt/
├── config/
│   └── pipeline_config.py      # Class PipelineConfig: argparse, env switcher
├── clients/
│   └── api_client.py           # Class ResilientAPIIngestor: Connection pool, rate limiter, retry
├── processors/
│   ├── json_flattener.py       # Class SparkJSONFlattener: đệ quy làm phẳng StructType
│   ├── quality_engine.py       # Class DataQualityEngine: metadata rules, concat_ws DLQ tagger
│   └── version_manager.py      # Class VersionDeduplicator: Window row_number multi-version dedup
├── sinks/
│   └── warehouse_sink.py       # Class MultiTargetWarehouseSink: 3-target writer & upsert
├── main.py                     # CLI Entrypoint cho Spark-submit / Local runner
└── prefect_flow.py             # Prefect Master Flow điều phối thứ tự nạp
```

---

## 2. Hướng dẫn Chạy CLI (`argparse`)

### Chạy môi trường Dev (thử nghiệm sandbox):
```bash
python -m enterprise_elt.main \
  --env dev \
  --domain-target muc-1 \
  --dlq-threshold-ratio 0.05 \
  --rate-limit-rps 5
```

### Chạy môi trường Prod với Spark-Submit trên cụm:
```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files enterprise_elt.zip \
  enterprise_elt/main.py \
  --env prod \
  --domain-target muc-2 \
  --batch-id batch_20260904_01 \
  --dlq-threshold-ratio 0.02 \
  --rate-limit-rps 15
```

---

## 3. Khởi chạy với Prefect 3.0

```bash
python enterprise_elt/prefect_flow.py
```
Flow sẽ tự động điều phối:
1. Nạp Master Dimension (`muc-1`) hoàn tất trước.
2. Sau khi Dimension sẵn sàng, kích hoạt song song các luồng nạp Fact (`muc-2`, `muc-3`, `muc-4`).
