# Kế Hoạch Thực Nghiệm Thực Tế Toàn Diện: Mockup Dữ Liệu Task, Benchmark Phân Trang, Spark 2.3.2 Transform, dbt & Trino Final Storage

## Mục Tiêu & Bối Cảnh
Chứng minh tính đúng đắn và chuẩn xác 100% của toàn bộ giải pháp kiến trúc đã đề xuất thông qua **thực nghiệm trực tiếp trên dữ liệu thật/mockup sinh từ schema 186 trường trong `task_data_raw.txt`**:
1. Thiết lập môi trường cách ly chuẩn xác: **Conda environment `python=3.7.9`**, **`openjdk=11`**, và **`pyspark=2.3.2`** trên máy local.
2. Xây dựng **Engine Mockup dữ liệu Task chuẩn Planview**: Sinh hàng ngàn bản ghi đầy đủ 7 nhóm nghiệp vụ (WBS, EVM, Financials CAPEX/OPEX, Hours, Baseline, Status, Reference Objects).
3. **Thực nghiệm Benchmark các Chunk Plan**: Thử nghiệm các mức `limit` khác nhau ($10, 50, 100, 250, 500, 1000$) để đo lường độ trễ mạng, dung lượng RAM, payload size và xác định con số tối ưu cho Planview API.
4. **Chạy luồng dữ liệu End-to-End**: Mock Server $\rightarrow$ Extractor $\rightarrow$ Checkpoint $\rightarrow$ Spark 2.3.2 Transform (Flattener, Dedup Window Ranking, DLQ) $\rightarrow$ Trino Storage $\rightarrow$ dbt (Staging, Intermediate EVM, Marts Fact/Dim).
5. **Soạn thảo Guide Line Chuyên Sâu**: Mô tả chi tiết từng bước luồng dữ liệu đi qua source code kèm bằng chứng số liệu để Leader hiểu tường tận.

---

## User Review Required
> [!IMPORTANT]
> - Môi trường Conda sẽ được tạo mới tại: `D:\miniconda-envs\envs\planview-spark37` với `python=3.7.9`, `openjdk=11`, `pyspark=2.3.2`.
> - Mock Server sẽ chạy cục bộ bằng `http.server` hoặc `Flask/urllib` trên cổng nội bộ (e.g. `127.0.0.1:8088`) để Extractor gọi trực tiếp như API thật.
> - Dữ liệu thực nghiệm sẽ được ghi ra thư mục lưu trữ cục bộ giả lập MinIO/Trino Warehouse (`./storage_data/warehouse/hive/personal_raw` và `./storage_data/warehouse/hive/global_clean`).

---

## Proposed Changes

### Phase 1: Thiết Lập Môi Trường Conda (Python 3.7.9 + JDK 11 + PySpark 2.3.2)
- Tạo environment `planview-spark37`:
  ```powershell
  & "D:\miniconda-envs\Scripts\conda.exe" create -n planview-spark37 python=3.7.9 openjdk=11 pyspark=2.3.2 -c pkgs/main -y
  ```
- Cài đặt thêm các package phụ trợ: `pyarrow`, `pandas`, `duckdb` (để test SQL dbt cục bộ giả lập Trino), `requests`.

---

### Phase 2: Mockup Data Generator & Pagination Benchmark
Dựa trên 186 trường trong [task_data_raw.txt](file:///D:/dataguystory/coding-interview-university/task_data_raw.txt):

#### [NEW] [mock_data_generator.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/mock_data_generator.py)
- Sinh danh mục ID ngẫu nhiên: Projects, Users, States, Phases.
- Sinh các bản ghi Task hoàn chỉnh với cấu trúc JSON chuẩn Clarizen:
  * Object lồng nhau: `"State": {"id": "/State/Active"}`, `"Parent": {"id": "/Task/..."}`, `"Project": {"id": "/Project/..."}`.
  * Chỉ số tài chính & EVM: Budget, ActualCost, Hours, Baseline, PercentCompleted.
  * Bổ sung các bản ghi trùng lặp (duplicate IDs) với timestamp khác nhau và bản ghi lỗi để kiểm thử Dedup & DLQ.

#### [NEW] [benchmark_pagination_chunks.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/benchmark_pagination_chunks.py)
- Chạy thử nghiệm các cấu hình phân trang:
  * **Plan A**: `limit = 10` (Rất nhỏ, kiểm tra overhead kết nối HTTP)
  * **Plan B**: `limit = 50` (Mặc định chuẩn Clarizen)
  * **Plan C**: `limit = 100` (Cân bằng tải)
  * **Plan D**: `limit = 250` (Tối ưu throughput mạng)
  * **Plan E**: `limit = 500` & `limit = 1000` (Kiểm tra giới hạn buffer & timeout)
- Đo lường và vẽ bảng so sánh:
  * Tổng thời gian crawl ($T_{\text{total}}$)
  * Payload size trên mỗi request (KB)
  * Mức tiêu thụ RAM đỉnh (Peak RAM MB)
  * Tỷ lệ overhead network handshake
  * Đưa ra kết luận con số `limit`/`offset` tối ưu nhất để bảo vệ với Leader.

---

### Phase 3: Mock Server & Luồng Ingestion Thực Tế
#### [NEW] [mock_epm_server.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/mock_epm_server.py)
- HTTP Server nội bộ mô phỏng chính xác hành vi của API Planview Clarizen:
  * Hỗ trợ method `POST /Task/query`.
  * Đọc `body["paging"]["from"]` và `body["paging"]["limit"]`.
  * Trả về JSON `{ "entities": [...], "paging": {...} }`.
  * Giả lập một số tình huống timeout, rate limit 429 để kiểm chứng `ResilientHTTPClient` và `RetryExecutor`.

- Chạy [ingestion/extractor.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/ingestion/extractor.py) kết nối vào Mock Server:
  * Xác thực token qua [auth/token_auth.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/auth/token_auth.py).
  * Chạy tuần tự qua các batch, commit checkpoint sau từng trang.
  * Xuất file Raw JSON Backup nén gzip.

---

### Phase 4: Spark 2.3.2 Transform & Storage
#### [NEW] [run_spark_transform_experiment.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/run_spark_transform_experiment.py)
- Chạy bằng Python 3.7.9 và PySpark 2.3.2:
  1. Đọc dữ liệu Raw JSON vào Spark DataFrame.
  2. Thực hiện làm phẳng đệ quy bằng [transform/json_flattener.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/json_flattener.py).
  3. Phân luồng DLQ cho bản ghi lỗi bằng [storage/dlq_router.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/storage/dlq_router.py).
  4. Khử trùng lặp Idempotent bằng [transform/dedup_engine.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/dedup_engine.py) (Window Ranking).
  5. Tạo Inferred Dimension stubs bằng [transform/race_condition_router.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/transform/race_condition_router.py).
  6. Ghi dữ liệu sạch ra Parquet phục vụ bảng `hive.personal_raw` và `hive.global_clean`.

---

### Phase 5: Chạy dbt Transform & Xuất Bảng Final Cho Trino
#### [NEW] [run_dbt_execution_simulation.py](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/scripts/run_dbt_execution_simulation.py)
- Thực thi toàn bộ pipeline dbt 3 tầng đã viết:
  * `stg_planview__tasks`
  * `int_tasks__evm_metrics` (tính toán đầy đủ PV, EV, AC, CV, SV, CPI, SPI, EAC, ETC)
  * `dim_tasks` (bảng chiều WBS)
  * `fct_task_daily_snapshot` (bảng fact snapshot tiến độ)
- Xuất kết quả final ra Parquet/DuckDB và in kết quả preview các chỉ số EVM thực tế.

---

### Phase 6: Tài Liệu Hướng Dẫn Chi Tiết Cho Leader (Guideline Document)
#### [NEW] [docs/LEADER_DEEP_DIVE_GUIDELINE.md](file:///d:/dataguystory/coding-interview-university/working/api_ingestion/docs/LEADER_DEEP_DIVE_GUIDELINE.md)
- Phân tích chi tiết đường đi của 1 bản ghi Task cụ thể từ lúc là JSON thô trong API Planview cho đến khi thành dòng dữ liệu trong Marts Fact của Trino.
- Trình bày biểu đồ Benchmark so sánh các Chunk Plan kèm khuyến nghị kỹ thuật.
- Bằng chứng kiểm thử và số liệu thực nghiệm.

---

## Verification Plan

### Automated Tests
- Tạo conda env thành công:
  ```powershell
  & "D:\miniconda-envs\envs\planview-spark37\python.exe" --version
  ```
- Kiểm tra PySpark 2.3.2 tương thích Java 11:
  ```powershell
  & "D:\miniconda-envs\envs\planview-spark37\python.exe" -c "import pyspark; sc = pyspark.SparkContext('local[1]'); print(sc.version); sc.stop()"
  ```
- Chạy benchmark phân trang:
  ```powershell
  & "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/benchmark_pagination_chunks.py
  ```
- Chạy toàn bộ pipeline transform và dbt:
  ```powershell
  & "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/run_spark_transform_experiment.py
  & "D:\miniconda-envs\envs\planview-spark37\python.exe" scripts/run_dbt_execution_simulation.py
  ```
- Kiểm tra các file Parquet đầu ra của Trino và dữ liệu snapshot EVM.
