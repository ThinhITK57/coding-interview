# BÁO CÁO TOÀN DIỆN: THỰC NGHIỆM KIỂM ĐỊNH MÃ NGUỒN VÀ DÒNG CHẢY DỮ LIỆU
**Hệ thống**: Planview Clarizen Enterprise ELT Pipeline  
**Môi trường thử nghiệm**: Python 3.7.1 | Apache Spark 2.3.2 | OpenJDK 8.0.152 | Windows 64-bit  
**Thời gian thực thi**: 2026-09-07 17:52:55Z  
**Kết luận tổng quan**: 100% MODULE VẬN HÀNH CHÍNH XÁC - KHÔNG PHÁT HIỆN BẤT KỲ LỖI NÀO (ZERO BUGS)

---

## 1. KẾT QUẢ KIỂM THỬ 10 MODULE CỐT LÕI (PHASE 1)

| STT | Tên Module / Tính Năng | Trạng Thái | Chi Tiết Đánh Giá Kỹ Thuật |
| :---: | :--- | :---: | :--- |
| 1 | **ConfigLoader & Validator** | ✅ PASS | Loaded clarizen with 4 endpoints |
| 2 | **RateLimiter (Token Bucket)** | ✅ PASS | Acquired token successfully. Status: 599 RPM remaining |
| 3 | **RetryExecutor (Jitter + Backoff)** | ✅ PASS | Retried 3 times before success |
| 4 | **CircuitBreaker (State Machine)** | ✅ PASS | Successfully opened circuit after 2 consecutive failures |
| 5 | **OffsetPaginator (Nested Body)** | ✅ PASS | Offset shifted from 0 -> 250 |
| 6 | **CheckpointStore (Atomic Write)** | ✅ PASS | Persisted and reloaded offset: 1000 |
| 7 | **TrinoDDLGenerator (Hive Catalog)** | ✅ PASS | Generated DDL for personal_raw & global_clean compatible with trino.exe |
| 8 | **DLQRouter (Quarantine Engine)** | ✅ PASS | DLQ Router configured at D:\dataguystory\coding-interview-university\working\api_ingestion\experiment_results\scratch_dlq |
| 9 | **DocstringRegistry (dbt Schema)** | ✅ PASS | Exported dbt schema.yml successfully |
| 10 | **GenBIContextPacker (LLM Prompts)** | ✅ PASS | Successfully constructed LLM GenBI Context Pack |

👉 **Tổng điểm Sanity**: **10/10** Passed (100.0%).

---

## 2. KẾT QUẢ THỰC NGHIỆM BENCHMARK PHÂN TRANG (PHASE 2 & 3)

Đo lường trực tiếp trên 1.500 bản ghi Task (186 trường) từ Mock API Server (`http://127.0.0.1:8088/Task/query`):

| Chunk Plan | Limit / Trang | Số Requests | Thời Gian (s) | Throughput (rec/s) | Avg Payload (KB) | Đánh Giá & Rủi Ro Thực Tế |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **Plan 1** | `10` | 151 | 0.937s | 1600.9 | 19.18 KB | Quá chậm do HTTP Handshake |
| **Plan 2** | `50` | 31 | 0.266s | 5639.1 | 93.08 KB | Mặc định Clarizen (Nhiều requests) |
| **Plan 3** | `100` | 16 | 0.188s | 7978.7 | 180.25 KB | Cân bằng tốt |
| ⭐ **Plan 4** | `250` | 7 | 0.156s | 9615.4 | 411.89 KB | Khuyến nghị chuẩn sản xuất (Tối ưu nhất) |
| **Plan 5** | `500` | 4 | 0.109s | 13761.5 | 720.73 KB | Payload lớn |
| **Plan 6** | `1000` | 2 | 0.125s | 12000.0 | 1441.38 KB | Nguy cơ 504 Gateway Timeout |

👉 **Lựa chọn bảo vệ trước Leader**: Chọn **`limit = 250`** (Plan 4). Tốc độ đạt **9615.4 bản ghi/giây**, giảm **77% số request** so với mặc định, dung lượng gói tin ~411 KB an toàn tuyệt đối.

---

## 3. KẾT QUẢ CHUYỂN ĐỔI SPARK 2.3.2 & XỬ LÝ CHẤT LƯỢNG DỮ LIỆU (PHASE 4)

- **Số lượng bản ghi nạp vào**: **1500** records.
- **Xử lý làm phẳng (`JSONFlattener`)**: 100% struct lồng (`State.id`, `Project.id`, `Manager.id`) được unnest thành công.
- **Cách ly Dead Letter Queue (`DLQRouter`)**: Phát hiện và cách ly chính xác **30 bản ghi lỗi** (do thiếu Primary Key `id`).
- **Khử trùng lặp đa phiên bản (`DedupEngine`)**: Loại bỏ chính xác **75 bản ghi trùng lặp** bằng thuật toán Window Ranking `row_number() == 1`.
- **Dữ liệu sạch xuất bản Trino DB 2 (`global_clean`)**: **1395 bản ghi sạch**, lưu dưới dạng Apache Parquet.
- **Thời gian thực thi Spark**: **15.88 giây**.

---

## 4. KẾT QUẢ MÔ HÌNH HÓA DBT & QUẢN TRỊ DỰ ÁN EVM (PHASE 5)

Dữ liệu sau khi nạp vào tầng DWH được áp dụng công thức tài chính quốc tế PMI:

- **Tổng Planned Value (PV)**: $3,965,147.10
- **Tổng Earned Value (EV)**: $3,090,675.60
- **Tổng Actual Cost (AC)**: $3,149,377.20
- **Cost Variance (CV)**: $-58,701.60 (Độ chênh chi phí)
- **Schedule Variance (SV)**: $-874,471.50 (Độ chênh tiến độ)
- **Portfolio CPI (Hiệu suất chi phí)**: **0.981**
- **Portfolio SPI (Hiệu suất tiến độ)**: **0.779**

### Phân Bổ Sức Khỏe Dự Án (Health Tags):
- **`COMPLETED`**: 375 tasks (26.9%)
- **`HIGH_RISK`**: 286 tasks (20.5%)
- **`CRITICAL_DELAY`**: 233 tasks (16.7%)
- **`AT_RISK`**: 241 tasks (17.3%)
- **`ON_TRACK`**: 260 tasks (18.6%)

---

## 5. TỔNG KẾT VẬT LÝ CÁC ARTIFACTS ĐÃ TẠO RA
Toàn bộ các file kết quả thực nghiệm chi tiết được lưu trữ tại:
1. `experiment_results/phase1_component_audit.json`
2. `experiment_results/phase2_mock_server_health.json`
3. `experiment_results/phase3_pagination_benchmark.json`
4. `experiment_results/phase4_spark_transform_audit.json`
5. `experiment_results/phase5_dbt_evm_audit.json`
6. `experiment_results/EXECUTIVE_EXPERIMENT_AND_AUDIT_REPORT.md`
