# SPEC: Tối Ưu Hóa Spark Job Xử Lý Dữ Liệu Lớn (50TB Date Column Transformation)

## Problem Statement

Một Spark Job cần chuyển đổi một cột ngày tháng có định dạng string `YYYY-MM-DD` sang 3 cột riêng biệt (`year`, `month`, `day`) trên một tập dữ liệu có kích thước 50 Terabytes. Kịch bản xử lý hiện tại đã bao gồm các quyết định cốt lõi (Parquet format, native functions, Narrow Transformation awareness) nhưng còn thiếu một số khía cạnh quan trọng: chiến lược ghi output, tính toán cluster sizing cụ thể, input partition sizing, speculative execution, và một framework để điều chỉnh chiến lược khi quy mô dữ liệu thay đổi (100GB → 1TB → 5TB → 50TB).

## Solution

Xây dựng một bộ tài liệu phỏng vấn hoàn chỉnh bao gồm:
1. Kịch bản xử lý 50TB đã được bổ sung các thiếu sót.
2. Bộ nguyên lý cốt lõi (4 Principles) để suy luận chiến lược cho bất kỳ quy mô nào.
3. Bảng quyết định (Decision Matrix) so sánh tham số tối ưu giữa 100GB, 1TB, 5TB, 50TB.
4. Framework "Right-Sizing Decision Tree" để tránh over-engineering hoặc under-engineering.

## User Stories

1. As a Data Engineer interview candidate, I want to explain the 5-layer optimization approach for a 50TB Spark Job, so that the interviewer sees I understand both theory and production-grade practices.
2. As a Data Engineer, I want to quantify the difference between Narrow and Wide Transformations on large datasets, so that I can predict whether a job needs Shuffle tuning.
3. As a Data Engineer, I want to calculate the optimal number of tasks and executors for a given data size, so that I avoid both under-parallelism and scheduling overhead.
4. As a Data Engineer, I want to know when Spark is overkill (e.g., 100GB) and recommend simpler tools (Pandas/DuckDB), so that I demonstrate pragmatic engineering judgment.
5. As a Data Engineer, I want to articulate the "tail latency" problem at scale (200K+ tasks), so that I can justify speculative execution and monitoring infrastructure.
6. As a Data Engineer, I want to design the output write strategy (partitioning, file count, compression) to avoid small files problem, so that downstream consumers can read efficiently.
7. As a Data Engineer, I want to estimate job runtime and cluster cost for different data sizes, so that I can propose cost-effective infrastructure to leadership.
8. As a Data Engineer, I want to handle the follow-up question "What if the requirement changes to include a GROUP BY?", so that I can pivot the strategy to address Wide Transformations.

## Implementation Decisions

- **Nguyên lý kiến trúc**: Chiến lược xử lý được xây dựng trên 4 nguyên lý bất biến:
  1. Minimize Data Touched (Partition Pruning → Column Pruning → Predicate Pushdown → Compression)
  2. Minimize Data Movement (tránh Shuffle — nhận diện Narrow vs Wide Transformations)
  3. Right-Sized Parallelism (128-512MB per task sweet spot)
  4. Stay in JVM (native functions > Pandas UDF > Python UDF)

- **Bổ sung Write Strategy**: Output phải sử dụng `.partitionBy("year", "month")` khi ghi Parquet, kết hợp `spark.sql.files.maxRecordsPerFile` để kiểm soát kích thước file.

- **Bổ sung Input Partition Sizing**: Tăng `spark.sql.files.maxPartitionBytes` từ 128MB lên 256-512MB cho 50TB để giảm tổng tasks từ 400K xuống 100-200K, giảm Driver scheduling pressure.

- **Bổ sung Reliability**: Bật `spark.speculation=true` cho quy mô >5TB vì xác suất straggler task tăng theo số lượng tasks (P(failure) ≈ 1 - (1-p)^N).

- **Decision Matrix**: Bảng so sánh 4 cấp quy mô (100GB, 1TB, 5TB, 50TB) bao gồm: công cụ phù hợp, cluster size, partition config, shuffle config, output strategy, monitoring level.

## Testing Decisions

- Đây là tài liệu phỏng vấn (documentation), không phải production code. Kiểm tra tính chính xác bằng:
  - Cross-reference với Apache Spark official documentation cho mỗi config parameter.
  - Verify công thức tính toán cluster sizing bằng số liệu thực tế.
  - Peer review bởi người có kinh nghiệm production Spark.

## Out of Scope

- Code chạy được (runnable Spark application) — đã có ở `research-spark-code-rdd-dataframe.md`.
- Benchmark thực tế trên cloud (AWS EMR cost calculation).
- Streaming use case (Structured Streaming cho real-time date parsing).
- Deep dive vào Spark internals (Catalyst Optimizer rule ordering, Tungsten memory layout).

## Further Notes

- Phiên bản hiện tại của kịch bản xử lý đã tốt ở mức ~70%. Các bổ sung chính tập trung vào 3 điểm mù: Write Strategy, Input Partition Sizing, và Reliability (Speculative Execution).
- Framework "Right-Sizing" là giá trị cộng thêm lớn nhất — nó biến câu trả lời từ "thuộc lòng một kịch bản" thành "hiểu nguyên lý và suy luận được cho mọi trường hợp", đây là điều phân biệt Senior vs Junior trong mắt interviewer.
