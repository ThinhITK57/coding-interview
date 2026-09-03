# KHUNG QUYẾT ĐỊNH ARCHITECTURE & TUNING CHO BIG DATA (7 BƯỚC TINH GỌN)

> **Tài liệu tối giản, tập trung vào hành động (actionable rules). Không lý thuyết lan man.**
> Sử dụng khung này để đưa ra quyết định thiết kế chỉ trong 2 phút khi làm việc hoặc phỏng vấn.

---

## 1. INPUT (Dạng vật lý của nguồn)
*   **Câu hỏi:** Dữ liệu nguồn dung lượng bao nhiêu, lưu ở đâu, format và compression gì?
*   **Quy tắc quyết định:**
    *   `Format là CSV/JSON` + `Size > 100GB` $\rightarrow$ **Bắt buộc** chạy job convert thô sang Parquet/ORC trước rồi mới xử lý.
    *   `File nén Gzip (.gz) + CSV` $\rightarrow$ **Non-splittable**. Spark sẽ chạy single task cho cả file $\rightarrow$ Phải `.repartition()` ngay sau khi đọc.
    *   `Kích thước file trung bình < 16MB` $\rightarrow$ Lỗi **Small Files**. Phải cấu hình gộp file khi đọc.
*   **Cấu hình Spark:**
    *   Gộp file nhỏ khi đọc: `spark.sql.files.openCostInBytes` = `4194304` (4MB - mặc định).
    *   Kích thước task đọc: `spark.sql.files.maxPartitionBytes` = `134217728` (128MB mặc định. Tăng lên 256MB/512MB nếu data > 10TB để giảm tổng số task).

---

## 2. OUTPUT (Yêu cầu đích & Downstream)
*   **Câu hỏi:** Đích ghi là gì, hành vi ghi (Overwrite/Append/Merge), và downstream truy vấn thế nào?
*   **Quy tắc quyết định:**
    *   `Ghi vào RDBMS/NoSQL` + `Data > 1TB` $\rightarrow$ **Tránh** JDBC write trực tiếp. Hãy ghi ra S3/GCS Parquet trước, sau đó dùng lệnh `COPY` của DB để load.
    *   `Downstream lọc theo Col_A` $\rightarrow$ Ghi kết hợp `.partitionBy("Col_A")`.
    *   `Downstream lọc theo Col_B (cardinality cao)` $\rightarrow$ Thực hiện `Sort` hoặc `Z-Order` theo Col_B trước khi ghi.
    *   `Ghi đè từng phần (Upsert/Merge)` $\rightarrow$ **Bắt buộc** dùng Delta Lake hoặc Apache Iceberg thay vì Parquet thuần.
*   **Cấu hình Spark:**
    *   Tránh ghi file quá lớn: `spark.sql.files.maxRecordsPerFile` = `5000000` (giới hạn ~5 triệu dòng/file).
    *   Ghi đè partition động: `spark.sql.sources.partitionOverwriteMode` = `dynamic`.

---

## 3. TRANSFORMATION (Phép toán & Shuffle)
*   **Câu hỏi:** Phép toán là Narrow (Zero Shuffle) hay Wide (Có Shuffle)?
*   **Quy tắc quyết định:**
    *   `Chỉ có Narrow (map, filter, withColumn)` $\rightarrow$ **Bỏ qua** tối ưu hóa shuffle partitions. Tập trung 100% vào I/O (Read/Write).
    *   `Có Wide (Join/GroupBy)` $\rightarrow$ **Bắt buộc** tối ưu hóa số lượng partition sau shuffle.
    *   `Join với bảng nhỏ < 2GB` $\rightarrow$ Dùng **Broadcast Hash Join** bằng hint: `df_large.join(broadcast(df_small), "id")`.
    *   `Khóa bị lệch dữ liệu (Data Skew)` $\rightarrow$ Bật AQE Skew Join hoặc áp dụng kỹ thuật **Salting** (thêm key ngẫu nhiên).
    *   `Sử dụng hàm tự chế (UDF) trong Python` $\rightarrow$ Đổi sang Spark SQL native functions. Nếu không thể, dùng **Pandas UDF** (Arrow).
*   **Cấu hình Spark:**
    *   Tự động join bảng nhỏ: `spark.sql.autoBroadcastJoinThreshold` = `10485760` (10MB mặc định, nâng lên tối đa 2GB nếu RAM dư).
    *   Xử lý skew join tự động: `spark.sql.adaptive.skewJoin.enabled` = `true`.

---

## 4. MATHEMATICS (Công thức tính toán nhanh)
Không đoán mò cấu hình. Áp dụng 5 công thức sau:

1.  **Số task đọc tối ưu:** 
    $$N_{\text{tasks}} = \frac{\text{Dung lượng dữ liệu thô}}{\text{Partition Size (128MB - 512MB)}}$$
2.  **Số lượng Shuffle Partition tối ưu (Wide Transform):**
    $$N_{\text{shuffle\_partitions}} = \frac{\text{Dung lượng data shuffle (Bytes)}}{200\text{ MB}}$$
3.  **Cores mỗi Executor (Sweet Spot):**
    $$\text{Executor Cores} = 5$$
    *(Tránh >5 vì nghẽn I/O và GC pause lớn; Tránh <2 vì mất khả năng chia sẻ cache trong node).*
4.  **RAM mỗi Executor:**
    $$\text{Executor Memory} = 4\text{GB} \text{ đến } 8\text{GB} \times \text{Executor Cores}$$
    *(Thường set 16GB - 32GB. Memory Overhead mặc định = 10% Executor Memory).*
5.  **RAM tối thiểu của Driver (Tránh OOM Driver):**
    $$\text{Driver Memory} = \max\left(4\text{GB}, N_{\text{tasks}} \times 50\text{KB}\right)$$

---

## 5. RELIABILITY (Độ bền bỉ & Dung lỗi)
*   **Câu hỏi:** Job chạy trong bao lâu, trên hạ tầng nào (On-demand hay Spot)?
*   **Quy tắc quyết định:**
    *   `Chạy trên Cloud Spot/Preemptible Instances` $\rightarrow$ Tăng số lần thử lại của task và dùng `MEMORY_AND_DISK` cho caching.
    *   `Tổng số task > 10,000` $\rightarrow$ Xác suất gặp node bị chậm (straggler) rất cao $\rightarrow$ **Bắt buộc** bật Speculative Execution.
*   **Cấu hình Spark:**
    *   Bật chạy đầu cơ: `spark.speculation` = `true`.
    *   Ngưỡng kích hoạt chạy đầu cơ: `spark.speculation.multiplier` = `1.5`, `spark.speculation.quantile` = `0.8`.
    *   Số lần thử lại task tối đa: `spark.task.maxFailures` = `8` (môi trường Spot) hoặc `4` (môi trường On-demand).

---

## 6. ENVIRONMENT (Hạ tầng vật lý)
*   **Câu hỏi:** Hệ quản trị tài nguyên là gì, cluster dùng chung hay riêng?
*   **Quy tắc quyết định:**
    *   `Dữ liệu ở S3/GCS` + `Cluster ở region khác` $\rightarrow$ **Dừng job ngay**. Chi phí Data Transfer và network latency sẽ phá hủy performance. Yêu cầu đưa cluster về cùng Region/AZ với storage.
    *   `Cluster dùng chung (Multi-tenant YARN/K8s)` $\rightarrow$ Cấu hình trần tài nguyên tối đa để tránh bị kill job do chiếm dụng hoặc làm nghẽn các job khác.
*   **Cấu hình Spark:**
    *   Bật co giãn executor: `spark.dynamicAllocation.enabled` = `true`.
    *   Giới hạn executor tối đa: `spark.dynamicAllocation.maxExecutors` = `N` (Giới hạn trần dựa trên dung lượng queue được cấp).

---

## 7. VERIFY (Nghiệm thu chất lượng & Hiệu năng)
*   **Câu hỏi:** Làm sao biết job chạy đúng và tối ưu bộ nhớ?
*   **Checklist kiểm tra nhanh sau khi chạy (Spark UI):**
    *   [ ] `Spill (Memory) / Spill (Disk)` tại mỗi Stage = **0 Bytes**. *(Nếu >0 $\rightarrow$ Phải tăng shuffle partitions hoặc tăng RAM executor)*.
    *   [ ] `Garbage Collection (GC) Time` < **10%** tổng Task Time. *(Nếu >10% $\rightarrow$ Giảm cores per executor hoặc đổi sang Kryo serializer)*.
    *   [ ] Tỷ lệ dòng: `Count(Input)` khớp với `Count(Output) + Count(Rejected)`.
    *   [ ] `Max Task Duration` không vượt quá **3x** `Median Task Duration` (Kiểm tra lệch dữ liệu).

---

## BẢN ĐỒ QUYẾT ĐỊNH NHANH THEO QUY MÔ DỮ LIỆU

| Tiêu chí | **< 100GB** | **100GB - 5TB** | **> 5TB (Ví dụ: 50TB)** |
|:---|:---|:---|:---|
| **Công cụ lựa chọn** | Pandas / DuckDB (Không cần Spark) | Spark Standalone / Small Cluster | Spark Cluster lớn (YARN/K8s) |
| **Max Partition Bytes** | Mặc định (128MB) | 128MB - 256MB | 256MB - 512MB (Giảm tải Driver) |
| **Speculative Execution**| Tắt | Tắt | **Bật** (`spark.speculation=true`) |
| **Shuffle Partitions** | 10 - 50 | 200 - 500 | 1000 - 4000+ (Công thức Phase 4) |
| **Driver Memory** | 2GB - 4GB | 4GB - 8GB | 12GB - 32GB (Tránh Driver OOM) |
| **Output File Strategy** | Ghi trực tiếp | `partitionBy` + `coalesce` | `partitionBy` + `maxRecordsPerFile` |
