# 🎯 50 Kịch Bản Phỏng Vấn Chuyên Sâu (Level Middle / Senior DE)

Bộ 50 kịch bản phỏng vấn này được thiết kế theo đúng nguyên lý **Cơ chế Ngầm & Con số Thực nghiệm (Rolls-Royce Principle)**, bám sát 4 mảng trách nhiệm chuyên môn bạn đã nhận trong dự án **Fleet Maintenance & Repair Data Platform**.

---

## 📌 PHẦN 1: LOG-BASED CDC, POSTGRESQL WAL & DEBEZIUM (Câu 1 - 12)

### Kịch bản 1: Polling vs Log-Based CDC
- **Interviewer:** *"Tại sao em chọn Debezium CDC thay vì viết script Python `SELECT * WHERE updated_at > last_run` chạy cronjob 5 phút/lần cho nhẹ?"*
- **Ứng viên:** *"Dùng Query Polling có 3 nhược điểm chết người trong production:*
  1. *Tải I/O OLTP*: Lệnh `SELECT` quét bảng vài triệu dòng làm khóa I/O của Postgres Odoo, khiến nhân viên bị đơ màn hình khi tạo đơn.
  2. *Mất dữ liệu DELETE*: Nếu ai đó chạy `DELETE FROM customers WHERE id = 10`, polling query hoàn toàn không thể phát hiện.
  3. *Bỏ lỡ trạng thái trung gian*: Nếu 1 đơn sửa chữa chuyển `scheduled` → `in_progress` → `completed` trong 2 phút, lần polling ở phút thứ 5 chỉ thấy `completed`, mất sạch lịch sử.
  *Debezium đọc trực tiếp file Write-Ahead Log (WAL) mà Postgres tự ghi xuống đĩa khi commit. Tải CPU/RAM lên Postgres Odoo là 0%, bắt được 100% thay đổi kể cả DELETE và trạng thái trung gian dưới 1 giây."*

### Kịch bản 2: REPLICA IDENTITY DEFAULT vs FULL
- **Interviewer:** *"Tại sao em phải chạy `ALTER TABLE customers REPLICA IDENTITY FULL;` trên Postgres?"*
- **Ứng viên:** *"Mặc định Postgres dùng `DEFAULT`, khi UPDATE nó chỉ ghi Primary Key (`id`) của row cũ vào WAL. CDC Event phát ra chỉ có `before: {id: 7}` và `after: {id: 7, status: 'thường niên'}`. Khi Spark Batch job đọc event để làm SCD Type 2, Spark cần biết status CŨ là gì để đóng record. Nếu `before` chỉ có `id`, Spark phải query ngược vào Postgres — vi phạm nguyên tắc 'Zero OLTP Querying'. `FULL` ép Postgres ghi toàn bộ cột cũ vào `before image` (`before: {id: 7, status: 'mới'}`), giúp Spark xử lý SCD2 hoàn toàn tự động."*

### Kịch bản 3: Xử lý Replication Slot Lag & Disk Full Risk
- **Interviewer:** *"Nếu Debezium worker bị crash 2 ngày, điều gì xảy ra với Postgres DB?"*
- **Ứng viên:** *"Postgres giữ 1 Logical Replication Slot cho Debezium. Slot này bảo vệ WAL, ngăn Postgres xóa các file WAL mà Debezium chưa đọc. Nếu Debezium chết 2 ngày, WAL log sẽ tích tụ liên tục trên ổ đĩa Postgres, có nguy cơ làm **đầy đĩa (Disk Full)** và crash cả DB Odoo. Giải pháp production của em là đặt `max_slot_wal_keep_size = 10GB` trong `postgresql.conf` và tạo alert Prometheus giám sát `pg_replication_slots.wal_status`."*

### Kịch bản 4: Debezium Initial Snapshot Overhead
- **Interviewer:** *"Lần đầu tiên Debezium connector start ở mode `initial`, nó làm thế nào để đọc toàn bộ DB mà không làm đơ Odoo?"*
- **Ứng viên:** *"Debezium thực hiện `SELECT` snapshot bằng cơ chế `REPEATABLE READ` isolation level trong 1 transaction duy nhất. Nó tạo 1 điểm nhất quán (consistent point), sau đó đọc dữ liệu snapshot mà không dùng `EXCLUSIVE LOCK` trên các bảng, cho phép các câu lệnh INSERT/UPDATE của Odoo vẫn diễn ra bình thường."*

### Kịch bản 5: Schema Evolution trong CDC
- **Interviewer:** *"Nếu DBA Odoo thêm 1 cột `discount_code` vào bảng `invoices`, Debezium và Kafka xử lý thế nào?"*
- **Ứng viên:** *"Debezium tự động đọc DDL change từ WAL và cập nhật schema của CDC Event. Nhờ em dùng `table.include.list` rõ ràng và cấu hình Kafka Connect `value.converter.schemas.enable=true`, field mới `discount_code` sẽ xuất hiện trong payload `after` của JSON event mà không làm vỡ các topic Kafka hiện tại."*

### Kịch bản 6: Message Keying & Partition Ordering
- **Interviewer:** *"Làm sao em đảm bảo các sự kiện CDC của cùng một khách hàng được xử lý đúng thứ tự thời gian trên Kafka?"*
- **Ứng viên:** *"Debezium tự động dùng Primary Key của bảng Postgres (ví dụ `customer_id`) làm Kafka Message Key. Trong Kafka, tất cả message có cùng Key sẽ luôn luôn được ghi vào **cùng một Partition**. Trong cùng 1 partition, thứ tự tin nhắn được bảo toàn 100% theo trình tự ghi của Producer."*

### Kịch bản 7: Debezium Tombstone Messages
- **Interviewer:** *"Tombstone message trong Debezium CDC là gì?"*
- **Ứng viên:** *"Khi một dòng bị DELETE, Debezium phát ra 1 event chứa `before` image và `after: null` với `op: d`. Ngay sau đó, Debezium phát tiếp 1 tin nhắn có Key = PK cũ và Value = `null` (gọi là Tombstone message). Tin nhắn này báo cho Kafka Topic (có `cleanup.policy=compact`) biết rằng key đó đã bị xóa hoàn toàn để Kafka Log Compaction dọn dẹp ổ đĩa."*

### Kịch bản 8: wal_level=logical vs replica
- **Interviewer:** *"Khác biệt giữa `wal_level=logical` và `wal_level=replica`?"*
- **Ứng viên:** *"`replica` chỉ ghi WAL đủ cho Physical Standby DB (replay lại các block đĩa). `logical` ghi thêm các thông tin Logical Decoding (tên bảng, tên cột, giá trị tuple trước và sau). Debezium bắt buộc cần `logical` để giải mã WAL thành JSON event."*

### Kịch bản 9: Duplicate CDC Events
- **Interviewer:** *"Debezium có đảm bảo Exactly-Once delivery sang Kafka không?"*
- **Ứng viên:** *"Debezium đảm bảo **At-Least-Once**. Nếu Debezium ghi tin nhắn vào Kafka thành công nhưng bị sập trước khi lưu offset vào `fleet-cdc-connect-offsets`, khi restart nó sẽ đọc lại WAL từ offset cũ và gửi lại tin nhắn bị trùng. Vì vậy downstream consumer (Spark / Redis Sync) phải thiết kế Idempotent (ghi đè theo PK)."*

### Kịch bản 10: PostgreSQL Toast Columns
- **Interviewer:** *"TOAST column trong Postgres ảnh hưởng gì đến CDC?"*
- **Ứng viên:** *"Khi cột lớn (như `TEXT` issue_summary) không bị UPDATE, Postgres sẽ không ghi giá trị TOAST column đó vào WAL để tiết kiệm băng thông. Debezium sẽ trả về mảng `__unavailable_value`. `REPLICA IDENTITY FULL` giải quyết được vấn đề này bằng cách ép ghi đầy đủ."*

### Kịch bản 11: Multi-Table CDC Consistency
- **Interviewer:** *"Nếu 1 transaction trong Odoo INSERT vào cả `work_orders` và `work_order_items`, CDC thể hiện thế nào?"*
- **Ứng viên:** *"Trong WAL, cả 2 lệnh INSERT nằm trong cùng 1 Transaction ID (`txId`). Debezium phát ra 2 events riêng biệt vào 2 Kafka topics khác nhau nhưng cả 2 events đều mang cùng trường `source.txId` và `source.lsn`, giúp downstream job biết chúng thuộc cùng 1 transaction."*

### Kịch bản 12: Heartbeat Action Topic
- **Interviewer:** *"Tại sao cần `heartbeat.interval.ms` trong Debezium config?"*
- **Ứng viên:** *"Nếu một bảng trong Postgres rất ít khi có thay đổi, Debezium sẽ không phát tin nhắn mới, làm offset của Kafka Connect bị đứng yên. `heartbeat.interval.ms` ép Debezium phát tin nhắn nhịp tim định kỳ để cập nhật LSN offset, ngăn Postgres giữ WAL log quá lâu."*

---

## 📌 PHẦN 2: PYSPARK STRUCTURED STREAMING & HDFS DATA LAKE (Câu 13 - 24)

### Kịch bản 13: Parquet Columnar Storage
- **Interviewer:** *"Tại sao em chọn định dạng Parquet cho Raw Data Lake trên HDFS?"*
- **Ứng viên:** *"Parquet là lưu trữ dạng cột (Columnar). 2 lợi ích lớn nhất:*
  1. *Column Projection*: Khi Spark batch đọc data chỉ lấy cột `latitude`, `longitude`, Spark bỏ qua 90% dung lượng các cột còn lại mà không cần scan đĩa.
  2. *Nén Snappy*: Dữ liệu dạng cột có cùng kiểu data nên nén cực tốt, giảm 70-80% dung lượng lưu trữ so với JSON raw."*

### Kịch bản 14: HDFS Partitioning Strategy
- **Interviewer:** *"Tại sao em phân vùng HDFS theo `year/month/day`?"*
- **Ứng viên:** *"Để đạt **Partition Pruning**. Khi Airflow chạy job tổng hợp báo cáo tháng 7/2026, Spark SQL tự động chỉ quét thư mục `hdfs:///fleet-datalake/raw/telemetry/year=2026/month=07/`, bỏ qua toàn bộ dữ liệu của các năm/tháng khác, giảm 99% I/O lãng phí."*

### Kịch bản 15: Spark Streaming Checkpointing & Exactly-Once
- **Interviewer:** *"Spark Structured Streaming làm sao đảm bảo Exactly-Once ghi xuống HDFS?"*
- **Ứng viên:** *"Phối hợp giữa Kafka Replayable Source và HDFS FileStreamSinkLog. Spark ghi nhận Kafka offset vào thư mục Checkpoint. Khi ghi file Parquet, Spark ghi ra file tạm. Chỉ khi micro-batch hoàn tất, Spark mới ghi một log commit nguyên tử vào metadata directory. Nếu sập giữa chừng, khi restart Spark đọc log checkpoint, xóa các file tạm chưa commit và đọc lại đúng Kafka offset cũ."*

### Kịch bản 16: Schema Enforcement trong Streaming
- **Interviewer:** *"Nếu cảm biến xe tải gửi về một JSON bị lỗi format hoặc thiếu trường, Spark Streaming của em có bị crash không?"*
- **Ứng viên:** *"Không. Em khai báo Explicit `StructType` Schema trong hàm `from_json()`. Bất kỳ JSON nào malformed hoặc không khớp schema sẽ bị Spark ép thành `NULL`. Em dùng filter loại bỏ các record null này, bảo vệ pipeline luôn chạy liên tục 24/7."*

### Kịch bản 17: Watermarking & Late Data
- **Interviewer:** *"Watermark trong Spark Streaming dùng để làm gì?"*
- **Ứng viên:** *"Watermark định nghĩa khoảng thời gian trễ tối đa cho phép của dữ liệu (ví dụ `.withWatermark("event_time", "10 minutes")`). Nếu xe tải mất sóng 5 phút rồi mới gửi telemetry về, Spark vẫn chấp nhận xử lý. Nếu mất sóng quá 10 phút, Spark sẽ drop dữ liệu trễ đó để giải phóng State Store trong bộ nhớ RAM."*

### Kịch bản 18: Small File Problem trên HDFS
- **Interviewer:** *"Spark Streaming chạy trigger 10 giây/lần sẽ tạo ra hàng ngàn file Parquet nhỏ (Small Files) trên HDFS. Em xử lý thế nào?"*
- **Ứng viên:** *"Small files làm quá tải NameNode RAM (mỗi file tốn ~150 bytes RAM trên NameNode). Em giải quyết bằng 2 bước:*
  1. *Đặt Trigger hợp lý*: `trigger(processingTime='1 minute')` để tích tụ data lớn hơn trước khi ghi.
  2. *Compaction Job*: Dùng Airflow chạy job định kỳ hàng đêm đọc các file Parquet nhỏ trong ngày, thực hiện `coalesce(1)` hoặc `repartition()` rồi ghi đè thành 1 file Parquet tối ưu (~128MB)."*

### Kịch bản 19: Processing Time vs Event Time Partitioning
- **Interviewer:** *"Em phân vùng HDFS theo thời gian nhận được tin nhắn (Kafka timestamp) hay thời gian thực tế xảy ra sự kiện (Payload timestamp)?"*
- **Ứng viên:** *"Em phân vùng theo **Event Time** (thời gian thực tế trong payload `timestamp`). Nếu phân vùng theo Processing Time, một tin nhắn bị trễ phát sinh lúc 23:59 đêm hôm trước nhưng 00:01 sáng hôm sau mới vào Kafka sẽ bị lọt sai sang thư mục ngày hôm sau, làm sai lệch báo cáo ngày."*

### Kịch bản 20: Spark Kryo Serializer
- **Interviewer:** *"Tại sao em set `spark.serializer = KryoSerializer`?"*
- **Ứng viên:** *"Kryo Serializer nhanh hơn 10 lần và nén nhỏ hơn nhiều so với Java Standard Serializer mặc định khi Spark shuffle dữ liệu giữa các executor nodes qua mạng."*

### Kịch bản 21: Spark Standalone vs YARN
- **Interviewer:** *"Tại sao em chọn Spark Standalone Mode thay vì YARN?"*
- **Ứng viên:** *"Spark Standalone đơn giản, nhẹ và tối ưu cho cụm dedicated 3 nodes chỉ chuyên chạy Spark. YARN thích hợp hơn khi cần chia sẻ tài nguyên chung với các hệ thống Hadoop MapReduce/Hive phức tạp khác."*

### Kịch bản 22: Spark Memory Management (Execution vs Storage)
- **Interviewer:** *"Bộ nhớ của Spark Executor phân chia thế nào?"*
- **Ứng viên:** *"Theo Unified Memory Manager (Spark 1.6+): 60% RAM dành cho Spark Memory (chia sẻ linh hoạt giữa Execution cho Shuffle/Join và Storage cho Caching). 40% còn lại cho User Data Structure và Overhead."*

### Kịch bản 23: FailOnDataLoss Config
- **Interviewer:** *"Option `option("failOnDataLoss", "false")` trong Kafka Reader có tác dụng gì?"*
- **Ứng viên:** *"Nếu ai đó xóa topic Kafka hoặc Kafka retention xóa mất offset mà Spark đang chờ, mặc định Spark sẽ quăng lỗi và dừng job. Set `false` giúp Spark bỏ qua offset bị mất và tiếp tục đọc từ offset nhỏ nhất hiện có."*

### Kịch bản 24: Driver Memory vs Executor Memory
- **Interviewer:** *"Nếu bị lỗi `OutOfMemoryError: Java heap space` trên Driver, nguyên nhân là gì?"*
- **Ứng viên:** *"Thường do gọi hàm `.collect()` lấy dữ liệu quá lớn từ tất cả Executor về Driver node. Giải pháp là dùng `.take(n)` hoặc ghi trực tiếp ra HDFS sink chứ không đưa data lớn về Driver."*

---

## 📌 PHẦN 3: DATA WAREHOUSE STAR SCHEMA, SCD TYPE 2 & SPARK BATCH (Câu 25 - 36)

### Kịch bản 25: Star Schema vs Snowflake Schema
- **Interviewer:** *"Tại sao em chọn Star Schema cho Data Warehouse thay vì Snowflake Schema?"*
- **Ứng viên:** *"Star Schema denormalize các bảng chiều (Dimension). Trong Spark SQL OLAP, join giữa 1 bảng Fact lớn và 1 bảng Dim phẳng (như `dim_customer`) diễn ra cực nhanh vì giảm thiểu số lượng phép JOIN. Snowflake Schema tuy chuẩn hóa 3NF hơn nhưng tạo ra nhiều join path phân nhánh, làm lãng phí chi phí Shuffle Exchange dữ liệu trong cụm Spark."*

### Kịch bản 26: Thiết kế SCD Type 2 Customer Dimension
- **Interviewer:** *"Mô tả cấu trúc bảng `dim_customer` SCD Type 2 của em?"*
- **Ứng viên:** *"Bảng gồm các cột:*
  - `customer_key`: Surrogate Key dạng MD5 hash (`MD5(customer_id + effective_date + status)`).
  - `customer_id`: Business Key từ Odoo.
  - `status`: Trạng thái (`mới`, `cũ`, `thường niên`).
  - `effective_date`: Ngày bắt đầu hiệu lực.
  - `expiration_date`: Ngày hết hiệu lực (mặc định '9999-12-31' cho bản ghi hiện tại).
  - `is_current`: Boolean (True nếu là bản ghi hiện tại, False nếu là lịch sử đã đóng).*

### Kịch bản 27: Xử lý Merge SCD Type 2 trong PySpark
- **Interviewer:** *"Em thực hiện cập nhật SCD Type 2 trong PySpark như thế nào khi Parquet trên HDFS không hỗ trợ UPDATE từng dòng?"*
- **Ứng viên:** *"Em đọc bảng `dim_customer` cũ từ HDFS Parquet và dữ liệu CDC mới từ Postgres. Thực hiện `FULL OUTER JOIN` trên `customer_id`:*
  1. *Những record cũ bị đổi status*: Cập nhật `expiration_date = today - 1` và `is_current = False`.
  2. *Tạo dòng mới*: Chèn dòng mới với status mới, `effective_date = today`, `expiration_date = '9999-12-31'`, `is_current = True`.
  3. *Union bản ghi cũ + bản ghi mới* rồi ghi đè (overwrite) lại file Parquet trên HDFS."*

### Kịch bản 28: Tính toán Năm tài khóa (Fiscal Year)
- **Interviewer:** *"Năm tài khóa (Fiscal Year) của dự án được xử lý thế nào?"*
- **Ứng viên:** *"Năm tài khóa của doanh nghiệp bắt đầu từ **01/10 năm nay đến 30/09 năm sau**. Em xây dựng bảng `dim_date` chứa thuộc tính `fiscal_year` và `fiscal_quarter`. Khi tính toán báo cáo năm tài khóa, PySpark SQL chỉ việc `WHERE fiscal_year = 2026` thay vì filter ngày tháng thủ công, đảm bảo chính xác 100% cho kiểm toán."*

### Kịch bản 29: Data Skew trong Spark Join
- **Interviewer:** *"Nếu 1 khách hàng đại lý chiếm 80% tổng số đơn sửa chữa, khi Join bảng Fact với `dim_customer` bị đơ ở 99% progress (Data Skew), em xử lý thế nào?"*
- **Ứng viên:** *"Áp dụng kỹ thuật **Salting** (Thêm muối):*
  1. *Thêm một cột ngẫu nhiên `salt = floor(rand() * 4)` vào bảng Fact.*
  2. *Nhân bản bảng `dim_customer` thành 4 bản copy với các giá trị `salt` từ 0-3.*
  3. *Join trên cả `customer_id` và `salt`. Dữ liệu của khách hàng lớn đó sẽ được phân tán đều ra 4 Executors khác nhau, giải quyết dứt điểm rẽ nhánh Data Skew."*

### Kịch bản 30: Broadcast Join
- **Interviewer:** *"Khi nào nên dùng Broadcast Join trong Spark?"*
- **Ứng viên:** *"Khi Join 1 bảng Fact lớn với 1 bảng Dimension nhỏ (< 10MB - 100MB, ví dụ `dim_component`). Spark sẽ copy toàn bộ bảng nhỏ gửi tới RAM của từng Executor (`broadcast(dim_df)`). Bỏ qua hoàn toàn bước Shuffle dữ liệu qua mạng, giúp tốc độ Join tăng 5-10 lần."*

### Kịch bản 31: Factless Fact Table
- **Interviewer:** *"Bảng Fact trong DWH của em có loại Factless Fact Table nào không?"*
- **Ứng viên:** *"Có. Bảng theo dõi lịch hẹn sửa chữa nhưng lái xe không đến (No-Show Events). Bảng này không chứa số tiền/chi phí (metric dạng số) mà chỉ chứa các Foreign Keys (`customer_key`, `head_key`, `date_key`) để đếm tần suất hủy lịch hẹn."*

### Kịch bản 32: Surrogate Key vs Business Key
- **Interviewer:** *"Tại sao trong Data Warehouse lại dùng Surrogate Key (MD5 Hash) mà không dùng trực tiếp Primary Key ID từ Odoo?"*
- **Ứng viên:** *"Vì Business Key ID trong Odoo bị thay đổi hoặc trùng lặp khi tích hợp nhiều chi nhánh. Hơn nữa, với SCD Type 2, 1 `customer_id` có thể xuất hiện 3 dòng lịch sử trong DWH. Nếu dùng ID làm PK sẽ bị trùng. Surrogate Key MD5 duy nhất cho từng phiên bản trạng thái."*

### Kịch bản 33: Degenerated Dimension
- **Interviewer:** *"Degenerated Dimension trong schema của em là gì?"*
- **Ứng viên:** *"`invoice_number` (mã hóa đơn HD-2024-000001) hoặc `truck_plate` (biển số xe). Nó nằm trực tiếp trong bảng Fact `fact_parts_sales` để nhóm/filter mà không cần tạo riêng một bảng chiều."*

### Kịch bản 34: Spark Partition Coalesce vs Repartition
- **Interviewer:** *"Khác nhau giữa `.coalesce(n)` và `.repartition(n)`?"*
- **Ứng viên:** *"`coalesce` giảm số partition bằng cách gộp các partition trên cùng node, **không gây Shuffle** (rất nhanh khi giảm file ghi). `repartition` có thể tăng hoặc giảm partition và **luôn luôn gây Shuffle** dữ liệu qua mạng để chia đều data."*

### Kịch bản 35: Optimistic Concurrency Control trên Parquet
- **Interviewer:** *"Nếu 2 Spark batch jobs cùng lúc ghi đè vào 1 bảng Parquet DWH trên HDFS thì sao?"*
- **Ứng viên:** *"Parquet HDFS không có cơ chế ACID native như Delta Lake. Nếu 2 jobs ghi cùng lúc sẽ gây đè/hỏng file. Vì vậy em dùng **Airflow Orchestration** để quản lý dependency, đảm bảo các Spark jobs chạy nối tiếp hoặc ghi vào các partition HDFS khác nhau."*

### Kịch bản 36: Handling NULL values trong Aggregation
- **Interviewer:** *"Trong Spark SQL, `SUM(parts_amount)` xử lý dòng có giá trị `NULL` thế nào?"*
- **Ứng viên:** *"Spark SQL tự động bỏ qua (ignore) các dòng NULL khi dùng hàm gom nhóm `SUM`, `AVG`. Nếu tất cả các dòng đều NULL, hàm trả về `NULL`. Em luôn bao bọc bằng `coalesce(SUM(parts_amount), 0.0)` để tránh bị trả về Null pointer cho downstream."*

---

## 📌 PHẦN 4: AIRFLOW ORCHESTRATION & WORKFLOW (Câu 37 - 43)

### Kịch bản 37: Airflow LocalExecutor vs CeleryExecutor
- **Interviewer:** *"Tại sao em chọn Airflow LocalExecutor?"*
- **Ứng viên:** *"LocalExecutor chạy các task dưới dạng multiprocess song song trên 1 máy server. Cấu hình đơn giản, nhẹ, không cần dựng thêm RabbitMQ/Redis Broker của Celery. Phù hợp cho cụm master 8-16 cores chuyên làm nhiệm vụ điều phối và submit Spark jobs."*

### Kịch bản 38: Airflow Backfill & Catchup
- **Interviewer:** *"Option `catchup=False` trong DAG của em có tác dụng gì?"*
- **Ứng viên:** *"Nếu DAG bị pause trong 5 ngày, khi unpause với `catchup=True`, Airflow sẽ lập tức trigger 5 lần chạy liên tiếp cho 5 ngày đã qua. Đặt `catchup=False` giúp Airflow bỏ qua các khoảng thời gian quá hạn và chỉ chạy duy nhất 1 lần cho lịch trình hiện tại."*

### Kịch bản 39: Xử lý DAG Failure & Notification
- **Interviewer:** *"Nếu Spark batch job bị lỗi giữa chừng trong Airflow DAG, em nhận cảnh báo thế nào?"*
- **Ứng viên:** *"Em cấu hình `on_failure_callback` trong `default_args` của DAG. Khi bất kỳ task nào FAILED, Airflow tự động gọi hàm Python gửi một alert webhook (chứa DAG ID, Task ID, Log URL) sang channel Telegram/Slack của team DE."*

### Kịch bản 40: Task Dependency Patterns
- **Interviewer:** *"Làm sao đảm bảo Task tổng hợp báo cáo tuần chỉ chạy sau khi Task SCD2 hoàn thành?"*
- **Ứng viên:** *"Dùng toán tử bitshift trong file DAG Python: `t1_scd2_update >> t2_weekly_aggregation`. Airflow sẽ xây dựng cây Hướng không chu kỳ (DAG), task t2 sẽ ở trạng thái `upstream_failed` nếu task t1 thất bại."*

### Kịch bản 41: Airflow XComs Usage & Limits
- **Interviewer:** *"XCom trong Airflow dùng để làm gì? Có nên dùng XCom truyền DataFrame không?"*
- **Ứng viên:** *"XCom dùng để trao đổi các metadata nhỏ (như số lượng dòng đã xử lý, đường dẫn file HDFS) giữa các tasks. **Tuyệt đối không dùng XCom truyền DataFrame hay data lớn** vì XCom lưu trực tiếp vào Postgres Metadata DB của Airflow, làm phình DB và crash scheduler."*

### Kịch bản 42: Dynamic DAG Creation
- **Interviewer:** *"Nếu có 50 trạm sửa chữa cần 50 báo cáo riêng biệt, em tạo 50 file DAG hay làm thế nào?"*
- **Ứng viên:** *"Em dùng **Dynamic DAG Generation** bằng Python script. Script đọc danh sách 50 `head_id` và vòng lặp `for` để tự động tạo ra 50 DAGs trong bộ nhớ Airflow với cấu trúc mã nguồn duy nhất."*

### Kịch bản 43: Airflow Sensor Timeout
- **Interviewer:** *"ExternalTaskSensor hoặc FileSensor bị treo mãi thì xử lý sao?"*
- **Ứng viên:** *"Cần set `timeout=3600` (1 tiếng) và `mode='reschedule'`. Mode `reschedule` giúp Sensor nhường worker slot cho các task khác trong lúc chờ đợi, thay vì chiếm dụng slot (mode `poke`)."*

---

## 📌 PHẦN 5: PUSH-BASED SERVING LAYER & SYSTEM ARCHITECTURE (Câu 44 - 50)

### Kịch bản 44: Push-based WebSocket vs Pull-based Polling
- **Interviewer:** *"Tại sao em chọn WebSocket Push thay vì REST API Polling cho Dashboard?"*
- **Ứng viên:** *"Nếu 30 sếp cùng mở Dashboard Auto-refresh 5s/lần: hệ thống gánh **360 requests/phút** lặp đi lặp lại dù dữ liệu chưa đổi. Mô hình Push: Dashboard mở lên kết nối WebSocket và đứng im (0% request rác). Khi Spark/Airflow chạy xong batch job đêm, nó quăng 1 event vào Redis Pub/Sub, Backend đẩy 1 gói JSON duy nhất qua WebSocket. Tiết kiệm 99.9% request và độ trễ nhận báo cáo là **< 15ms**."*

### Kịch bản 45: Initial State Sync trên WebSocket
- **Interviewer:** *"Nếu sếp mở Dashboard lúc 10h sáng mà job chạy từ 2h đêm, WebSocket Push chưa có event mới thì Dashboard hiển thị gì?"*
- **Ứng viên:** *"Em thiết kế hàm `fetch_current_state()`. Ngay khi WebSocket Client kết nối thành công, Backend chủ động đọc dữ liệu báo cáo mới nhất đang lưu trong Redis String Key `report:agg:monthly` và gửi sự kiện `INITIAL_STATE` xuống client ngay lập tức. Dashboard hiển thị ngay số liệu mà không phải chờ event tiếp theo."*

### Kịch bản 46: Redis Pub/Sub At-Most-Once Nature
- **Interviewer:** *"Redis Pub/Sub không lưu trữ tin nhắn (no persistence). Nếu WebSocket Backend bị sập 1 phút đúng lúc Pub/Sub phát event thì sao?"*
- **Ứng viên:** *"Redis Pub/Sub là At-Most-Once. Khi Backend sập và start lại, nó tự động gọi hàm Reconnect và fetch lại `INITIAL_STATE` từ Redis String Key (nơi Spark job đã `SET` dữ liệu bền vững). Không bao giờ bị mất số liệu hiển thị."*

### Kịch bản 47: WebSocket Scaling (Sticky Session / Redis Adapter)
- **Interviewer:** *"Nếu có 10.000 trình duyệt cùng kết nối WebSocket, 1 server Python không gánh nổi thì scale thế nào?"*
- **Ứng viên:** *"Scale ngang nhiều WebSocket Server nodes sau Nginx Load Balancer (bật Sticky Sessions / Upgrade Header). Tất cả các WebSocket nodes này đều Subscribe chung vào 1 kênh Redis Pub/Sub `channel:report-updates`. Khi có tin nhắn mới từ Redis, tất cả các WebSocket nodes sẽ cùng broadcast xuống các client đang nối với node đó."*

### Kịch bản 48: Separation of Concerns (Spark vs Serving)
- **Interviewer:** *"Tại sao không cho Spark job gửi thẳng HTTP request tới Dashboard UI luôn?"*
- **Ứng viên:** *"Để đạt **Decoupling (Phân tách trách nhiệm)**. Spark là Engine tính toán dữ liệu lớn, không nên biết IP hay quản lý kết nối kết nối mạng của người dùng. Spark chỉ việc phát tín hiệu nhẹ vào Redis Pub/Sub. Lớp WebSocket Async Python chuyên trách quản lý kết nối I/O người dùng."*

### Kịch bản 49: End-to-End Latency Metrics
- **Interviewer:** *"Độ trễ tổng thể (End-to-End Latency) của toàn bộ luồng từ lúc Odoo phát sinh giao dịch đến khi CDC lên Kafka là bao lâu?"*
- **Ứng viên:** *"Khoảng **200ms đến 800ms**. Trong đó: Postgres ghi WAL (~10ms), Debezium đọc WAL & gửi Kafka (~150ms), Redis CDC Worker sync Redis (~50ms)."*

### Kịch bản 50: System Bottleneck Identification
- **Interviewer:** *"Nếu toàn bộ hệ thống bị chậm, em làm thế nào để tìm ra điểm nghẽn (Bottleneck)?"*
- **Ứng viên:** *"Em kiểm tra theo chiều luồng dữ liệu:*
  1. *Postgres*: Kiểm tra `pg_stat_activity` và `pg_replication_slots` xem WAL lag bao nhiêu MB.
  2. *Kafka*: Kiểm tra Consumer Group Lag bằng `kafka-consumer-groups.sh`.
  3. *Spark*: Xem Spark UI (`http://master:4040`), kiểm tra Event Timeline xem Stage nào bị Data Skew hoặc Shuffle Read chậm.
  4. *Redis/WebSocket*: Kiểm tra CPU usage và memory usage qua `redis-cli info`."*
