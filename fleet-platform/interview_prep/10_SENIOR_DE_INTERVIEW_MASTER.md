# 🏆 SENIOR DATA PLATFORM ENGINEER — INTERVIEW MASTER GUIDE
## Bộ Câu Hỏi & Câu Trả Lời Chuẩn Tối Đa Cho Vị Trí Senior / Technical Lead DE

---

## 📌 MODULE 1: APACHE SPARK INTERNALS, MEMORY & STREAMING

### Câu 1: Catalyst Optimizer của Spark vận hành qua các giai đoạn nào? Thế nào là Predicate Pushdown và Whole-Stage Code Generation?
- **Trả lời chuẩn Senior:**
  Catalyst Optimizer của Spark trải qua 4 giai đoạn chính:
  1. **Analysis**: Phân tích cú pháp Abstract Syntax Tree (AST) từ câu lệnh SQL/DataFrame và đối chiếu tên cột, tên bảng với Catalog (Unresolved Logical Plan $\rightarrow$ Resolved Logical Plan).
  2. **Logical Optimization**: Áp dụng các quy tắc tối ưu hóa chuẩn (Rule-Based Optimization) như **Predicate Pushdown** (đẩy điều kiện `WHERE` xuống tận nguồn nạp đĩa Parquet/DB để giảm I/O) và **Projection Pruning** (chỉ chọn các cột cần dùng, bỏ các cột thừa).
  3. **Physical Planning**: Chuyển Logical Plan thành 1 hoặc nhiều Physical Plans, tính toán chi phí (Cost-Based Optimizer - CBO) để chọn Physical Plan tối ưu nhất (ví dụ chọn giữa Broadcast Hash Join và Sort-Merge Join).
  4. **Code Generation (Janino)**: Thực thi **Whole-Stage Code Generation**, tổng hợp nhiều phép toán (Filter, Map, Project) trong cùng 1 Stage thành 1 khối mã Java Bytecode duy nhất, loại bỏ Virtual Function Calls và tận dụng bộ đệm CPU L1/L2/L3 tối đa.

---

### Câu 2: Engine Tungsten quản lý bộ nhớ Spark như thế nào để vượt qua giới hạn của Java Virtual Machine (JVM)?
- **Trả lời chuẩn Senior:**
  Tungsten giải quyết 2 nhược điểm lớn nhất của JVM (Overhead của Java Object Header và hiện tượng đơ hệ thống do Garbage Collection - GC Pauses):
  1. **Off-Heap Memory Management (UnsafeRow)**: Tungsten lưu trữ dữ liệu dưới dạng mảng byte thô (binary byte array) trực tiếp trên bộ nhớ Off-Heap (bỏ qua JVM Heap). Mỗi dòng dữ liệu được mã hóa ở dạng 64-bit word offset, loại bỏ hoàn toàn các Java Object wrapper (tiết kiệm 80% RAM).
  2. **Cache-Conscious Algorithms**: Sắp xếp và truy vấn dữ liệu trực tiếp trên bộ đệm L1/L2/L3 của CPU bằng cách đặt các con trỏ (Pointers) và Key 8-byte cạnh nhau trong bộ nhớ, tránh hiện tượng CPU Cache Miss.

---

### Câu 3: Cơ chế Watermarking trong PySpark Structured Streaming vận hành thế nào dưới tầng Engine? Sự khác biệt giữa `outputMode("append")` và `outputMode("update")` khi có Watermark?
- **Trả lời chuẩn Senior:**
  Watermarking trong Spark Structured Streaming được dùng để quản lý **State Store** (tránh tràn bộ nhớ RAM khi gom nhóm thời gian):
  - **Cơ chế**: Watermark thiết lập một khoảng trễ tối đa cho phép so với Event Time lớn nhất từng ghi nhận ($Watermark = Max(EventTime) - DelayThreshold$). Khi micro-batch mới tiến tới, Engine sẽ cập nhật giá trị Watermark này. Tất cả các record có Event Time nhỏ hơn Watermark sẽ bị **drop lập tức** mà không đưa vào State Store. Đồng thời, các state của các window nằm hoàn toàn trước Watermark sẽ bị **xóa khỏi bộ nhớ RAM/RocksDB**.
  - **Khác biệt OutputMode**:
    - `outputMode("update")`: Spark sẽ phát ra ngay kết quả của window bị thay đổi trong micro-batch hiện tại (dù window đó chưa đóng). Tương thích tốt khi muốn đẩy báo cáo thời gian thực xuống Redis.
    - `outputMode("append")`: Spark **chỉ phát ra kết quả duy nhất 1 lần khi window đó đã chính thức đóng** (tức là khi $Watermark > WindowEnd$). Mode này bắt buộc khi ghi dữ liệu ra HDFS Parquet Sink để tránh bị ghi lặp file.

---

### Câu 4: Phân tích kỹ thuật Salting để giải quyết Data Skew trong Spark Join? Khi nào dùng Broadcast Hash Join (BHJ) vs Sort-Merge Join (SMJ)?
- **Trả lời chuẩn Senior:**
  - **Broadcast Hash Join (BHJ)**: Kích hoạt khi 1 trong 2 bảng nhỏ hơn ngưỡng `spark.sql.autoBroadcastJoinThreshold` (mặc định 10MB, có thể nâng lên 100MB). Driver copy toàn bộ bảng nhỏ gửi tới RAM của từng Executor. Bỏ qua hoàn toàn bước Shuffle dữ liệu qua mạng, tốc độ nhanh nhất.
  - **Sort-Merge Join (SMJ)**: Thuật toán mặc định khi 2 bảng đều lớn. Spark Shuffle dữ liệu theo Join Key để dữ liệu cùng Key về cùng Executor, sau đó sắp xếp (Sort) theo Key rồi Merge. Nếu 1 Key bị Skew (chiếm 90% dữ liệu), 1 Executor sẽ bị treo ở 99% progress.
  - **Kỹ thuật Salting**:
    1. Thêm một cột ngẫu nhiên $salt = floor(rand() \times N)$ vào bảng Fact bị skew.
    2. Nhân bản bảng Dim thành $N$ bản copy tương ứng với các giá trị $salt \in [0, N-1]$ (dùng `array` và `explode`).
    3. Thực hiện Join trên cả `JoinKey` VÀ `salt`. Dữ liệu của Key bị Skew sẽ được phân tán đều ra $N$ Executors khác nhau, triệt tiêu dứt điểm Data Skew.

---

### Câu 5: Vấn đề Small Files trên HDFS do Spark Streaming gây ra là gì? Cách xử lý bằng Compaction Job và `coalesce` vs `repartition`?
- **Trả lời chuẩn Senior:**
  - **Nguyên nhân**: Spark Streaming chạy trigger 10 giây/lần sẽ tạo ra hàng nghìn file Parquet kích thước vài KB. Mỗi file lưu trên HDFS ngốn ~150 bytes RAM trên NameNode Metadata. Hàng triệu file nhỏ sẽ làm **tràn RAM NameNode** và khiến các batch job đọc chậm do overhead mở/đóng file descriptor.
  - **Giải pháp Compaction**: Dùng Airflow trigger job định kỳ hàng đêm đọc các file Parquet nhỏ trong ngày, thực hiện repartition rồi ghi đè lại.
  - **Phân biệt `coalesce(n)` và `repartition(n)`**:
    - `coalesce(n)`: Giảm số partition bằng cách gộp các partition trên cùng node. **Không gây Shuffle dữ liệu qua mạng** (cực nhanh khi ghi ra đĩa).
    - `repartition(n)`: Tăng hoặc giảm partition và **luôn luôn gây Full Shuffle** dữ liệu qua mạng để chia đều kích thước đĩa.

---

## 📌 MODULE 2: POSTGRESQL CDC & KAFKA MULTI-BROKER ARCHITECTURE

### Câu 6: Tại sao CDC Log-Based cần `REPLICA IDENTITY FULL` trên Postgres? Nếu đặt `DEFAULT` thì điều gì xảy ra với Downstream SCD Type 2?
- **Trả lời chuẩn Senior:**
  - Khi Postgres chạy ở chế độ CDC (`wal_level=logical`), Debezium giải mã các file WAL thành các sự kiện JSON có 2 cấu trúc: `before` (ảnh trước khi đổi) và `after` (ảnh sau khi đổi).
  - Nếu để `REPLICA IDENTITY DEFAULT`, Postgres chỉ ghi Primary Key vào `before` image đối với lệnh `UPDATE`.
  - **Tác hại lên SCD Type 2**: Khi khách hàng đổi trạng thái từ `'mới'` sang `'thường niên'`, CDC Event phát ra có `before: {id: 1}` và `after: {id: 1, status: 'thường niên'}`. Spark Batch job không thể biết `status` cũ của khách hàng là gì để xác định xem trạng thái có bị biến đổi thực sự hay không, trừ khi phải query ngược vào OLTP Database (vi phạm nguyên tắc Zero OLTP Load).
  - Khi bật `REPLICA IDENTITY FULL`, Postgres bắt buộc ghi **toàn bộ giá trị tất cả các cột cũ** vào `before` image, giúp Spark xử lý SCD Type 2 hoàn toàn tự động và độc lập.

---

### Câu 7: Trong Debezium Distributed Connect, hiện tượng WAL Accumulation (Lag Slot) là gì? Cách cấu hình `max_slot_wal_keep_size` và nhịp tim `heartbeat.interval.ms` để phòng thủ tràn đĩa Postgres?
- **Trả lời chuẩn Senior:**
  - **Vấn đề**: Postgres duy trì 1 Logical Replication Slot cho Debezium. Slot này bảo vệ các file WAL chưa được Debezium xác nhận ACK. Nếu Debezium Connector bị ngắt kết nối (crash hoặc đứt mạng) trong vài ngày, Postgres sẽ **giữ lại toàn bộ file WAL trên đĩa**, dẫn đến nguy cơ đầy đĩa (Disk Full) và làm crash toàn bộ DB OLTP của doanh nghiệp.
  - **Phòng thủ 1 (`max_slot_wal_keep_size`)**: Khai báo trong `postgresql.conf` (ví dụ `10GB`). Nếu Debezium lag vượt quá 10GB WAL, Postgres sẽ tự động invalidated slot đó để giải phóng ổ đĩa, ưu tiên bảo vệ sự sống còn của OLTP DB.
  - **Phòng thủ 2 (`heartbeat.interval.ms`)**: Nếu một bảng ít có giao dịch, Debezium không phát tin nhắn mới, làm LSN offset bị đứng yên. Cấu hình `heartbeat.interval.ms = 10000` ép Debezium gửi tin nhắn nhịp tim định kỳ vào Kafka Heartbeat Topic, giúp Postgres giải phóng các đoạn WAL cũ liên tục.

---

### Câu 8: Phân tích mô hình chịu lỗi của Kafka Multi-Broker với $RF=3$ và $min.insync.replicas=2$? Khi nào Producer nhận lỗi `NotEnoughReplicasException`?
- **Trả lời chuẩn Senior:**
  - **Cấu hình**: $ReplicationFactor = 3$ (mỗi partition có 1 Leader và 2 Followers nằm trên 3 máy physical/VM khác nhau). Producer gửi tin nhắn với `acks=all` (hoặc `acks=-1`).
  - **Cơ chế**: `acks=all` yêu cầu tin nhắn phải được ghi thành công vào Leader VÀ tất cả các Replicas nằm trong tập hợp ISR (In-Sync Replicas). Giá trị `min.insync.replicas = 2` quy định số lượng ISR tối thiểu phải xác nhận.
  - **Tính toán khả năng chịu lỗi**:
    $$\text{Max Brokers Tolerated Down} = RF - min.insync.replicas = 3 - 2 = 1 \text{ broker}$$
  - **Lỗi `NotEnoughReplicasException`**: Nếu 2 Brokers sập cùng lúc, số lượng ISR khả dụng giảm xuống $1 < min.insync.replicas (2)$. Kafka sẽ từ chối nhận tin nhắn mới từ Producer và quăng lỗi `NotEnoughReplicasException` để ngăn chặn việc ghi dữ liệu khi hệ thống mất khả năng sao lưu chịu lỗi.

---

## 📌 MODULE 3: PRODUCTION OPERATIONS 24/7 (IDEMPOTENCY, REPLAY & BACKFILL)

### Câu 9: Tính Khả Trùng (Idempotency) là gì trong hệ thống Data Pipeline? Cách thiết kế Idempotent Sink trên Redis và HDFS Parquet khi Kafka truyền tin nhắn At-Least-Once?
- **Trả lời chuẩn Senior:**
  - **Bản chất**: Vì Debezium và Kafka giao tin nhắn theo cơ chế **At-Least-Once** (có thể bị lặp lại tin nhắn khi network retry hoặc consumer crash trước commit offset), **Idempotency** đảm bảo rằng cho dù một tin nhắn được xử lý 1 lần hay 100 lần, trạng thái cuối cùng của hệ thống lưu trữ (Sink) vẫn hoàn toàn giống nhau.
  - **Thiết kế Idempotent Sink**:
    1. **Trên Redis**: Dùng lệnh `HSET head:info:{id} field value` hoặc `SET report:agg:{month} json_payload`. Vì Key cố định theo ID hoặc ngày báo cáo, việc ghi lặp lại 100 lần chỉ đơn giản là thao tác Ghi đè (Overwrite), không làm lệch số liệu.
    2. **Trên HDFS Parquet**: Dùng cơ chế **Dynamic Partition Overwrite** (`spark.sql.sources.partitionOverwriteMode=dynamic`). Spark sẽ ghi đè toàn bộ thư mục partition (`year=2026/month=07/day=31/`) của batch đó một cách nguyên tử (Atomic commit), loại bỏ hoàn toàn rủi ro bị nhân đôi bản ghi (duplicate rows).

---

### Câu 10: Khi Consumer Group bị ngắt kết nối trong 3 tiếng và bị tích tụ Consumer Lag 5 triệu tin nhắn, quy trình xử lý Data Replay & Backfill của Senior DE diễn ra như thế nào?
- **Trả lời chuẩn Senior:**
  Quy trình 4 bước chuẩn production:
  1. **Dừng và Sửa lỗi Downstream**: Cách ly và khắc phục triệt để nguyên nhân crash của Consumer Service.
  2. **Kiểm tra Offset & Time Window**: Sử dụng công cụ `kafka-consumer-groups.sh` kiểm tra LOG-END-OFFSET và CURRENT-OFFSET của topic.
  3. **Reset Offset về thời điểm sự cố (Data Replay)**:
     ```bash
     kafka-consumer-groups.sh --bootstrap-server master:9092 --group fleet-serving-group \
       --reset-offsets --to-datetime 2026-08-03T07:00:00.000 --execute --topic fleet-cdc-events
     ```
  4. **Tăng tốc độ Tiêu thụ (Catch-up Tuning)**:
     - Tăng tạm thời `max.poll.records` từ 500 lên 2000.
     - Tăng số lượng Partition của Kafka Topic và scale ngang số lượng Consumer Instances trong Group (đảm bảo $1 \text{ Consumer} = 1 \text{ Partition}$) để tiêu thụ song song 5 triệu tin nhắn tích tụ trong vài phút.

---

### Câu 11: Làm thế nào Spark Structured Streaming đảm bảo Exactly-Once Sink ghi xuống HDFS qua `FileStreamSinkLog`?
- **Trả lời chuẩn Senior:**
  Spark Structured Streaming kết hợp 2 thành phần:
  1. **Replayable Source (Kafka Offset Checkpoint)**: Lưu trữ chính xác Kafka topic, partition và offset của micro-batch hiện tại vào thư mục `checkpoint/offsets/v1/X`.
  2. **Metadata Transaction Log (`FileStreamSinkLog`)**: Khi ghi file Parquet ra HDFS, Spark tạo các file có tên ngẫu nhiên (dạng `part-00000-xxx.c000.snappy.parquet`). Khi toàn bộ Executor ghi file tạm xong, Driver mới ghi một file log commit nguyên tử vào thư mục `_spark_metadata/X`.
  - **Khôi phục khi crash**: Nếu Driver bị sập giữa chừng, các file Parquet tạm đã ghi ra đĩa nhưng chưa được liệt kê trong `_spark_metadata` sẽ bị Spark Reader **bỏ qua hoàn toàn** khi đọc. Khi job restart, Spark đọc checkpoint offset cũ và ghi lại micro-batch đó từ đầu, đảm bảo tính nguyên tử Exactly-Once.

---

## 📌 MODULE 4: DATA WAREHOUSING, SCD TYPE 2 & LAKEHOUSE ARCHITECTURE

### Câu 12: Phân tích 5 bước của Thuật toán Merge SCD Type 2 trong PySpark? Tại sao lại dùng Surrogate Key MD5 thay vì Business Key từ OLTP?
- **Trả lời chuẩn Senior:**
  - **Lý do dùng Surrogate Key MD5**: Business Key từ OLTP (ví dụ `customer_id = 1`) bị lặp lại nhiều dòng trong bảng Dim SCD Type 2 khi khách hàng thay đổi trạng thái theo thời gian. Khóa chính của bảng Dim bắt buộc phải là một **Surrogate Key duy nhất cho từng phiên bản**. Dùng `MD5(concat_ws("_", customer_id, effective_date, status))` tạo ra một chuỗi hash 32 ký tự định danh duy nhất cho từng phiên bản trạng thái.
  - **Thuật toán 5 bước Merge SCD Type 2 trong PySpark**:
    1. *Tách bản ghi lịch sử cũ*: Lấy tất cả các dòng đã hết hạn trong quá khứ (`is_current == False`), giữ nguyên 100% không đụng tới.
    2. *Left Join Active Records với CDC*: Lấy các dòng đang Active (`is_current == True`) Left Join với dữ liệu CDC mới từ Postgres.
    3. *Đóng dòng Active bị thay đổi*: Những dòng active có status khác status mới trong CDC sẽ bị đổi `expiration_date = today` và `is_current = False`.
    4. *Giữ dòng Active không đổi*: Những dòng active không có CDC mới hoặc status không đổi được giữ nguyên.
    5. *Tạo dòng Active mới*: Tạo dòng active mới (`effective_date = today`, `expiration_date = '9999-12-31'`, `is_current = True`) cho khách đổi status và khách mới hoàn toàn (dùng `left_anti` join).
    - Sau đó `UNION` cả 5 tập dữ liệu và ghi đè lại Data Warehouse.

---

### Câu 13: Mô hình Năm Tài Khóa (Fiscal Year: 01/10 - 30/09) được thiết kế và tính toán như thế nào trong Data Warehouse?
- **Trả lời chuẩn Senior:**
  - **Thiết kế**: Trong Doanh nghiệp, Năm tài khóa không trùng với Năm dương lịch. Năm tài khóa 2027 bắt đầu từ **01/10/2026 đến 30/09/2027**.
  - **Giải pháp**: Xây dựng bảng chiều thời gian `dim_date` chứa sẵn các cột pre-computed:
    - `date_key` (dạng INTEGER `20261001`)
    - `full_date` (DATE `2026-10-01`)
    - `calendar_year` (2026), `calendar_quarter` (4)
    - `fiscal_year` (2027), `fiscal_quarter` ("Q1-FY2027")
  - **Tính toán**: Khi Airflow chạy job tổng hợp báo cáo năm tài khóa, PySpark SQL chỉ việc `JOIN dim_date ON fact.date_key = dim_date.date_key WHERE dim_date.fiscal_year = 2027`, triệt tiêu hoàn toàn việc tính toán `CASE WHEN` ngày tháng phức tạp trong runtime, đảm bảo chính xác 100% cho kiểm toán.

---

## 📌 MODULE 5: AIRFLOW ORCHESTRATION & KUBERNETES DEPLOYMENT

### Câu 14: So sánh Airflow LocalExecutor vs CeleryExecutor vs KubernetesExecutor? Khi nào nên chọn KubernetesExecutor?
- **Trả lời chuẩn Senior:**
  - **LocalExecutor**: Chạy các task dưới dạng Multiprocessing trên 1 máy Airflow Server. Thích hợp cho cụm Master dedicated 8-16 cores chuyên làm nhiệm vụ điều phối và submit Spark jobs.
  - **CeleryExecutor**: Phân tán task ra nhiều Worker machines cố định thông qua Celery Broker (Redis/RabbitMQ). Khuyết điểm: Phải duy trì các Worker machines chạy liên tục 24/7 gây tốn chi phí dù không có task.
  - **KubernetesExecutor**: Mỗi task khi kích hoạt sẽ khởi tạo **1 Kubernetes Pod độc lập** trên cụm K8s. Pod này chứa đúng môi trường Docker image cần thiết cho task đó, chạy xong task sẽ tự động bị xóa (Scale down về 0).
  - **Khi nào chọn K8sExecutor**: Khi các task có tài nguyên và dependency khác nhau (Task A cần GPU, Task B cần Python 3.11), và cần tối ưu chi phí hạ tầng Cloud (chỉ trả tiền đúng thời gian Pod chạy).

---

### Câu 15: Tại sao nên đặt `mode='reschedule'` thay vì `mode='poke'` trong Airflow ExternalTaskSensor?
- **Trả lời chuẩn Senior:**
  - **`mode='poke'` (Mặc định)**: Sensor sẽ giữ chặt Airflow Worker Slot và liên tục ngủ (sleep) rồi kiểm tra điều kiện. Nếu sensor chờ 2 tiếng, 1 Worker Slot sẽ bị đóng đóng băng suốt 2 tiếng, khiến các DAG khác thiếu worker slot để chạy.
  - **`mode='reschedule'`**: Khi chưa đủ điều kiện, Sensor sẽ **giải phóng hoàn toàn Worker Slot** lại cho Airflow Pool và tự giải tán. Sau khoảng thời gian `poke_interval` (ví dụ 60s), Scheduler mới cấp slot lại để Sensor kiểm tra 1 lần rồi giải phóng tiếp. Đây là best practice bắt buộc cho production.
