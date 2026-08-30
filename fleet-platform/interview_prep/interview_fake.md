Trong dự án Fleet Platform, bạn nói rằng bạn dùng Debezium Log-Based CDC để trích xuất dữ liệu từ Odoo PostgreSQL sang Kafka với '0% SQL query load' trên OLTP database.

Câu hỏi: Giải thích cho tôi cơ chế cụ thể tại sao Log-Based CDC không gây tải I/O lên database OLTP? Nếu tôi nói rằng 'Debezium vẫn phải đọc file WAL từ đĩa của Postgres, vậy nó vẫn gây tải I/O đĩa chứ?' — bạn phản biện thế nào?

Debezium đọc WAL file từ đĩa Postgres, nhưng WAL file là Sequential I/O (đọc tuần tự) chứ không phải Random I/O (đọc ngẫu nhiên) như SELECT query. Hệ điều hành cache WAL file trong Page Cache RAM, nên chi phí đĩa gần bằng 0.
❌ Sai rồi! Đây là bẫy kinh điển trong phỏng vấn Senior DE.

🔴 Đáp án đúng là Option 2:
Debezium không tự đọc file WAL từ đĩa. Cơ chế thực tế diễn ra như sau:

Postgres sử dụng Logical Replication Protocol (thông qua pgoutput plugin hoặc wal2json). Khi bạn tạo Logical Replication Slot, Postgres tự tạo một Walsender process bên trong engine.
Walsender process đọc WAL segments từ bộ nhớ (hoặc đĩa nếu cần), giải mã (decode) WAL binary thành các sự kiện logical (INSERT/UPDATE/DELETE với tên bảng, tên cột, giá trị) ngay bên trong Postgres engine.
Sau đó Postgres stream các sự kiện đã decode này qua TCP Replication Connection tới Debezium Connect Worker.
Debezium nhận sự kiện qua socket, đóng gói thành JSON Envelope và gửi lên Kafka.
💡 Tại sao câu trả lời của bạn sai?
Debezium không truy cập trực tiếp filesystem/đĩa của Postgres. Nó kết nối qua Streaming Replication Protocol (cùng protocol mà Physical Standby DB dùng, nhưng ở mode Logical).
Nói "0% query load" là đúng vì không có câu lệnh SELECT nào được thực thi trên bảng dữ liệu. Nhưng Postgres vẫn tốn CPU/RAM cho quá trình WAL decoding ở Walsender process (chi phí này rất nhỏ, khoảng 1-3% CPU trên production).
📝 Ghi nhớ cho phỏng vấn:
"Debezium kết nối vào Postgres qua Streaming Replication Protocol. Postgres tự khởi tạo một Walsender process, giải mã WAL binary thành các sự kiện logical bên trong engine, rồi stream qua TCP socket tới Debezium. Không có câu lệnh SELECT nào chạy trên bảng dữ liệu OLTP, nên không gây contention với các transaction của ứng dụng Odoo."


🎤 Câu 2 (Follow-up xóay sâu - CDC Fault Recovery):

Tốt, giờ tôi hiểu Debezium dùng Streaming Replication Protocol. Vậy khi Debezium Connect Worker bị crash và restart, nó làm thế nào để biết đọc tiếp WAL từ đúng vị trí bị ngắt mà không bị mất hoặc lặp dữ liệu?

Debezium đọc offset cuối cùng từ Kafka Internal Topic `connect-offsets` và gửi lại vị trí LSN này cho Postgres để tiếp tục nhận WAL events từ đúng nơi bị ngắt.
✅ Chính xác! Câu trả lời rất chuẩn.

💡 Bổ sung thêm chiều sâu cho phỏng vấn:
Cơ chế khôi phục của Debezium thực tế kết hợp 2 tầng:

Tầng Kafka Connect: Khi Debezium Worker restart, nó đọc LSN offset cuối cùng đã commit thành công từ Kafka Internal Topic connect-offsets (topic nội bộ của Kafka Connect Framework).
Tầng Postgres Replication Slot: Đồng thời, Postgres giữ confirmed_flush_lsn trên Replication Slot. Khi Debezium reconnect và gửi LSN từ connect-offsets, Postgres đối chiếu với confirmed_flush_lsn trên slot và stream tiếp từ vị trí đúng.
Bẫy phỏng vấn: Nếu interviewer hỏi "Vậy có thể bị lặp tin nhắn không?" → Trả lời: Có, vì Debezium gửi tin nhắn lên Kafka TRƯỚC khi commit offset. Nếu crash sau khi gửi nhưng trước khi commit, khi restart sẽ gửi lại → At-Least-Once delivery, downstream phải thiết kế Idempotent Sink.


🎤 Câu 3 (Spark Streaming - Exactly-Once Semantics):

Bạn nói rằng PySpark Structured Streaming của bạn ghi dữ liệu xuống HDFS Parquet với Exactly-Once Semantics.

Nhưng tôi biết HDFS không hỗ trợ ACID transaction như database. Vậy cơ chế cụ thể nào giúp Spark đảm bảo không bị ghi lặp (duplicate) hoặc mất dữ liệu khi Driver crash giữa chừng?

Spark Structured Streaming lưu Kafka offset vào thư mục checkpoint trên HDFS. Khi ghi Parquet, Spark dùng FileStreamSinkLog ghi metadata commit vào thư mục `_spark_metadata/`. File Parquet chưa được liệt kê trong metadata sẽ bị bỏ qua khi đọc. Khi restart, Spark replay lại đúng micro-batch bị lỗi.




Câu 4 (Spark Streaming - Watermark Deep Dive):

Giải thích cho tôi chính xác cơ chế vận hành của Watermark dưới tầng Spark Engine:

Watermark tiến dần như thế nào qua các micro-batch?
Nó quyết định xóa State Store trong bộ nhớ RAM như thế nào?
Tại sao nếu không dùng Watermark, Streaming Window Aggregation sẽ bị tràn bộ nhớ (OOM)?
Watermark tiến dần theo công thức: Watermark = Max(EventTime) - DelayThreshold. Spark Engine cập nhật giá trị này sau mỗi micro-batch. Tất cả record có EventTime < Watermark sẽ bị drop. Đồng thời, các Window đã đóng hoàn toàn (WindowEnd < Watermark) sẽ được xóa khỏi State Store để giải phóng RAM.



 Câu 5 (SCD Type 2 - Edge Case):

Trong quy trình Merge SCD Type 2 của bạn, bước 5 là 'Tạo dòng Active mới cho khách hàng mới hoàn toàn (chưa từng tồn tại trong Dim)'.

Câu hỏi: Bạn dùng kỹ thuật gì trong PySpark để phân biệt giữa khách hàng đổi status (cần đóng dòng cũ + mở dòng mới) và khách hàng mới hoàn toàn (chỉ cần mở dòng mới)?

Sử dụng `left_anti` join giữa CDC DataFrame và danh sách `customer_id` đã tồn tại trong bảng Dim. `left_anti` trả về chỉ những dòng trong CDC mà không match với bất kỳ dòng nào trong bảng Dim hiện tại.


Câu 6 (Push-Based Architecture - End-to-End Flow):

Mô tả cho tôi luồng dữ liệu chi tiết từ lúc Airflow kích hoạt Spark Batch job cho đến lúc Dashboard UI của sếp nhận được số liệu mới nhất.

Cụ thể: Ai là người PUBLISH event lên Redis Pub/Sub? Và ai là người nhận event đó để push xuống Dashboard:

🔴 Đáp án chuẩn xác nhất là:
Trong kiến trúc mà chúng ta đã xây dựng, luồng hoạt động như sau:

Airflow kích hoạt Spark Batch job (qua BashOperator hoặc SparkSubmitOperator).
Spark Batch job tính toán xong doanh thu → ghi kết quả vào Redis String Key (SET report:agg:monthly '{json}') → rồi chính Spark job gọi lệnh PUBLISH channel:report-updates '{json}' trực tiếp.
Async Python WebSocket Server đang Subscribe kênh channel:report-updates → nhận event → broadcast push xuống tất cả Dashboard clients.
💡 Tại sao Option 3 đúng hơn Option 1?
Separation of Concerns (Phân tách trách nhiệm): Airflow chỉ chịu trách nhiệm điều phối lịch trình (orchestration), không nên biết về WebSocket hay Redis Pub/Sub. Spark job là người sở hữu dữ liệu kết quả, nên Spark mới là người phát tín hiệu "dữ liệu mới đã sẵn sàng".
Nếu để Airflow PUBLISH, bạn tạo ra tight coupling giữa Orchestration Layer và Serving Layer. Khi thay đổi Redis channel name hoặc format JSON, bạn phải sửa cả Spark job lẫn Airflow DAG.



🎤 Câu 7 (Production Operations - Bottleneck Diagnosis):

Lúc 2h sáng, hệ thống báo cáo của bạn bị chậm, Dashboard không cập nhật số liệu mới. Bạn không biết điểm nghẽ (bottleneck) nằm ở đâu trong pipeline CDC → Kafka → Spark → HDFS → Redis → WebSocket.

Mô tả quy trình chẩn đoán hệ thống từng tầng (Layer-by-Layer Diagnosis)?

Bước 1: Kiểm tra `pg_replication_slots` xem `active = false` và `wal_status`. Bước 2: Kiểm tra Consumer Group Lag trên Kafka bằng `kafka-consumer-groups.sh --describe`. Bước 3: Kiểm tra Spark UI (port 4040) xem Stage nào bị Data Skew hoặc Shuffle Read chậm. Bước 4: Kiểm tra Redis `INFO` xem memory usage và connected clients.
✅ Chính xác tuyệt đối! Đây là quy trình chẩn đoán chuẩn mực của Senior DE — kiểm tra theo chiều luồng dữ liệu từ Source đến Sink, không đoán mò.

💡 Bổ sung câu trả lời thêm để ghi điểm tuyệt đối:
Khi trả lời trong phỏng vấn, bạn nên nói thêm lệnh cụ thể cho từng bước:

Tầng	Lệnh Chẩn đoán	Dấu hiệu Lỗi
Postgres	SELECT slot_name, active, wal_status, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes FROM pg_replication_slots;	active = false hoặc lag_bytes > 1GB
Kafka	kafka-consumer-groups.sh --bootstrap-server master:9092 --describe --group fleet-serving-group	LAG column > 100,000
Spark	http://master:4040 → Jobs → Stages → Event Timeline	1 Task chạy 10 phút, các Task khác xong 10 giây (= Data Skew)
Redis	redis-cli INFO memory và redis-cli INFO clients	used_memory > maxmemory hoặc connected_clients = 0



Câu 8 (Airflow - Dynamic DAG Generation):

Nếu hệ thống có 50 trạm sửa chữa (Heads) và mỗi trạm cần 1 DAG báo cáo riêng biệt với lịch chạy khác nhau, bạn làm thế nào để tự động sinh 50 DAGs mà không phải viết 50 file Python giống nhau?

Viết code trong file DAG Python dùng vòng lặp `for head_id in head_list:` để tạo động nhiều DAG objects trong cùng 1 file. Mỗi DAG có `dag_id` duy nhất chứa `head_id`. Cuối cùng gán các DAG objects vào `globals()` để Airflow Scheduler phát hiện được.




Câu 9 (Idempotency on HDFS Parquet):
Bạn nói rằng Pipeline của bạn đảm bảo Idempotent Write trên HDFS Parquet. Nhưng Parquet là Append-Only format (không hỗ trợ UPDATE/DELETE native như Delta Lake).

Làm thế nào bạn đảm bảo Idempotency trên Parquet thuần túy (không dùng Delta Lake)?

Dùng `spark.sql.sources.partitionOverwriteMode = dynamic`. Spark sẽ chỉ ghi đè đúng những thư mục partition bị ảnh hưởng (ví dụ `month=07/`) mà không đụng chạm các partition khác. Dù ghi đè 10 lần, kết quả cuối cùng vẫn giống hệt ghi 1 lần.



Vòng 2 — Câu 1 (Architecture Trade-off):

Tại sao bạn đặt Kafka giữa Debezium và Spark, thay vì để Debezium ghi thẳng vào HDFS hoặc để Spark đọc trực tiếp từ Postgres WAL?

Giải thích cụ thể giá trị kỹ thuật mà Kafka mang lại trong kiến trúc này.

Vì Kafka giữ dữ liệu theo retention period (7 ngày), nên nếu Spark Streaming consumer bị chậm, dữ liệu sẽ không bị mất. Kafka đóng vai trò **bộ đệm (buffer)** giữa CDC producer (Debezium) và ETL consumer (Spark), giúp 2 bên chạy ở tốc độ khác nhau mà không ảnh hưởng lẫn nhau (backpressure isolation). Nếu không có Kafka, khi Spark bị lỗi, Debezium không có nơi gửi dữ liệu → WAL retention tăng → Postgres đầy đĩa.




Câu 2 (Kafka Partitioning Strategy):

Bạn có 50 trạm sửa chữa (Heads), mỗi trạm có hàng trăm xe. Khi Debezium đẩy CDC events lên Kafka topic fleet.odoo.vehicles, bạn chọn partition key là gì?

Nếu chọn head_id (50 partitions) → có vấn đề gì?
Nếu chọn vehicle_id (hàng nghìn partitions) → có vấn đề gì?

"Tôi dùng vehicle_id làm partition key vì cần đảm bảo ordering cho cùng 1 xe (VD: INSERT → UPDATE status → UPDATE maintenance phải đúng thứ tự). Số partition tôi đặt bằng số Spark Executor cores (12 partitions cho cluster 3 node x 4 cores). Debezium mặc định dùng PK của bảng làm key, nên tôi cấu hình SMT ExtractField để map key sang vehicle_id."

Vấn đề của vehicle_id làm partition key:
vehicle_id có cardinality rất cao (hàng nghìn xe) nhưng Kafka topic thường chỉ có 12-50 partitions. Kafka dùng murmur2(key) % num_partitions để phân bổ → nhiều vehicle_id khác nhau rơi vào cùng 1 partition → ordering vẫn đảm bảo cho cùng 1 key, nhưng...
Vấn đề thực tế: Debezium mặc định dùng Primary Key của bảng Postgres làm Kafka message key (không phải vehicle_id mà có thể là id auto-increment). Bạn cần cấu hình Debezium SMT (Single Message Transform) để đổi key sang vehicle_id. 



