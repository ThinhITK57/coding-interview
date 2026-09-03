# 📘 100 CÂU HỎI PHỎNG VẤN SENIOR DE — PHẦN 2: KAFKA, CDC & POSTGRESQL (Q21-Q40)

### Câu 21: Debezium lấy dữ liệu từ PostgreSQL như thế nào? Có phải nó đọc trực tiếp file WAL trên đĩa không?
**Trả lời chuẩn Senior:**
Không, Debezium không đọc trực tiếp file WAL trên đĩa. Nó kết nối tới PostgreSQL thông qua **Streaming Replication Protocol**. 
- PostgreSQL tạo một tiến trình gọi là **Walsender** để đọc WAL và gửi các thay đổi qua kết nối mạng.
- Debezium đóng vai trò như một logical replica (sử dụng plugin `pgoutput` trong các phiên bản Postgres mới). 
- Điều này an toàn hơn vì Debezium không cần quyền truy cập file system của DB server, và Postgres sẽ tự lo việc parse nội dung WAL nhị phân thành format logical (như JSON/Protobuf) trước khi gửi qua mạng.

### Câu 22: Trong PostgreSQL CDC, sự khác biệt giữa `REPLICA IDENTITY FULL` và `DEFAULT` là gì? Tại sao phải dùng `FULL` khi xây dựng SCD Type 2?
**Trả lời chuẩn Senior:**
- **`REPLICA IDENTITY DEFAULT`**: WAL chỉ lưu lại giá trị của khóa chính (Primary Key) đối với các thao tác `UPDATE` và `DELETE`. Nó không ghi lại trạng thái "trước khi đổi" (before-image) của các cột khác.
- **`REPLICA IDENTITY FULL`**: WAL lưu lại giá trị của *tất cả các cột* ở cả trạng thái trước (`before`) và sau (`after`) khi có thao tác `UPDATE`/`DELETE`.
- **Tại sao cần cho SCD2**: Khi xây dựng SCD Type 2, chúng ta cần so sánh dữ liệu cũ và mới để đóng record cũ (cập nhật `end_date`) và tạo bản ghi mới (với `start_date` hiện tại). Nếu chỉ có PK (từ `DEFAULT`), downstream consumer sẽ không biết chính xác thuộc tính nào đã thay đổi để xử lý logic SCD2 một cách chính xác. 
```sql
ALTER TABLE fleet_vehicles REPLICA IDENTITY FULL;
```

### Câu 23: Replication Slot trong PostgreSQL là gì? Rủi ro nào lớn nhất khi dùng nó với Debezium và cách monitor, phòng ngừa?
**Trả lời chuẩn Senior:**
Replication Slot đảm bảo PostgreSQL không xóa các file WAL mà consumer (Debezium) chưa đọc tới.
- **Rủi ro**: Nếu Debezium bị crash hoặc mất kết nối thời gian dài, Postgres sẽ tiếp tục giữ lại tất cả file WAL mới, dẫn đến **WAL bloat**, gây cạn kiệt dung lượng ổ cứng của server DB (Out of Disk), làm crash hệ thống OLTP (Odoo).
- **Cách monitor**: Sử dụng hàm `pg_wal_lsn_diff` để tính độ trễ (tính bằng bytes):
```sql
SELECT slot_name, plugin, slot_type, active, 
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) AS retained_bytes
FROM pg_replication_slots;
```
- **Cách phòng ngừa**: Cấu hình `max_slot_wal_keep_size` (Postgres 13+) để giới hạn dung lượng WAL tối đa mà một slot có thể giữ lại. Nếu vượt ngưỡng này, Postgres sẽ hy sinh slot để bảo vệ DB khỏi bị đầy đĩa.
```ini
# postgresql.conf
max_slot_wal_keep_size = 50GB
```

### Câu 24: Tại sao cần cấu hình `heartbeat.interval.ms` trong Debezium, đặc biệt đối với các bảng có tần suất cập nhật thấp?
**Trả lời chuẩn Senior:**
Trong Postgres, một Replication Slot được cập nhật `confirmed_flush_lsn` chỉ khi Debezium xác nhận đã xử lý xong các sự kiện. Nếu Debezium lắng nghe một database có lượng traffic thấp (hoặc có các bảng rất hiếm khi thay đổi), Postgres có thể không nhận được phản hồi LSN mới từ Debezium, do đó nó không thể xóa WAL cũ, dẫn đến WAL bloat.
- Cấu hình `heartbeat.interval.ms` (ví dụ: `10000` = 10s) buộc Debezium phải gửi tín hiệu nhịp tim định kỳ tới một bảng heartbeat đặc biệt hoặc gửi tín hiệu LSN ack về Postgres ngay cả khi không có thay đổi dữ liệu nào. Điều này giúp tiến trình LSN liên tục tăng lên và Postgres có thể dọn dẹp các WAL files không còn cần thiết.

### Câu 25: Giải thích cơ chế Fault Recovery của Debezium khi bị crash. Làm sao nó biết nên đọc lại từ đâu?
**Trả lời chuẩn Senior:**
Debezium phục hồi dựa trên sự kết hợp giữa Kafka Connect và PostgreSQL Replication Slot:
1. **Kafka Connect Offsets Topic**: Debezium liên tục ghi vị trí LSN (Log Sequence Number) cuối cùng đã xử lý thành công vào một internal topic của Kafka (ví dụ: `connect-offsets`). 
2. **Postgres Replication Slot `confirmed_flush_lsn`**: Đồng thời, nó gửi LSN này về cho Postgres để báo "tôi đã đọc và commit tới đây".
- Khi Debezium crash và khởi động lại, nó sẽ đọc LSN cuối cùng từ topic `connect-offsets`, kết nối lại với Postgres Replication Slot và yêu cầu stream từ LSN đó. Nhờ vậy, đảm bảo at-least-once delivery (có thể gửi lặp một số event nếu crash xảy ra giữa lúc xử lý và lúc commit offset, nhưng không mất dữ liệu).

### Câu 26: Chiến lược chọn Partition Key trong Kafka cho dữ liệu CDC. Làm thế nào để dùng `vehicle_id` làm khóa và tại sao?
**Trả lời chuẩn Senior:**
- **Tại sao**: Trong CDC, thứ tự các event (Insert -> Update -> Delete) là cực kỳ quan trọng. Kafka chỉ đảm bảo thứ tự các message *trong cùng một partition*. Nếu gửi event của cùng một `vehicle_id` vào các partition khác nhau, consumer có thể nhận event Update trước Insert, gây lỗi dữ liệu. Do đó, cần chọn PK (`vehicle_id`) làm Partition Key để hash vào cùng một partition.
- **Cách thực hiện với Debezium**: Debezium mặc định lấy Primary Key làm Kafka message key. Nếu cấu trúc key mặc định quá phức tạp (dạng JSON struct), ta dùng SMT (Single Message Transforms) `ExtractNewRecordState` hoặc `ExtractField` để trích xuất trực tiếp `vehicle_id` làm chuỗi nguyên bản cho key.
```json
"transforms": "extractKey",
"transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
"transforms.extractKey.field": "vehicle_id"
```

### Câu 27: Cụm Kafka 3 broker, Replication Factor = 3, `min.insync.replicas = 2`, `acks = all`. Hệ thống có thể chịu được tối đa bao nhiêu broker chết mà vẫn duy trì Write/Read bình thường?
**Trả lời chuẩn Senior:**
- **Write**: Hệ thống có thể chịu được **1 broker chết**.
  - `acks=all` yêu cầu tất cả các replica trong danh sách ISR (In-Sync Replicas) phải xác nhận.
  - `min.insync.replicas=2` yêu cầu tối thiểu phải có 2 replica còn sống (bao gồm cả Leader) trong ISR để chấp nhận message mới.
  - Nếu 2 broker chết, ISR chỉ còn 1 (< 2), Producer sẽ nhận lỗi `NotEnoughReplicasException`.
- **Read**: Hệ thống có thể chịu được **2 broker chết** (miễn là 1 broker còn lại chứa bản sao của dữ liệu và zookeeper/kraft quorum vẫn tồn tại, tuy nhiên với KRaft/Zookeeper cluster 3 nodes thì mất 2 node sẽ mất Quorum và cluster ngừng hoạt động. Nhưng nói riêng về mặt data replica, 1 node là đủ để read).

### Câu 28: Làm thế nào để monitor Consumer Group Lag trong Kafka bằng CLI? Lag lớn thể hiện điều gì?
**Trả lời chuẩn Senior:**
Dùng lệnh `kafka-consumer-groups.sh` để kiểm tra:
```bash
kafka-consumer-groups.sh --bootstrap-server kafka1:9092,kafka2:9092 \
  --describe --group fleet_scd2_consumer
```
- **Kết quả trả về**: Cột `CURRENT-OFFSET` (vị trí consumer đang đọc), `LOG-END-OFFSET` (vị trí cuối của partition), và `LAG` (hiệu số giữa LOG-END và CURRENT).
- **Ý nghĩa**: Lag lớn báo hiệu Consumer xử lý quá chậm so với tốc độ Producer đẩy dữ liệu vào (có thể do logic SCD2 xử lý chậm ở DB đích, hoặc do network bottleneck). Giải pháp: tăng số lượng consumer (scale out, phải <= số lượng partition), hoặc tối ưu lại logic batch insert/upsert của consumer.

### Câu 29: Nếu Consumer xử lý sai dữ liệu và cần chạy lại (Data Replay) CDC event của ngày hôm qua. Bạn làm như thế nào?
**Trả lời chuẩn Senior:**
Trong Kafka, ta có thể reset lại offset của Consumer Group về một thời điểm trong quá khứ để replay dữ liệu (với điều kiện dữ liệu vẫn còn trong Kafka - chưa bị xóa do retention policy).
- Bước 1: Dừng các ứng dụng/consumer thuộc group đó.
- Bước 2: Dùng lệnh CLI để reset offset về thời điểm cụ thể:
```bash
kafka-consumer-groups.sh --bootstrap-server kafka:9092 \
  --group fleet_scd2_consumer --topic dbserver1.public.fleet_vehicles \
  --reset-offsets --to-datetime 2023-10-01T00:00:00.000 --execute
```
- Bước 3: Khởi động lại ứng dụng, nó sẽ đọc lại từ thời điểm đã reset và ghi đè/xử lý lại (đòi hỏi downstream logic phải Idempotent - không bị ảnh hưởng nếu chạy lại cùng dữ liệu).

### Câu 30: Trong Kafka, khác biệt giữa `retention.ms` và `retention.bytes` là gì? Khi nào message thực sự bị xóa khỏi đĩa?
**Trả lời chuẩn Senior:**
- `retention.ms`: Thời gian tối đa lưu message (mặc định 7 ngày).
- `retention.bytes`: Dung lượng tối đa của một partition (mặc định -1, tức vô hạn).
- **Khi nào bị xóa**: Kafka chia partition thành các "segment files" (mặc định 1GB/file). Việc đánh giá xóa dựa trên **toàn bộ file segment**, không phải từng message lẻ tẻ. 
  - Nếu `retention.ms` vượt quá *hoặc* tổng dung lượng partition vượt `retention.bytes`, Kafka sẽ đánh dấu (mark for deletion) segment *cũ nhất* (nếu timestamp lớn nhất trong segment đó nhỏ hơn thời điểm hiện tại trừ đi retention.ms).
  - Tức là một message dù đã quá hạn vẫn có thể chưa bị xóa ngay nếu segment chứa nó còn chứa các message chưa hết hạn, hoặc tiến trình log cleaner chưa chạy qua.

### Câu 31: PostgreSQL có các mức `wal_level` là `minimal`, `replica`, `logical`. Sự khác biệt và khi nào dùng?
**Trả lời chuẩn Senior:**
- `minimal`: Mức cơ bản nhất, chỉ lưu lượng thông tin tối thiểu cần để khôi phục DB sau crash. Không hỗ trợ HA (High Availability) hay replication. Ít ai dùng trên production.
- `replica`: (Mặc định từ Postgres 10+) Hỗ trợ WAL archiving và read-only replica (streaming replication vật lý). Không hỗ trợ đọc dữ liệu mức dòng (logical).
- `logical`: Bổ sung thông tin vào WAL để có thể trích xuất sự thay đổi dưới dạng logical row-level (JSON/Protobuf...). **Bắt buộc** để sử dụng Debezium CDC hoặc logical replication giữa các database khác phiên bản.

### Câu 32: MVCC trong PostgreSQL là gì? Tại sao các long-running transactions lại gây ra hiện tượng table bloat?
**Trả lời chuẩn Senior:**
- **MVCC (Multi-Version Concurrency Control)**: Cơ chế kiểm soát đồng thời của Postgres. Khi có `UPDATE` hoặc `DELETE`, DB không xóa dòng cũ ngay lập tức mà đánh dấu dòng cũ là "dead tuple" (có `xmax` được set) và tạo một dòng mới. Điều này cho phép các transaction cũ vẫn có thể đọc được dữ liệu đúng thời điểm của chúng mà không bị lock bởi transaction đang ghi.
- **Vấn đề với long-running transaction**: Tiến trình dọn dẹp (Autovacuum) có nhiệm vụ thu hồi không gian của các "dead tuple". Tuy nhiên, Autovacuum **không thể** xóa các dead tuples sinh ra *sau* khi transaction cũ nhất (oldest active transaction) bắt đầu, vì transaction đó *có thể* cần nhìn thấy chúng. Nếu một query chạy quá lâu (ví dụ ETL quên commit), lượng dead tuple tích tụ rất lớn, dẫn đến **table bloat**, làm tăng dung lượng đĩa và chậm các câu query scan.

### Câu 33: Phân biệt các Window Functions cơ bản: `DENSE_RANK`, `NTILE`, `FIRST_VALUE`, `LAG`/`LEAD`.
**Trả lời chuẩn Senior:**
- `DENSE_RANK()`: Xếp hạng các dòng, nếu trùng giá trị thì đồng hạng, thứ hạng tiếp theo **không bị nhảy số** (ví dụ: 1, 2, 2, 3).
- `NTILE(n)`: Chia tập kết quả thành `n` nhóm có số lượng phần tử gần bằng nhau nhất. (vd: chia khách hàng thành 4 nhóm tứ phân vị).
- `FIRST_VALUE(col)`: Trả về giá trị của dòng đầu tiên trong window frame. Đặc biệt chú ý đến thứ tự `ORDER BY` trong hàm `OVER()`.
- `LAG(col, offset)` / `LEAD(col, offset)`: Lấy giá trị của dòng trước (`LAG`) hoặc dòng sau (`LEAD`) dòng hiện tại cách một khoảng `offset`. Cực kỳ hữu dụng để so sánh giá trị thay đổi giữa 2 ngày liên tiếp.

### Câu 34: Materialized Views trong PostgreSQL là gì? Điều kiện để dùng `REFRESH MATERIALIZED VIEW CONCURRENTLY`?
**Trả lời chuẩn Senior:**
- **Materialized View (MV)**: Khác với View thông thường (chỉ là câu lệnh lưu sẵn), MV tính toán và lưu kết quả vật lý ra đĩa, giúp query nhanh hơn cho các báo cáo phức tạp. Tuy nhiên dữ liệu sẽ bị "stale" (cũ) cho đến khi được refresh.
- `REFRESH MATERIALIZED VIEW`: Khóa view, không ai có thể đọc trong lúc refresh.
- `REFRESH MATERIALIZED VIEW CONCURRENTLY`: Refresh ở background, cho phép user tiếp tục đọc dữ liệu cũ trong khi đang cập nhật. Postgres tạo một bảng nhị phân và đổi chỗ nó (swap).
- **Điều kiện bắt buộc**: MV phải có ít nhất một **`UNIQUE INDEX`** trên một cột (hoặc nhiều cột) để Postgres có thể so sánh sự khác biệt (INSERT/UPDATE/DELETE) ở chế độ concurrently.

### Câu 35: Lợi ích của JSONB trong PostgreSQL? Phân biệt các toán tử `->`, `->>`, `@>`, `?|`.
**Trả lời chuẩn Senior:**
`JSONB` lưu dữ liệu dạng phân rã nhị phân, loại bỏ khoảng trắng, sắp xếp lại key, nhưng hỗ trợ **đánh index (GIN)** và query cực nhanh (khác với kiểu `JSON` lưu dạng plain text).
- `->` : Lấy object/array JSON con (trả về kiểu JSON/JSONB).
- `->>` : Lấy giá trị dưới dạng **TEXT** thuần túy (rất hay dùng để ép kiểu, ví dụ `(metadata->>'age')::int`).
- `@>` : Toán tử "chứa" (contains). Ví dụ `WHERE metadata @> '{"status":"active"}'` -> cực kỳ tối ưu vì có thể dùng GIN Index.
- `?|` : Toán tử tồn tại "bất kỳ key nào". Ví dụ `WHERE tags ?| array['error', 'warning']`.

### Câu 36: Thiết kế bảng Audit log bằng Trigger trong Postgres sử dụng `to_jsonb`.
**Trả lời chuẩn Senior:**
Trigger kết hợp với `to_jsonb(OLD)` và `to_jsonb(NEW)` giúp lưu lại lịch sử thay đổi row mà không cần viết lại từng cột (rất linh hoạt khi thêm bớt cột).
```sql
CREATE OR REPLACE FUNCTION audit_log_trigger_func() RETURNS trigger AS $$
BEGIN
    INSERT INTO audit_logs (table_name, operation, old_data, new_data)
    VALUES (
        TG_TABLE_NAME, 
        TG_OP, 
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN to_jsonb(OLD) ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN to_jsonb(NEW) ELSE NULL END
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_audit_fleet_vehicles
AFTER INSERT OR UPDATE OR DELETE ON fleet_vehicles
FOR EACH ROW EXECUTE FUNCTION audit_log_trigger_func();
```

### Câu 37: `EXPLAIN ANALYZE` trong Postgres khác gì `EXPLAIN`? Phân biệt `Seq Scan`, `Index Scan`, `Bitmap Scan`.
**Trả lời chuẩn Senior:**
- `EXPLAIN`: Chỉ đưa ra "kế hoạch dự kiến" dựa trên statistics của bảng (chưa chạy thật).
- `EXPLAIN ANALYZE`: **Chạy thật sự câu query** và so sánh thời gian, số lượng dòng thực tế (actual time/rows) với ước tính. (Cẩn thận khi dùng với `DELETE`/`UPDATE` vì nó sẽ cập nhật dữ liệu thật!).
- **Seq Scan (Sequential Scan)**: Đọc từ đầu đến cuối bảng trên đĩa. Tốt khi phải trả về phần lớn dữ liệu của bảng.
- **Index Scan**: Duyệt cây B-Tree Index, tìm được tham chiếu tới đĩa và bốc trực tiếp từng row ra. Tốt khi lấy một lượng nhỏ dữ liệu rải rác.
- **Bitmap Heap Scan / Bitmap Index Scan**: Lai giữa hai cách trên. Khi cần lấy một lượng vừa phải dữ liệu, Postgres quét Index để gom lại các page/block có chứa dữ liệu cần tìm, sắp xếp lại trong memory (Bitmap), rồi xuống đĩa bốc khối lượng lớn lên cùng lúc theo thứ tự (giảm việc seek đĩa ngẫu nhiên).

### Câu 38: PostgreSQL Partitioning. Sự khác biệt giữa `RANGE`, `LIST`, `HASH` và khi nào sử dụng?
**Trả lời chuẩn Senior:**
Partitioning (chia bảng lớn thành nhiều bảng con vật lý) tối ưu tốc độ bằng Partition Pruning (bỏ qua quét bảng con không chứa dữ liệu).
- **RANGE**: Chia theo khoảng liên tục. Thường dùng nhất cho **thời gian (Time-series)**. VD: Mỗi bảng con lưu dữ liệu 1 tháng. Khi query `WHERE created_at = ...`, nó chỉ quét đúng bảng tháng đó. Quản lý dọn dẹp data cũ rất tiện (chỉ việc `DROP TABLE`).
- **LIST**: Chia theo tập giá trị cụ thể. VD: Chia theo `country_code` ('VN', 'US', 'JP'). Hợp lý khi query thường xuyên lọc theo các category cố định.
- **HASH**: Phân tán ngẫu nhiên và đồng đều dựa trên giá trị băm của khóa. Dùng khi muốn chia đều dữ liệu lớn mà không có quy luật Range hay List nào rõ ràng, tránh hotspot data.

### Câu 39: Trong PostgreSQL, cú pháp `FILTER (WHERE ...)` cho Aggregate functions có ưu điểm gì so với `CASE WHEN` truyền thống khi làm Pivot/Cross-tabulation?
**Trả lời chuẩn Senior:**
Khi tính toán báo cáo (pivot/tổng hợp nhiều điều kiện trên cùng 1 hàng), chuẩn SQL hiện đại hỗ trợ `FILTER(WHERE...)`.
**Cách cũ (CASE WHEN):**
```sql
SELECT 
  SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count,
  SUM(CASE WHEN status = 'MAINTENANCE' THEN 1 ELSE 0 END) AS maint_count
FROM fleet_vehicles;
```
**Cách chuẩn Senior Postgres (FILTER):**
```sql
SELECT 
  COUNT(*) FILTER (WHERE status = 'ACTIVE') AS active_count,
  COUNT(*) FILTER (WHERE status = 'MAINTENANCE') AS maint_count
FROM fleet_vehicles;
```
- **Ưu điểm**: Syntax rõ ràng, đọc dễ hiểu hơn. Postgres Planner có khả năng tối ưu (optimize) mệnh đề `FILTER` tốt hơn `CASE WHEN` trong một số trường hợp tính toán phức tạp, làm giảm CPU overhead.

### Câu 40: Database production (Odoo) bị treo do có query bị lock. Làm sao dùng `pg_stat_activity` và `pg_locks` để xử lý sự cố?
**Trả lời chuẩn Senior:**
Đây là tình huống khẩn cấp On-call:
- **Bước 1**: Xác định query nào đang chờ lock (bị block) quá lâu bằng `pg_stat_activity`:
```sql
SELECT pid, usename, state, query_start, wait_event_type, wait_event, query 
FROM pg_stat_activity 
WHERE state = 'active' AND wait_event_type = 'Lock' 
ORDER BY query_start ASC;
```
- **Bước 2**: Tìm root cause, process nào đang "giữ" lock khiến tiến trình khác phải chờ (thông qua `pg_blocking_pids`):
```sql
SELECT blocked_locks.pid AS blocked_pid,
       blocking_locks.pid AS blocking_pid,
       blocked_activity.query AS blocked_query,
       blocking_activity.query AS blocking_query
FROM pg_catalog.pg_locks blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.GRANTED;
```
*(hoặc dùng cách ngắn hơn là gọi hàm `pg_blocking_pids(pid)` trên Postgres bản mới).*
- **Bước 3**: Kill tiến trình đang giữ lock (nếu là transaction treo, idle in transaction) để giải cứu production:
```sql
SELECT pg_terminate_backend(<blocking_pid>);
```
