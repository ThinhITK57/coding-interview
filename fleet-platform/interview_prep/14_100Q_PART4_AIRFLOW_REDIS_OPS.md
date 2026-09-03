# 📘 100 CÂU HỎI PHỎNG VẤN SENIOR DE — PHẦN 4: AIRFLOW, REDIS & PRODUCTION OPS (Q61-Q80)

### Câu 61: Hãy trình bày kiến trúc tổng thể của Apache Airflow và vai trò của từng thành phần (Scheduler, Executor, Worker, Metadata DB, Webserver).
**Trả lời chuẩn Senior:**
Kiến trúc Airflow được thiết kế theo mô hình phân tán, gồm 5 thành phần cốt lõi tương tác với nhau qua Metadata Database:
1. **Metadata Database (PostgreSQL/MySQL):** Trái tim của Airflow, lưu trữ trạng thái của tất cả DAGs, Tasks, Variables, Connections, và lịch sử chạy.
2. **Scheduler:** Tiến trình daemon chạy liên tục, parse các file DAG, xác định task nào đã đến lịch chạy hoặc đủ điều kiện chạy (dependencies met), sau đó tạo TaskInstance và gửi vào queue của Executor.
3. **Executor:** Nhận task từ Scheduler và quyết định *cách thức* chạy task. Executor không trực tiếp chạy code (trừ Local/Sequential), mà phân phối cho các Workers. 
4. **Worker:** Các tiến trình thực thi code thực tế của task. Trong kiến trúc phân tán (như Celery), Workers là các node riêng biệt poll task từ message broker (như Redis/RabbitMQ).
5. **Webserver:** Giao diện UI (Flask-based) đọc dữ liệu từ Metadata DB để hiển thị trạng thái DAG/Task, logs, và cho phép user trigger/retry task.

**Cơ chế hoạt động:** Scheduler parse DAG -> Lưu trạng thái vào DB -> Gửi task vào Executor -> Executor giao cho Worker -> Worker chạy xong update trạng thái lại vào DB. Webserver chỉ đọc DB để hiển thị.

### Câu 62: So sánh LocalExecutor, CeleryExecutor và KubernetesExecutor. Khi nào nên dùng loại nào?
**Trả lời chuẩn Senior:**
Đây là các cấu hình Executor phổ biến, quyết định khả năng scale của hệ thống Airflow:
1. **LocalExecutor:**
   - **Cơ chế:** Chạy task trên cùng một máy (single node) với Scheduler thông qua các tiến trình con (multiprocessing).
   - **Ưu điểm:** Dễ setup, không cần phụ thuộc bên ngoài, hiệu năng tốt cho single-node.
   - **Nhược điểm:** Không thể scale out vượt quá tài nguyên của máy chủ đó (giới hạn bởi CPU/RAM).
   - **Use case:** Phù hợp cho dự án vừa và nhỏ, resource cố định (như Fleet Maintenance Platform chạy 50 trạm quy mô nhỏ).
2. **CeleryExecutor:**
   - **Cơ chế:** Scale out ra nhiều worker nodes bằng cách dùng Message Broker (Redis/RabbitMQ) làm queue.
   - **Ưu điểm:** Scale ngang tốt, quản lý nhiều worker pools.
   - **Nhược điểm:** Setup phức tạp, cần duy trì thêm Broker và Result Backend.
   - **Use case:** Hệ thống lớn, tải nặng, cần cấp phát resource cố định 24/7 cho các worker.
3. **KubernetesExecutor:**
   - **Cơ chế:** Mỗi task chạy trong một Pod K8s riêng biệt. Airflow gọi K8s API để tạo Pod khi chạy task và xóa Pod khi xong.
   - **Ưu điểm:** Resource cô lập hoàn toàn, tự động scale down về 0 khi không có task (tiết kiệm chi phí), cho phép cấu hình resource (CPU/Memory) riêng rẽ cho từng task qua `executor_config`.
   - **Nhược điểm:** Cần có cluster K8s, độ trễ khởi động Pod (vài giây).
   - **Use case:** Hạ tầng Cloud-native, khối lượng công việc có tính chất "burst" hoặc các task cần thư viện/môi trường Docker khác biệt.

### Câu 63: Trong Airflow, Sensor mode='poke' khác gì với mode='reschedule'? Tại sao dùng 'poke' có thể gây ra hiện tượng Sensor Deadlock?
**Trả lời chuẩn Senior:**
Sensors là operators chờ đợi một điều kiện ngoại vi xảy ra.
1. **`mode='poke'` (Mặc định):**
   - Task chiếm dụng vĩnh viễn một worker slot từ lúc bắt đầu cho đến khi điều kiện thỏa mãn. 
   - Nó chạy hàm poke định kỳ, sleep (tắt luồng) giữa các lần poke, nhưng *không nhả worker slot*.
   - **Vấn đề Sensor Deadlock:** Nếu bạn có 32 worker slots, và bạn có 32 Sensors đang `poke` chờ dữ liệu (chưa có), toàn bộ Airflow cluster sẽ bị kẹt. Các task khác không thể chạy vì không còn slot trống.
2. **`mode='reschedule'`:**
   - Khi hàm poke trả về False, Sensor lập tức nhả (free) worker slot, update trạng thái thành `up_for_reschedule` trong DB.
   - Đến chu kỳ tiếp theo, Scheduler sẽ cấp lại slot cho nó để check lại.
   - **Ưu điểm:** Khắc phục hoàn toàn deadlock, tối ưu tài nguyên.
   - **Nhược điểm:** Overhead nhỏ khi Scheduler phải quản lý việc cấp phát lại nhiều lần.
**Best Practice:** Với các điều kiện có thể phải chờ lâu (> 1-2 phút), **luôn luôn dùng `mode='reschedule'`**.

### Câu 64: Trình bày pattern Dynamic DAG Generation bằng vòng lặp `for` và `globals()`. Điều gì cần lưu ý để tránh làm sập Scheduler?
**Trả lời chuẩn Senior:**
Tạo DAG động hữu ích khi có cấu trúc pipeline giống nhau nhưng áp dụng cho nhiều thực thể (ví dụ: 50 Head stations).
**Pattern bằng globals():**
```python
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime

stations = ['station_A', 'station_B', 'station_C'] # Thường lấy từ biến môi trường hoặc file config tĩnh

for station in stations:
    dag_id = f'fleet_process_{station}'
    dag = DAG(dag_id, start_date=datetime(2023, 1, 1), schedule_interval='@daily')
    
    with dag:
        task1 = DummyOperator(task_id='start')
        
    # Inject vào global scope để Airflow Scheduler có thể nhận diện DAG
    globals()[dag_id] = dag
```
**Lưu ý cực kỳ quan trọng (Tránh sập Scheduler):**
Scheduler parse tất cả code ở cấp độ top-level của file Python (bên ngoài hàm `execute`) liên tục (mặc định 30s/lần).
- **Tuyệt đối KHÔNG thực hiện I/O nặng (query DB, gọi API)** ở top-level để lấy danh sách stations. Nếu query DB mất 5s, mỗi lần parse Scheduler sẽ bị block 5s, gây quá tải, trễ schedule, cpu 100%.
- **Giải pháp:** Đọc danh sách từ Variable, từ file JSON tĩnh, hoặc dùng cơ chế Dynamic Task Mapping (Airflow 2.3+).

### Câu 65: Airflow XCom dùng để làm gì? Trình bày những giới hạn của XCom (size limit, serialization) và best practice khi truyền dữ liệu lớn.
**Trả lời chuẩn Senior:**
XCom (Cross-Communication) cho phép các task trong cùng DAG run chia sẻ dữ liệu (key-value) với nhau.
- **Cơ chế:** Khi task push XCom, dữ liệu được serialize (thường là JSON/Pickle) và lưu thẳng vào Metadata DB. Task khác có thể pull dữ liệu này.
- **Giới hạn (Limitations):**
  - **Size Limit:** Do lưu trong DB (như cột BLOB/TEXT của PostgreSQL), XCom bị giới hạn dung lượng cứng (Postgres: 1GB, MySQL: 64KB - XCom size limit thông thường là rất nhỏ, khuyến nghị < 48KB).
  - **Serialization:** Dữ liệu phải serialize được sang JSON (từ Airflow 2.0 mặc định dùng JSON thay vì Pickle để bảo mật). Không thể truyền trực tiếp DataFrame hoặc Object phức tạp.
- **Best Practice cho Dữ liệu lớn:**
  - KHÔNG truyền data (như file CSV, DataFrame) qua XCom.
  - XCom **chỉ nên chứa metadata hoặc URI** (ví dụ: đường dẫn file S3, `s3://bucket/data_20231010.parquet`).
  - Task 1 ghi data ra S3, push S3 URI vào XCom. Task 2 pull XCom lấy S3 URI và đọc file từ S3 về xử lý.

### Câu 66: Quản lý Secret (Connections và Variables) trong Airflow như thế nào ở môi trường Production?
**Trả lời chuẩn Senior:**
Trong Production, không nên lưu mật khẩu, API key dạng plaintext trong Airflow Webserver UI hay Metadata DB.
Các phương pháp bảo mật:
1. **Sử dụng Fernet Key:** Airflow mã hóa mật khẩu trong Connections/Variables trước khi lưu vào DB bằng một Fernet key đối xứng (cấu hình trong `airflow.cfg`).
2. **Sử dụng Alternative Secrets Backend (Best Practice Production):**
   - Thay vì dùng DB, cấu hình Airflow đọc trực tiếp từ các Secret Managers bên ngoài như **HashiCorp Vault, AWS Secrets Manager, hoặc Google Secret Manager**.
   - **Lợi ích:** Quản lý tập trung, tự động xoay vòng key (rotation), audit logs, không lưu trữ persistent trong Airflow DB.
   - Khi task gọi `BaseHook.get_connection()`, Airflow sẽ ngầm định query tới Secrets Backend để lấy thông tin kết nối theo runtime.
3. **Environment Variables:** Truyền kết nối qua biến môi trường dạng URI (ví dụ: `AIRFLOW_CONN_MY_PG=postgresql://user:pass@host:5432/db`). Airflow tự động map vào connection `my_pg`. 

### Câu 67: Giải thích cơ chế SLA (Service Level Agreement) monitoring trong Airflow và cách cấu hình `sla_miss_callback`.
**Trả lời chuẩn Senior:**
SLA trong Airflow định nghĩa khoảng thời gian tối đa mà một task hoặc DAG phải hoàn thành so với thời điểm execution_date. Nếu vượt quá, một sự kiện "SLA miss" sẽ được sinh ra.
- **Cấu hình:** Truyền tham số `sla=timedelta(hours=2)` vào Operator hoặc `default_args`.
- **Cơ chế:** Scheduler có một luồng định kỳ check các task có cài SLA. Nếu thời gian hiện tại > `execution_date + sla` và task chưa chạy xong, nó sẽ ghi log vào DB.
- **`sla_miss_callback`:** Hàm được kích hoạt khi miss SLA.
```python
def my_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    # Logic gửi cảnh báo (VD: Slack, PagerDuty, Email)
    send_slack_alert(f"SLA Missed for DAG {dag.dag_id}")

dag = DAG('sla_dag', default_args={
    'sla': timedelta(hours=1),
    'email_on_failure': True
}, sla_miss_callback=my_sla_miss_callback)
```
*Lưu ý:* SLA được tính từ `execution_date`, không phải từ lúc task bắt đầu chạy. Nó không giết (kill) task đang chạy (đó là `execution_timeout`).

### Câu 68: Làm thế nào để tạo một Custom Airflow Operator? Nêu ví dụ override hàm `execute(context)`.
**Trả lời chuẩn Senior:**
Để tái sử dụng logic (DRY), ta kế thừa class `BaseOperator` và override hàm `execute`.
- `__init__`: Nhận các tham số cấu hình riêng (API endpoints, query, connection_id), dùng decorator `@apply_defaults` (bắt buộc trước bản 2.0).
- `execute(self, context)`: Chứa logic chính. Biến `context` là dictionary chứa các macro như `execution_date`, `ti` (task_instance).
**Ví dụ:**
```python
from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook

class FleetMaintenanceAPIOperator(BaseOperator):
    def __init__(self, endpoint, conn_id='fleet_api', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = endpoint
        self.conn_id = conn_id

    def execute(self, context):
        execution_date = context['execution_date'].isoformat()
        self.log.info(f"Calling Fleet API at {self.endpoint} for {execution_date}")
        
        # Dùng Hook để lấy credentials
        connection = BaseHook.get_connection(self.conn_id)
        # Thực hiện request
        import requests
        response = requests.get(f"{connection.host}/{self.endpoint}", auth=(connection.login, connection.password))
        response.raise_for_status()
        
        # Giá trị return tự động được push vào XCom
        return response.json() 
```

### Câu 69: Trình bày cơ chế phân luồng dependency với `trigger_rule` (all_success, one_success, none_failed).
**Trả lời chuẩn Senior:**
Mặc định, một task chỉ chạy khi TẤT CẢ các task đứng trước nó (upstream) chạy thành công (`trigger_rule='all_success'`). Ta có thể tùy chỉnh rule này:
- **`all_success`** (Default): Upstream fail hoặc skipped -> task này sẽ bị skipped.
- **`one_success`**: Chỉ cần MỘT task upstream thành công, task này lập tức chạy. Phù hợp cho thiết kế pipeline có nhiều nhánh fallback, nhánh nào xong trước thì đi tiếp.
- **`none_failed`**: Tất cả upstream đã chạy xong (thành công hoặc bị skip), nhưng KHÔNG có task nào Failed. Hay dùng ở task tổng hợp cuối DAG sau khi dùng `BranchPythonOperator` (nơi mà một số nhánh sẽ cố ý bị skip).
- **`one_failed`**: Chạy ngay khi có ít nhất 1 task upstream thất bại. Dùng cho các task dọn dẹp (cleanup, rollback) hoặc gửi cảnh báo lỗi đặc thù.
- **`all_done`**: Chạy khi tất cả upstream đã có kết quả cuối cùng (thành công, lỗi, hoặc skip). Bất chấp trạng thái là gì, task cuối (teardown) vẫn chạy.

### Câu 70: Giải thích sự khác biệt giữa `catchup=True` và `catchup=False`. Backfill hoạt động như thế nào?
**Trả lời chuẩn Senior:**
`catchup` quyết định cách Scheduler xử lý các khoảng thời gian bị bỏ lỡ (missed schedule intervals) trong quá khứ.
- **`catchup=True` (Mặc định):** Nếu start_date là 1 tháng trước, và DAG mới được bật (unpaused) hôm nay, Scheduler sẽ tự động tạo và chạy tất cả các DAG runs bị thiếu từ 1 tháng trước đến nay (theo schedule_interval). 
  - *Cảnh báo:* Có thể gây quá tải toàn bộ hệ thống ngay khi bật DAG nếu start_date quá xa.
- **`catchup=False`:** Bỏ qua toàn bộ quá khứ. Scheduler chỉ tạo DAG run cho khoảng thời gian hợp lệ gần nhất ngay trước thời điểm hiện tại. Luôn nên set `catchup=False` trong production trừ phi có chủ đích.
- **Backfill:** Là quá trình chạy lại dữ liệu trong quá khứ một cách chủ động thông qua CLI, bỏ qua cờ `catchup=False`.
  ```bash
  airflow dags backfill -s 2023-01-01 -e 2023-01-31 my_dag
  ```
  Backfill không làm phiền lịch chạy hiện tại và có giới hạn song song riêng, đảm bảo an toàn hơn để fill data.

### Câu 71: Các cấu trúc dữ liệu cơ bản của Redis (String, Hash, List, Set, Sorted Set, Geo) và Use Case tiêu biểu cho từng loại.
**Trả lời chuẩn Senior:**
Redis không chỉ là Key-Value mà là Data Structures Server.
1. **String:** Binary safe strings. 
   - *Use case:* Caching HTML, session, đếm số (Counters) bằng `INCR`, phân phối khóa (Distributed Locks) bằng `SETNX`.
2. **Hash (HSET/HGET):** Lưu dictionary/object. O(1).
   - *Use case:* Lưu thông tin user, thông tin của 1 chiếc xe (vehicle_id -> {status, location, battery}).
3. **List (LPUSH/RPOP):** Linked list. O(1) ở hai đầu.
   - *Use case:* Message queues cơ bản, timeline feed mới nhất.
4. **Set (SADD/SMEMBERS):** Tập hợp không thứ tự, các phần tử unique. O(1).
   - *Use case:* Tập hợp các tag, bạn bè chung (SINTER), các IP bị block.
5. **Sorted Set (ZADD/ZRANGE):** Set kết hợp với Score (float) để sắp xếp. O(log(N)).
   - *Use case:* Leaderboards, Rate limiting theo sliding window, hàng đợi ưu tiên, scheduler theo timestamp.
6. **Geo (GEOADD/GEORADIUS):** Dựa trên Sorted Set (mã hóa tọa độ thành geohash 52-bit làm score). 
   - *Use case:* Tìm điểm sửa xe gần nhất, theo dõi vị trí real-time của xe.

### Câu 72: Trình bày cơ chế tìm kiếm không gian (Spatial Queries) trong Redis dùng GEOADD và GEOSEARCH. Độ phức tạp O(log(N)+M) nghĩa là gì?
**Trả lời chuẩn Senior:**
Redis hỗ trợ Geo queries bằng cách mã hóa cặp Longitude/Latitude thành GeoHash string (biến đổi không gian 2D thành mảng 1D). Cấu trúc lưu trữ ngầm chính là Sorted Set (ZSET).
- **Thêm điểm:** `GEOADD fleet:stations 105.8048 21.0285 "Station_A"`
- **Tìm kiếm:** (Từ Redis 6.2 dùng `GEOSEARCH`, thay thế cho `GEORADIUS`)
  `GEOSEARCH fleet:stations FROMLONLAT 105.8 21.0 BYRADIUS 5 km WITHDIST ASC`
- **Độ phức tạp `O(log(N) + M)`:**
  - `N` là tổng số phần tử trong Sorted Set. Tìm kiếm vùng (Bounding box của GeoHash) trong ZSET mất thời gian nhị phân: `O(log(N))`.
  - `M` là số lượng phần tử trả về (nằm trong bán kính). Duyệt qua M phần tử để tính khoảng cách chính xác (haversine) và lọc ra kết quả mất `O(M)`.
  - Tức là hiệu năng cực kỳ nhanh và phụ thuộc chính vào diện tích tìm kiếm (làm tăng M). 

### Câu 73: Tại sao cần dùng Lua Script trong Redis cho các thao tác check-and-set? Cơ chế `EVALSHA` và tính chất single-threaded của Redis đảm bảo atomicity như thế nào?
**Trả lời chuẩn Senior:**
- **Bài toán:** Chống Overbooking (cấp 1 slot sửa xe cho 2 người cùng lúc). Cần kiểm tra slot trống (GET) và chiếm slot (SET) một cách nguyên tử (Atomic). Cấu trúc `GET` rồi `SET` rời rạc bằng ứng dụng sẽ sinh ra Race Condition do nhiều threads cùng gọi.
- **Giải pháp:** Sử dụng Lua Script.
  Redis thực thi lệnh tuần tự trên một luồng duy nhất (Single-threaded Event Loop). Khi một Lua Script được chạy, Redis block toàn bộ các client khác cho đến khi script chạy xong. Điều này đảm bảo tính Atomic tuyệt đối:
  ```lua
  local available = redis.call("GET", KEYS[1])
  if tonumber(available) > 0 then
      redis.call("DECR", KEYS[1])
      return 1
  else
      return 0
  end
  ```
- **`EVALSHA` Optimization:** Truyền toàn bộ script dạng string (EVAL) mỗi lần sẽ tốn băng thông. Thay vào đó, tải script lên Redis một lần (`SCRIPT LOAD`), Redis trả về mã SHA-1. Ứng dụng sau đó chỉ cần gọi `EVALSHA <sha1> 1 <key>`, giảm thiểu overhead mạng đáng kể.

### Câu 74: So sánh Redis Pub/Sub và Redis Streams cho bài toán broadcast sự kiện.
**Trả lời chuẩn Senior:**
1. **Redis Pub/Sub:**
   - **Cơ chế:** Hoạt động theo nguyên lý "Fire and Forget". Client PUBLISH message vào channel, mọi client đang SUBSCRIBE sẽ nhận được.
   - **Lưu trữ:** KHÔNG lưu message. Nếu một subscriber bị rớt mạng lúc message được gửi, nó sẽ vĩnh viễn mất message đó.
   - **Use case:** Push notifications real-time, WebSocket broadcast layer, invalidation cache, nơi mà mất message (đôi khi) được chấp nhận hoặc có cơ chế sync full data đắp vào.
2. **Redis Streams (Giống Kafka thu nhỏ):**
   - **Cơ chế:** Append-only log. Message (gồm ID sinh tự động và cặp Key-Value) được ghi vào Stream và lưu trữ vĩnh viễn (hoặc đến khi bị cắt bằng `MAXLEN`).
   - **Tính năng:** Hỗ trợ Consumer Groups. Nhiều consumer có thể share tải (chỉ 1 consumer trong group nhận message), có cơ chế `XACK` xác nhận đã xử lý xong.
   - **Use case:** Xử lý pipeline bất đồng bộ cần sự tin cậy (reliable queue), event sourcing, đảm bảo không mất dữ liệu ngay cả khi client rớt mạng.

### Câu 75: Trình bày cơ chế Persistence của Redis (RDB vs AOF) và Trade-offs khi vận hành trên Production.
**Trả lời chuẩn Senior:**
Redis lưu data hoàn toàn trên RAM (In-Memory), nhưng hỗ trợ 2 cơ chế đẩy xuống ổ cứng (Disk Persistence) để phục hồi khi restart:
1. **RDB (Redis Database Snapshot):**
   - **Cơ chế:** Chụp lại toàn bộ bộ nhớ ra một file nhị phân `.rdb` theo chu kỳ (vd: 5 phút nếu có 100 thay đổi). Thực hiện bằng cách `fork()` tiến trình con.
   - **Ưu điểm:** File nhỏ, backup dễ, tốc độ load lại khi khởi động cực nhanh. Disk I/O thấp (chỉ ghi lúc snapshot).
   - **Nhược điểm:** Mất dữ liệu (Data Loss) trong khoảng thời gian giữa 2 lần snapshot nếu server crash.
2. **AOF (Append Only File):**
   - **Cơ chế:** Ghi log từng câu lệnh write (SET, INCR...) vào file `.aof` ngay lập tức (thường cấu hình `fsync` mỗi giây).
   - **Ưu điểm:** Đảm bảo độ bền dữ liệu cao (chỉ mất tối đa 1 giây dữ liệu).
   - **Nhược điểm:** File phình to (cần cơ chế BGREWRITEAOF), tốc độ restart chậm do phải replay lại toàn bộ lệnh, overhead Disk I/O cao hơn.
**Best Practice Production:** Bật cả hai. RDB để backup hàng ngày/chuyển server nhanh, AOF (fsync=everysec) để đảm bảo không mất data khi crash.

### Câu 76: Lập trình bất đồng bộ Python (Asyncio) hoạt động thế nào? Sự khác biệt giữa coroutines và luồng (threads) khi xử lý concurrent I/O (WebSocket server).
**Trả lời chuẩn Senior:**
- **Threads (Đa luồng):** Hệ điều hành tạo nhiều thread. Chuyển đổi ngữ cảnh (context switching) do OS quản lý, tốn tài nguyên RAM và CPU overhead. Trong Python bị giới hạn bởi GIL (chỉ 1 thread chạy mã Python tại 1 thời điểm).
- **Asyncio (Event Loop):**
  - Chạy trên một luồng duy nhất (Single-threaded).
  - Sử dụng Event Loop để theo dõi các tác vụ I/O (mạng, file, database). 
  - Khi một Coroutine (hàm `async def`) gặp lệnh `await` (vd: chờ mạng trả kết quả), nó sẽ trả lại quyền điều khiển (yield control) cho Event Loop. Event Loop lập tức chạy một Coroutine khác.
  - Hàm `asyncio.create_task()` dùng để schedule các coroutine chạy "đồng thời" trên event loop mà không block.
- **Áp dụng (WebSocket):** Hàng ngàn kết nối WebSocket có thể được duy trì mở đồng thời trên một process Python duy nhất vì phần lớn thời gian các kết nối đều ở trạng thái "chờ" (I/O bound). Asyncio cực kì nhẹ (ít overhead memory) so với việc sinh ra 1000 threads.

### Câu 77: So sánh các công nghệ đẩy dữ liệu Real-time: WebSocket vs Server-Sent Events (SSE) vs REST Polling (Short/Long).
**Trả lời chuẩn Senior:**
1. **REST Polling (Short Polling):**
   - Client gọi API liên tục mỗi 3s. Tốn tài nguyên mạng, overhead HTTP headers, độ trễ cao, tạo tải vô ích lên server khi không có data mới.
2. **Long Polling:**
   - Client gọi API, server treo request (không trả lời) cho đến khi có data mới thì mới respond. Cải thiện so với short polling nhưng duy trì connection HTTP mở tốn kém, không tối ưu cho high-frequency events.
3. **Server-Sent Events (SSE):**
   - Giao thức chuẩn HTTP/1.1. Client mở 1 kết nối, Server liên tục push data về (dạng text stream).
   - **Ưu điểm:** Dễ dùng, hỗ trợ tự động reconnect, qua firewall/proxy HTTP dễ dàng.
   - **Nhược điểm:** Kết nối một chiều (chỉ Server -> Client).
4. **WebSocket:**
   - Nâng cấp từ HTTP sang giao thức TCP độc lập. Kết nối Full-Duplex (Hai chiều liên tục).
   - **Ưu điểm:** Overhead cực thấp (không có HTTP headers cồng kềnh sau khi handshake), độ trễ cực thấp, client và server có thể chủ động đẩy data.
   - **Use case:** Lý tưởng cho Fleet Dashboard cần cập nhật tọa độ xe (10 times/second) và có thể gửi tín hiệu điều khiển ngược lại từ UI.

### Câu 78: Giả sử Dashboard hiển thị trạng thái xe bị trễ 5 phút so với thực tế. Hãy trình bày phương pháp chẩn đoán lỗi (Diagnosis) qua từng lớp hệ thống (Postgres → Kafka → Spark → Redis → UI).
**Trả lời chuẩn Senior:**
Để tìm "nút thắt cổ chai" (bottleneck), ta phải trace theo luồng dữ liệu (Data Lineage):
1. **Postgres & Debezium (CDC Layer):** 
   - Check `pg_replication_slots` trên Postgres: Lượng `restart_lsn` có bị giữ lại nhiều không? Nếu có, Debezium đang đọc chậm hoặc mất kết nối.
2. **Kafka (Message Broker Layer):**
   - Check **Kafka Consumer Lag** (vd: qua Prometheus/Burrow). Nếu số lượng message unread (lag) trong topic tăng đột biến, chứng tỏ Spark Streaming đang tiêu thụ chậm hơn tốc độ sản xuất.
3. **Apache Spark (Processing Layer):**
   - Mở Spark UI -> Streaming tab. Check `Processing Time` so với `Batch Duration`. Nếu `Processing Time > Batch Duration`, cluster đang bị quá tải (backpressure). Cần check Data Skew, GC pause, hoặc tăng executor cores.
4. **Redis (Serving Layer):**
   - Gọi lệnh `INFO` hoặc check metric: Số lượng kết nối (connected_clients), CPU Usage (nếu tiệm cận 100% nghĩa là single-thread Redis đang bị block bởi lệnh O(N) nào đó như KEYS *), Memory usage.
5. **WebSocket Server (Push Layer):**
   - Check log Asyncio (Event Loop bị block?). Nếu có logic tính toán CPU-bound vô tình viết trong coroutine, nó sẽ treo cả event loop, làm chậm việc push data xuống Dashboard.

### Câu 79: Thiết kế pipeline Idempotent là gì? Đưa ra ví dụ về cách thiết kế Idempotent cho (1) Cập nhật trạng thái vào Redis, (2) Ghi dữ liệu vào HDFS/S3.
**Trả lời chuẩn Senior:**
- **Định nghĩa:** Idempotent (Tính lũy đẳng) nghĩa là việc chạy lại (retry) một tác vụ một lần hay 100 lần thì trạng thái/kết quả cuối cùng của hệ thống vẫn giống hệt nhau, không sinh ra lỗi hay lặp dữ liệu (duplicate). Đặc tính bắt buộc trong phân tán (At-least-once delivery).
- **Ví dụ 1: Ghi vào HDFS/S3:**
  - *Non-idempotent:* Task append (nối) file CSV. Nếu task fail giữa chừng và chạy lại, dữ liệu bị ghi đúp.
  - *Idempotent:* Phân vùng (Partitioning) data theo `execution_date` (vd: `s3://data/dt=20231010/`). Mỗi lần chạy sẽ ghi đè (Overwrite) toàn bộ thư mục đó. Spark có chế độ `mode("overwrite")` hoặc Airflow xóa phân vùng trước khi task load data.
- **Ví dụ 2: Cập nhật Redis:**
  - *Non-idempotent:* Nhận event "xe chạy 5km", dùng lệnh `INCRBY distance 5`. Nếu consumer đọc đúp event do Kafka rebalance, quãng đường bị sai.
  - *Idempotent:* Data source đẩy tọa độ GPS tuyệt đối + timestamp. Task dùng HSET lưu trạng thái: `HSET vehicle:123 distance 15200 timestamp 1690001000`. Dù nhận duplicate, việc HSET cùng một giá trị vẫn bảo toàn tính chính xác. (Cần check timestamp để tránh out-of-order events: chỉ update nếu TS mới > TS cũ).

### Câu 80: Mô tả một Monitoring Stack tiêu chuẩn (Prometheus + Grafana) cho Data Platform. Các metrics quan trọng cần theo dõi đối với Kafka, Spark và Redis là gì?
**Trả lời chuẩn Senior:**
Monitoring Stack chuẩn: Các hệ thống (Kafka, Spark) phơi bày endpoint `/metrics`. **Prometheus** đóng vai trò pull các metrics (chuỗi thời gian) định kỳ. **Grafana** query từ Prometheus qua PromQL để vẽ Dashboard và thiết lập Alerting.
Các Golden Metrics cần theo dõi:
1. **Kafka (JMX Exporter):**
   - `kafka_server_brokertopicmetrics_messagesin_total` (Throughput).
   - **Consumer Lag:** Khoảng cách giữa offset lớn nhất của partition và offset consumer đã commit. Nếu trend tăng dần -> báo động đỏ.
   - `UnderReplicatedPartitions`: Báo hiệu có Broker đang chết hoặc rớt mạng.
2. **Spark Structured Streaming:**
   - `spark_streaming_processingRate`: Tốc độ xử lý (records/sec).
   - `spark_streaming_schedulingDelay`: Thời gian chờ của batch trước khi được cấp resource. (Backpressure indicator).
3. **Redis (Redis Exporter):**
   - `redis_memory_used_bytes`: Cảnh báo khi tiệm cận Max Memory (sắp kích hoạt eviction policies hoặc OOM kill).
   - `redis_connected_clients`: Phát hiện connection leak.
   - `redis_commands_duration_seconds`: Chờ lâu (Latency), phát hiện các câu lệnh slow query (gây block event loop).
   - `redis_keyspace_hits / misses`: Tính tỉ lệ Cache Hit Ratio.

---
*Tài liệu nội bộ: Kiến thức thiết kế và tối ưu cho Fleet Maintenance Platform.*
