# 📘 100 CÂU HỎI PHỎNG VẤN SENIOR DE — PHẦN 1: APACHE SPARK & PYSPARK (Q1-Q20)

### Câu 1: Trình bày chi tiết 4 giai đoạn của Catalyst Optimizer trong Spark SQL.
**Trả lời chuẩn Senior:**
Catalyst Optimizer là "trái tim" của Spark SQL, biến đổi truy vấn (SQL hoặc DataFrame API) thành Kế hoạch Thực thi Vật lý (Physical Plan) tối ưu thông qua 4 giai đoạn:
1. **Analysis (Phân tích):** Spark kiểm tra cú pháp và đối chiếu các tên cột/bảng với **Catalog** (metadata lưu thông tin schema). Nó biến đổi Unresolved Logical Plan thành **Resolved Logical Plan**. Nếu sai tên cột, lỗi sẽ văng ra ở bước này.
2. **Logical Optimization (Tối ưu hóa Logic):** Áp dụng các rules (quy tắc) dựa trên heuristics để tối ưu hóa Resolved Logical Plan. Các tối ưu phổ biến:
   - *Constant Folding:* Tính toán trước các hằng số (VD: `1 + 1` thành `2`).
   - *Predicate Pushdown:* Đẩy các điều kiện `WHERE`/`FILTER` xuống gần nguồn dữ liệu nhất (đặc biệt hiệu quả với Parquet/ORC).
   - *Column Pruning:* Chỉ đọc những cột thực sự cần thiết.
   Kết quả là **Optimized Logical Plan**.
3. **Physical Planning (Lên kế hoạch Vật lý):** Spark sinh ra một hoặc nhiều Physical Plans từ Optimized Logical Plan. Ở đây, Spark quyết định *cách* thực hiện (VD: chọn Sort-Merge Join hay Broadcast Hash Join). Sau đó, nó dùng Cost Model để chọn ra Physical Plan có chi phí thấp nhất (thường dựa trên kích thước bảng/thống kê).
4. **Code Generation (Sinh mã - Project Tungsten):** Spark dùng tính năng *Whole-Stage Code Generation* để biến toàn bộ cây truy vấn thành Java bytecode tối ưu (tương tự như code viết tay) thực thi trực tiếp trên CPU, gộp nhiều toán tử vào một hàm duy nhất để tránh việc gọi hàm ảo (virtual function calls).

### Câu 2: Tungsten Engine sử dụng Off-Heap Memory và UnsafeRow như thế nào để tránh GC pauses?
**Trả lời chuẩn Senior:**
Project Tungsten tối ưu hóa mạnh mẽ hiệu năng CPU và RAM trong Spark thông qua việc tự quản lý bộ nhớ thay vì phụ thuộc vào JVM:
- **Off-Heap Memory & `sun.misc.Unsafe`:** Thay vì lưu các object Java thông thường (ví dụ một chuỗi 4 ký tự trong Java tốn tới 48 bytes do object header), Tungsten cấp phát bộ nhớ trực tiếp trên RAM (Off-Heap) bằng API `sun.misc.Unsafe`. Do bộ nhớ này nằm ngoài sự kiểm soát của JVM, **Garbage Collector (GC) của Java sẽ không quét vùng nhớ này**, giúp triệt tiêu hoàn toàn hiện tượng GC Pauses (Dừng chương trình để dọn rác) khi xử lý tập dữ liệu lớn.
- **UnsafeRow format:** Tungsten mã hóa dữ liệu thành định dạng nhị phân gọn nhẹ gọi là `UnsafeRow`. Cấu trúc này liên tục trên RAM, thân thiện với CPU Cache (L1/L2/L3), cho phép Spark tính toán trực tiếp trên dữ liệu nhị phân mà không cần giải mã (deserialize) ngược lại thành Java Objects (vd: so sánh 2 chuỗi trực tiếp qua byte offset).

### Câu 3: Giải thích cơ chế Watermark trong Structured Streaming và công thức tính. Nó giúp dọn dẹp State Store ra sao?
**Trả lời chuẩn Senior:**
Trong xử lý stream, dữ liệu thường đến trễ (late data). Watermark là một ngưỡng thời gian "di động" giúp Spark quyết định dữ liệu nào đã quá trễ và có thể bị loại bỏ, đồng thời giải phóng bộ nhớ (State Store).
- **Công thức:** $Watermark = Max(EventTime)_{seen} - DelayThreshold$
- **Cơ chế hoạt động & State Store Cleanup:** Spark duy trì trạng thái (state) cho các tính toán có trạng thái (như window aggregation, stream-stream join). Bất kỳ dữ liệu nào đến với `EventTime < Watermark` sẽ bị **drop** (bỏ qua). Khi Watermark vượt qua một khung giờ (window), Spark coi như không còn dữ liệu nào cho khung giờ đó nữa, nó sẽ xuất kết quả cuối cùng (nếu dùng append mode) và **xóa toàn bộ state của khung giờ đó khỏi State Store** (lưu trên HDFS/RocksDB). Điều này chặn đứng hiện tượng phình to vô hạn của RAM/Disk khi chạy streaming 24/7.

### Câu 4: Sự khác biệt giữa outputMode 'append', 'update', và 'complete' khi kết hợp với Watermark là gì?
**Trả lời chuẩn Senior:**
- **Append Mode (Chỉ thêm mới):** Chỉ những dòng kết quả *đã chốt* (không bao giờ thay đổi nữa) mới được ghi ra sink. Với Watermark, một khung window chỉ được ghi ra sink khi Watermark đã vượt qua `Window_End_Time`. Đây là mode duy nhất an toàn để ghi ra các hệ thống không hỗ trợ update (như File Sink/HDFS).
- **Update Mode (Cập nhật):** Chỉ những record/khung window có *sự thay đổi* (hoặc mới sinh ra) trong micro-batch hiện tại mới được ghi ra sink. Thường dùng khi sink là database (Redis, Postgres, Cassandra) có khả năng upsert/update.
- **Complete Mode (Toàn bộ):** Spark luôn ghi lại *toàn bộ* Bảng Kết Quả (Result Table) vào sink ở mỗi micro-batch. Không hỗ trợ kết hợp với Watermark để xóa state (vì phải giữ state để xuất lại toàn bộ). Thường chỉ dùng cho truy vấn gom nhóm nhỏ gọn.

### Câu 5: So sánh Broadcast Hash Join và Sort-Merge Join. Ngưỡng (threshold) nào quyết định việc chọn mỗi loại?
**Trả lời chuẩn Senior:**
- **Broadcast Hash Join (BHJ):**
  - *Cơ chế:* Spark gửi (broadcast) bảng nhỏ gọn (Dimension table) tới tất cả các Executor. Executor build in-memory hash table và duyệt bảng lớn (Fact table) để match.
  - *Ưu điểm:* Không có bước Shuffle (Zero Shuffle), cực kỳ nhanh. Giảm tắc nghẽn mạng cục bộ trên cụm 3-node bare-metal.
  - *Khi nào dùng:* Khi bảng nhỏ bé hơn `spark.sql.autoBroadcastJoinThreshold` (mặc định 10MB, có thể tăng lên 50-100MB tùy RAM). Dùng hint `/*+ BROADCAST(dim_table) */`.
- **Sort-Merge Join (SMJ):**
  - *Cơ chế:* 3 pha: Shuffle (phân phối lại dữ liệu 2 bảng theo join key) -> Sort (sắp xếp theo join key ở từng partition) -> Merge (trộn 2 partition đã sort lại).
  - *Ưu điểm:* Xử lý được hai bảng siêu lớn. Không bị lỗi Out Of Memory.
  - *Khi nào dùng:* Mặc định cho dữ liệu lớn. (Trong Spark 3.x, AQE có thể tự động giáng cấp SMJ xuống BHJ nếu sau lọc, bảng hóa ra nhỏ hơn threshold).

### Câu 6: Trình bày từng bước kỹ thuật Salting (Thêm muối) để giải quyết Data Skew trong Spark.
**Trả lời chuẩn Senior:**
Data Skew (lệch dữ liệu) xảy ra khi một join key xuất hiện quá nhiều, khiến 1 Executor (VD Slave 1) ôm đồm tính toán (straggler) trong khi Slave 2 rảnh rỗi.
**Kỹ thuật Salting (VD: Join bảng FACT bị lệch với DIM):**
1. **Thêm Salt vào Fact:** Tạo một số ngẫu nhiên từ `0` đến `N-1` (VD N=10) nối vào Join Key của bảng lớn. (Ví dụ: key `NULL` hoặc `ID_1` thành `ID_1_5`).
   ```python
   fact_df = fact_df.withColumn("salt", F.round(F.rand() * 9))
   fact_salted = fact_df.withColumn("salted_key", F.concat_ws("_", "join_key", "salt"))
   ```
2. **Explode (Nhân bản) bảng Dim:** Nhân bản mỗi dòng của bảng nhỏ (DIM) lên N lần, tương ứng với các giá trị salt từ `0` đến `N-1`.
   ```python
   dim_df = dim_df.withColumn("salt_array", F.array([F.lit(i) for i in range(10)]))
   dim_exploded = dim_df.withColumn("salt", F.explode("salt_array"))
   dim_salted = dim_exploded.withColumn("salted_key", F.concat_ws("_", "join_key", "salt"))
   ```
3. **Thực hiện Join:** Join trên `salted_key`. Lúc này, lượng dữ liệu khổng lồ của `ID_1` trong FACT đã được chia đều ra 10 phân vùng khác nhau để xử lý song song, khớp với 10 bản sao của `ID_1` bên DIM. Tốc độ tăng đột biến.

### Câu 7: So sánh coalesce(n) và repartition(n). Khi nào nên dùng loại nào?
**Trả lời chuẩn Senior:**
- **`repartition(n)`:** Thay đổi số lượng partition bằng cách thực hiện **Full Shuffle** toàn bộ dữ liệu qua mạng. Có thể tăng hoặc giảm số lượng partition.
  *Khi nào dùng:* Muốn tăng tính song song, hoặc muốn phân phối lại dữ liệu thật đều (khắc phục skew) trước những tính toán nặng.
- **`coalesce(n)`:** Chỉ có thể **giảm** số lượng partition (hoặc giữ nguyên) mà **không sinh ra Shuffle** (tránh I/O mạng tốn kém). Nó gộp các partition cục bộ trên cùng một Node lại với nhau.
  *Khi nào dùng:* Sau filter loại bỏ nhiều dữ liệu, hoặc trước khi ghi ra HDFS để tránh ghi quá nhiều file nhỏ (Small Files Problem), ví dụ `df.coalesce(5).write.parquet()`.

### Câu 8: Vấn đề "Small Files Problem" trên HDFS từ Streaming sinh ra lỗi gì cho NameNode? Giải pháp Compaction là gì?
**Trả lời chuẩn Senior:**
- **Nguyên nhân & Hậu quả:** Structured Streaming ghi dữ liệu liên tục theo micro-batch (vài giây/phút 1 lần). Kết quả là tạo ra hàng vạn file Parquet kích thước KB/MB trên HDFS. HDFS NameNode lưu metadata của *mỗi* file, block vào RAM (khoảng 150 bytes/file). Càng nhiều file nhỏ, **RAM của NameNode càng phình to**, làm chậm toàn bộ cluster hoặc crash NameNode (NameNode RAM Bloat). Nó cũng làm chậm các Spark batch jobs đọc dữ liệu (quá nhiều thao tác mở/đóng file).
- **Giải pháp Compaction:** Viết một Spark Batch Job định kỳ (VD: hàng ngày lúc 1h sáng) đọc thư mục chứa file nhỏ và ghi đè lại thành file lớn (cỡ 128MB - 256MB).
  ```python
  spark.read.parquet("hdfs://path/date=2023-10-01/")\
       .coalesce(10)\
       .write.mode("overwrite").parquet("hdfs://temp_path/")
  # Xóa path cũ, move temp_path về path cũ (bảo đảm tính toàn vẹn)
  ```

### Câu 9: Tính năng `spark.speculation = true` giải quyết vấn đề Straggler Tasks trên hệ thống Bare-Metal bị suy thoái đĩa như thế nào?
**Trả lời chuẩn Senior:**
Trên cụm Bare-Metal 3-node, một ổ cứng HDD/SSD bị bad sector hoặc quá tải I/O (degraded disk) ở Slave 1 có thể khiến một Task chạy siêu chậm, kéo lùi cả Stage (Straggler Task).
- **Cách hoạt động:** Khi bật `spark.speculation = true`, Spark theo dõi tiến trình của các tasks. Nếu phát hiện một task đang chạy chậm hơn đáng kể so với trung bình (cấu hình qua `spark.speculation.multiplier`, mặc định 1.5x), Spark sẽ **phóng ra một bản sao (speculative task)** của task đó sang một Node khác (VD: Slave 2).
- Cả 2 task cùng chạy đua. Task nào hoàn thành trước, Spark sẽ lấy kết quả của task đó và "kill" task chậm chạp còn lại. Cực kỳ hiệu quả cho hệ thống HDFS có disk chập chờn.

### Câu 10: Chế độ Dynamic Partition Overwrite Mode của Spark giải quyết tính Idempotent (ghi đè an toàn) khi ghi HDFS như thế nào?
**Trả lời chuẩn Senior:**
Khi ghi dữ liệu vào HDFS có chia phân vùng (Partitioned by `date`), nếu job bị fail giữa chừng hoặc chạy lại (backfill), ta muốn ghi đè (overwrite) một cách an toàn mà không làm mất phân vùng khác.
- Mặc định (`spark.sql.sources.partitionOverwriteMode=static`), chế độ overwrite sẽ **XÓA TOÀN BỘ** thư mục cha và chỉ giữ lại dữ liệu mới, gây mất dữ liệu các ngày khác.
- Đặt `spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")`: Lúc này, Spark chỉ ghi đè (overwrite) lên **những phân vùng nào mà dữ liệu mới chạm tới** (VD: chỉ ghi đè thư mục `date=2023-10-01`), các phân vùng khác (`date=2023-09-30`) hoàn toàn nguyên vẹn. Đảm bảo tính Idempotent khi rerun pipeline.

### Câu 11: Structured Streaming đạt được Exactly-Once Semantics nhờ sự kết hợp giữa Replayable Source và FileStreamSinkLog như thế nào?
**Trả lời chuẩn Senior:**
Để không mất hoặc trùng lặp dữ liệu khi hệ thống sập (Exactly-Once), Spark yêu cầu 3 yếu tố:
1. **Replayable Source (Kafka):** Lưu lại offset đọc. Spark duy trì Write-Ahead Log (WAL) lưu trạng thái (offsets) đã đọc vào Checkpoint HDFS. Nếu sập, nó xin lại Kafka từ offset cũ.
2. **Idempotent / Transactional Sink:** Đối với File Sink (HDFS), Spark lưu một siêu dữ liệu (Metadata) dưới dạng một Transaction Log gọi là `FileStreamSinkLog` (thuộc thư mục `_spark_metadata`).
3. **Ghi theo batch nguyên tử:** Dữ liệu từng micro-batch được sinh ra thành file ẩn, sau khi xong, Spark update `FileStreamSinkLog` ghi nhận batch_id đã thành công. Nếu crash, khi restart, Spark check SinkLog, bỏ qua các file thừa (uncommitted) và rerun đúng batch bị fail. Kết quả cuối cùng luôn đúng Exactly-Once.

### Câu 12: Sử dụng foreachBatch() sink để ghi song song (Dual-Write) vào HDFS (Data Lake) và Redis (Fast Cache) một cách Transactional.
**Trả lời chuẩn Senior:**
Structured Streaming mặc định chỉ hỗ trợ 1 sink. Để ghi 1 luồng dữ liệu vào 2 hệ thống (Data Lake và NoSQL), ta dùng `foreachBatch`.
- **Cơ chế:** Trong hàm `foreachBatch(df, batch_id)`, ta có thể thực thi nhiều lệnh output cho `df` (micro-batch DataFrame).
- **Lưu ý Cực Kỳ Quan Trọng:** Vì DataFrame `df` sẽ được execute lại từ đầu (re-computed) cho mỗi `.write`, nếu không cẩn thận sẽ chạy lại toàn bộ DAG tính toán 2 lần! Để tránh điều này, phải **cache** DataFrame lại trước khi ghi:
  ```python
  def write_to_both(df, batch_id):
      df.cache() # Bắt buộc
      # Ghi vào HDFS SCD2 staging
      df.write.format("parquet").mode("append").save("hdfs://lake/stg/")
      # Ghi vào Redis (Dùng thư viện spark-redis)
      df.write.format("org.apache.spark.sql.redis").option("table", "cdc_cache").mode("overwrite").save()
      df.unpersist()
  
  stream.writeStream.foreachBatch(write_to_both).start()
  ```

### Câu 13: Cấu trúc thư mục Checkpoint của Structured Streaming gồm các thư mục offsets/, commits/, state/. Vai trò của từng cái?
**Trả lời chuẩn Senior:**
Thư mục Checkpoint là sinh mạng của Structured Streaming (lưu trên HDFS):
- `offsets/`: Lưu trữ Write-Ahead Log (WAL). Ghi lại chính xác offset của Kafka mà Spark *đang định* xử lý trong micro-batch hiện tại (Batch N).
- `commits/`: Xác nhận hoàn thành. Sau khi Batch N hoàn tất ghi ra sink, Spark ghi vào `commits/` một file báo hiệu batch_id N đã thành công. Khi restart, nếu thấy offsets N nhưng thiếu commits N, Spark biết batch N tạch và sẽ rerun.
- `state/`: Lưu trữ Trạng thái in-memory (RocksDB hoặc HDFS back-end) của các phép tính Stateful (Window Aggr, Watermark). Đảm bảo giữ được state giữa các lần khởi động lại.
- `sources/` và `sinks/`: Metadata của source/sink.
*Lưu ý:* Xóa nhầm thư mục này đồng nghĩa với việc phá vỡ luồng stream.

### Câu 14: Xử lý chuỗi JSON Envelope từ Debezium CDC Kafka (chứa payload.before, payload.after, payload.op) trong PySpark.
**Trả lời chuẩn Senior:**
Debezium đẩy dữ liệu vào Kafka dạng JSON lồng nhau phức tạp. Để bung dữ liệu hiệu quả bằng PySpark, ta dùng hàm `from_json` cùng schema định nghĩa sẵn `StructType`.
```python
schema = StructType([
    StructField("payload", StructType([
        StructField("before", StringType()), # Hoặc định nghĩa chi tiết
        StructField("after", StringType()),
        StructField("op", StringType()) # c, u, d
    ]))
])

# Đọc từ Kafka
df = spark.readStream.format("kafka")...load()
# Cột value của Kafka là binary, cast sang chuỗi và phân tích JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
              .withColumn("data", F.from_json("value", schema)) \
              .select("data.payload.*")
              
# Lọc lấy các bản ghi update (u) hoặc insert (c)
final_df = parsed_df.filter(F.col("op").isin("c", "u")).select("after.*")
```

### Câu 15: Tại sao Stream-Stream Join lại CẦN THIẾT phải có Watermark trên CẢ HAI stream và ràng buộc Time Range?
**Trả lời chuẩn Senior:**
Khi Join hai DataStream (VD: Ad Clicks stream Join với Ad Impressions stream):
- Nếu không có Watermark, Spark phải lưu lại *toàn bộ* lịch sử của cả 2 stream vào State Store mãi mãi (để chờ lỡ record kia đến trễ 1 năm), gây Out-of-Memory ngay lập tức.
- **Ràng buộc:** Bắt buộc (1) Đặt Watermark trên cả hai stream. (2) Phải có điều kiện thời gian giữa 2 stream trong mệnh đề JOIN, ví dụ:
  `click.clickTime >= imp.impTime AND click.clickTime <= imp.impTime + interval 1 hour`
- Nhờ vậy, Spark biết rằng State của luồng Impressions sau 1 giờ (+ độ trễ watermark) sẽ KHÔNG BAO GIỜ cần dùng để join với Clicks nữa, và sẽ dọn dẹp (clean-up) state an toàn.

### Câu 16: Sử dụng hàm ROLLUP và CUBE trong PySpark cho tác vụ báo cáo đa chiều (Multi-dimensional Reporting) của Star Schema.
**Trả lời chuẩn Senior:**
Thực hiện Data Warehouse OLAP trên HDFS:
- **`ROLLUP` (Cuộn lên):** Tính tổng hợp theo cấp bậc (hierarchical). VD: `df.rollup("Country", "City").sum("Revenue")` sẽ sinh ra các dòng tổng doanh thu theo (Country, City), theo (Country), và Tổng tất cả (Grand Total). N cột sẽ sinh ra N+1 cấp độ.
- **`CUBE` (Khối lập phương):** Tính toán tổng hợp cho **TẤT CẢ tổ hợp có thể có**. VD: `df.cube("Country", "City").sum("Revenue")` sẽ sinh ra tổng theo (Country, City), (Country), **(City)**, và (Grand Total). Thích hợp dựng OLAP Cube để end-user slice-and-dice bất cứ chiều nào.
- *Kết hợp:* Dùng hàm `F.grouping_id()` để xác định xem dòng hiện tại là kết quả tổng hợp của những cột nào (lưu thành cột meta trong Star Schema).

### Câu 17: Cách đọc giao diện Spark UI (Jobs → Stages → Tasks) để chẩn đoán nguyên nhân gây thắt cổ chai (bottleneck).
**Trả lời chuẩn Senior:**
Một kỹ sư Senior phải rành Spark UI:
1. **Jobs tab:** Nhìn xem Job nào đang chạy lâu nhất. Một Action (như `.write()`, `.count()`) sinh ra 1 Job.
2. **Stages tab:** DAG Scheduler chia Job thành các Stages. Ranh giới (boundary) giữa các Stage chính là **Shuffle** (Wide Transformation như `groupBy`, `join`). Nếu một Stage tốn nhiều thời gian, rà soát lượng "Shuffle Read/Write Size".
3. **Tasks tab:** Một Stage gồm nhiều Tasks (mỗi task xử lý 1 partition cục bộ trên 1 CPU core). Nếu:
   - Các tasks kết thúc trong vài giây, nhưng 1 task tốn 2 tiếng -> Bị Data Skew!
   - Garbage Collection Time (GC Time) màu đỏ và chiếm > 10% thời gian chạy -> Cần tăng RAM hoặc dùng Off-heap (Tungsten).

### Câu 18: Tham số spark.sql.shuffle.partitions (mặc định 200). Khi nào cần tăng/giảm và cách tính tối ưu?
**Trả lời chuẩn Senior:**
Tham số này quyết định số lượng partition được sinh ra SAU các phép tính Shuffle (join, groupBy).
- Giá trị mặc định 200 thường gây hại:
  - Nếu dữ liệu quá LỚN (vd 1TB), 200 partitions = mỗi partition 5GB -> Quá lớn, tràn RAM Executor (OOM). -> **Phải tăng lên**.
  - Nếu dữ liệu quá NHỎ (vd 50MB), 200 partitions = mỗi partition vài trăm KB -> Small Files Problem, tốn thời gian lập lịch Tasks hơn cả tính toán. -> **Phải giảm xuống**.
- **Công thức chuẩn Senior:**
  $Partitions = \frac{\text{Total Shuffle Stage Input Size}}{\text{Target Partition Size (100MB - 200MB)}}$
  Đồng thời, con số này nên là BỘI SỐ của tổng số Cores toàn cụm (VD: 3 Node x 16 Cores = 48 -> Nên chọn 48, 96, 144...).

### Câu 19: Tính năng Adaptive Query Execution (AQE) trong Spark 3.x tự động hóa việc gộp shuffle partitions và xử lý skew như thế nào?
**Trả lời chuẩn Senior:**
Trong Spark 3.0+, AQE thay đổi cách lập kế hoạch: thay vì chốt DAG ngay từ đầu, nó tối ưu hóa *ngay trong lúc chạy* (tại ranh giới giữa các Stages).
1. **Dynamically Coalesce Shuffle Partitions:** Giải quyết câu 18 tự động. Ta cứ set `spark.sql.shuffle.partitions` thật lớn (VD: 1000). Sau pha Shuffle, AQE nhìn thấy nhiều partition quá bé, nó sẽ tự động `coalesce` gộp các khối dữ liệu nhỏ kề nhau thành partition lớn vừa đủ (mặc định 64MB) để xử lý ở Stage sau, tiết kiệm tài nguyên.
2. **Dynamically Optimize Skew Joins:** AQE phát hiện partition bị Skew (> 256MB) ở runtime, nó tự động cắt nhỏ partition đó ra thành nhiều mảnh và nhân bản dữ liệu tương ứng ở bảng đối diện, thực hiện Join song song (tương tự như kỹ thuật Salting nhưng tự động hoàn toàn).
3. **Demote to Broadcast Join:** Chuyển đổi SMJ thành BHJ nếu sau Filter bảng lớn bị teo nhỏ.

### Câu 20: Phân bổ bộ nhớ Spark (Memory Management): Phân biệt spark.executor.memory, memoryOverhead, và spark.memory.fraction.
**Trả lời chuẩn Senior:**
Bộ nhớ trong 1 Container/Pod của Executor được chia rất khắt khe:
- **`spark.executor.memory` (Heap Memory):** Bể bộ nhớ chính của JVM. Cấu thành từ 2 vùng do `spark.memory.fraction` (mặc định 0.6 = 60%) chia sẻ:
  - *Execution Memory:* Dành cho tính toán (shuffles, joins, sorts). Nếu thiếu sẽ tràn đĩa (Spill to Disk).
  - *Storage Memory:* Dành cho Caching (`df.cache()`, broadcast variables).
  Vùng 40% còn lại là *User Memory* dành cho các biến nội bộ hoặc object người dùng khởi tạo (UDFs).
- **`spark.executor.memoryOverhead` (Off-Heap):** Mặc định bằng 10% của Executor Memory (tối thiểu 384MB). Đây là vùng RAM non-JVM dành cho hệ điều hành, NIO buffers, hoặc các thư viện C/C++ native (như PySpark Python processes, Netty, TCP buffers).
- *Lỗi phổ biến:* Nếu code dùng PySpark UDFs (Python chạy process riêng ngoài JVM) và sinh dữ liệu lớn, Container sẽ bị hệ điều hành "OOM Killed" (exit code 137). Khắc phục: phải tăng `memoryOverhead` lên, thay vì tăng `executor.memory`.
