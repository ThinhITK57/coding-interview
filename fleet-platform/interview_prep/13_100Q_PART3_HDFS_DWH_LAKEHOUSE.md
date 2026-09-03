# 📘 100 CÂU HỎI PHỎNG VẤN SENIOR DE — PHẦN 3: HDFS, DATA WAREHOUSE & LAKEHOUSE (Q41-Q60)

### Câu 41: Kiến trúc HDFS hoạt động như thế nào? Tại sao block size thường là 128MB?
**Trả lời chuẩn Senior:**
Kiến trúc HDFS (Hadoop Distributed File System) dựa trên mô hình Master/Slave:
- **NameNode (Master):** Lưu trữ metadata (file hierarchy, quyền truy cập, mapping giữa file và blocks) trên bộ nhớ RAM để đảm bảo tốc độ tra cứu siêu nhanh. Chịu trách nhiệm quản lý namespace và điều phối truy cập.
- **DataNode (Slave):** Lưu trữ dữ liệu vật lý dưới dạng các block. Thực hiện các thao tác đọc/ghi theo yêu cầu của client và gửi Heartbeat/BlockReport định kỳ về NameNode.
- **Block Size (128MB hoặc 256MB):** 
  - Tối ưu hóa seek time: Ổ cứng từ (HDD) cần thời gian để định vị đầu đọc (seek time). Nếu file quá nhỏ, seek time sẽ chiếm phần lớn thời gian đọc. Với block 128MB, transfer time sẽ áp đảo seek time (khoảng 1% seek time).
  - Giảm tải cho RAM của NameNode: Mỗi block tốn khoảng 150 bytes metadata trên RAM. File lớn chia thành block lớn sẽ giảm số lượng block, giúp NameNode quản lý Petabytes dữ liệu mà không bị tràn RAM.

### Câu 42: Replication Factor mặc định của HDFS là 3. Giải thích chiến lược Rack-Awareness và điều gì xảy ra khi 1 DataNode chết?
**Trả lời chuẩn Senior:**
**Rack-Awareness Placement Policy:**
HDFS sắp xếp 3 bản sao (replica) như sau để tối đa hóa khả năng chịu lỗi và băng thông mạng:
1. Replica 1: Đặt trên node cục bộ nơi client đang ghi dữ liệu (hoặc một node ngẫu nhiên nếu client ở ngoài cluster).
2. Replica 2: Đặt trên một node thuộc một **rack khác** với rack của replica 1.
3. Replica 3: Đặt trên một node khác thuộc **cùng rack** với replica 2.

**Khi 1 DataNode chết:**
1. NameNode không nhận được Heartbeat từ DataNode đó trong khoảng thời gian timeout định trước (thường là 10.5 phút).
2. NameNode đánh dấu DataNode là "Dead".
3. NameNode quét metadata và phát hiện các block trên node đó bị "Under-replicated" (dưới mức 3).
4. NameNode điều phối các DataNode khác đang giữ bản sao của các block đó tiến hành copy (replicate) sang các DataNode còn sống để khôi phục lại Replication Factor bằng 3.

### Câu 43: Tại sao Parquet thường được dùng cho Data Lake/Warehouse? Giải thích Column Pruning và Predicate Pushdown.
**Trả lời chuẩn Senior:**
Parquet là định dạng lưu trữ cột (columnar format) được sinh ra để tối ưu hóa truy vấn OLAP:
- **Snappy Compression:** Parquet có tỷ lệ nén tốt nhờ việc các giá trị cùng kiểu dữ liệu đứng cạnh nhau (ví dụ: dùng Run-Length Encoding, Dictionary Encoding), sau đó nén bằng Snappy mang lại sự cân bằng giữa tốc độ nén/giải nén và dung lượng (Compression ratio thường từ 1:3 đến 1:4).
- **Column Pruning (Projection Pushdown):** Động cơ truy vấn (như Spark, Presto) chỉ đọc vật lý những cột được gọi trong câu lệnh `SELECT`, bỏ qua toàn bộ các cột khác trên disk, giảm đáng kể I/O.
- **Predicate Pushdown (Filter Pushdown):** Parquet lưu trữ metadata (min/max/count/nulls) ở cấp độ Row Group và Page. Khi có mệnh đề `WHERE` (ví dụ `WHERE date = '2023-10-01'`), engine sẽ kiểm tra metadata trước. Nếu giá trị không nằm trong khoảng min/max của Row Group, nó sẽ skip toàn bộ Row Group đó mà không cần đọc dữ liệu vật lý.

### Câu 44: So sánh Parquet, ORC và Avro. Khi nào nên dùng định dạng nào?
**Trả lời chuẩn Senior:**
- **Avro (Row-based):** 
  - Lưu trữ theo dòng, schema lưu cùng dữ liệu (JSON format).
  - Rất mạnh về **Schema Evolution** (thêm/sửa/xóa cột dễ dàng).
  - **Usecase:** Dùng làm landing zone, message format trong Kafka, hoặc khi cần xử lý toàn bộ các cột của dòng (ví dụ ETL ban đầu).
- **Parquet (Column-based):** 
  - Tối ưu cho truy vấn đọc phân tích (read-heavy, OLAP), nén cực tốt. Hỗ trợ nested data phức tạp.
  - Hỗ trợ tốt nhất bởi hệ sinh thái Spark, Impala, Drill.
  - **Usecase:** Data Lake storage, Data Warehouse (fact/dim tables), các bảng cần query aggregation nhanh.
- **ORC (Column-based):** 
  - Tương tự Parquet nhưng tối ưu riêng cho **Hive/Presto**. Hỗ trợ ACID transactions trên Hive.
  - **Usecase:** Phù hợp nếu kiến trúc heavily relies on Hive hoặc Presto.

### Câu 45: Đánh giá Trade-offs giữa Star Schema, Snowflake Schema và Data Vault trong thiết kế Data Warehouse.
**Trả lời chuẩn Senior:**
1. **Star Schema (Kimball):**
   - *Ưu điểm:* Cấu trúc đơn giản (1 Fact nối trực tiếp với nhiều Dims được denormalized). Truy vấn nhanh nhất do ít JOIN. Dễ hiểu cho BI users.
   - *Nhược điểm:* Dư thừa dữ liệu (Data redundancy) trong các bảng Dimension. Cập nhật (Update/Insert) có thể chậm hơn do bảng Dim lớn.
2. **Snowflake Schema:**
   - *Ưu điểm:* Dimension được chuẩn hóa (Normalized) thành nhiều bảng con (ví dụ: Customer -> City -> Country), tiết kiệm không gian lưu trữ, đảm bảo tính toàn vẹn dữ liệu.
   - *Nhược điểm:* Truy vấn phức tạp, nhiều JOIN, hiệu năng đọc giảm đi đáng kể. Khó sử dụng cho end-users.
3. **Data Vault (Hub, Link, Satellite):**
   - *Ưu điểm:* Cực kỳ linh hoạt, audit-ability cao (lưu mọi lịch sử), thiết kế cho Enterprise Data Warehouse quy mô khổng lồ. Schema evolution dễ dàng (chỉ cần thêm Satellite mới, không phá vỡ cấu trúc cũ).
   - *Nhược điểm:* Cấu trúc phức tạp, số lượng bảng bùng nổ, không dùng trực tiếp cho BI (phải build Star Schema view/marts ở phía trên).

### Câu 46: Trong Kimball Star Schema, phân biệt Fact table và Dimension table. Có những loại measure nào trong Fact?
**Trả lời chuẩn Senior:**
- **Dimension Tables:** Chứa bối cảnh (context) của nghiệp vụ — các câu hỏi "Ai, Cái gì, Ở đâu, Khi nào" (Vd: `dim_customer`, `dim_date`, `dim_head`). Thường chứa text, attributes mô tả, schema rộng (nhiều cột).
- **Fact Tables:** Chứa các sự kiện đo lường được (metrics) của quy trình nghiệp vụ (Vd: `fact_repair_service_revenue`, `fact_parts_sales`). Chứa các khóa ngoại (Foreign Keys) trỏ tới Dimensions và các Measures.
- **Phân loại Measures:**
  1. **Additive:** Có thể cộng gộp theo mọi dimensions. (Vd: `revenue`, `quantity`).
  2. **Semi-additive:** Chỉ cộng gộp được theo một số dimensions, thường KHÔNG cộng được theo thời gian (Vd: `inventory_level` - số tồn kho cuối tháng không thể cộng dồn giữa các tháng, nhưng có thể cộng giữa các kho).
  3. **Non-additive:** Không thể cộng gộp theo bất kỳ dimension nào (Vd: Tỷ lệ phần trăm `margin_ratio`, nhiệt độ, giá đơn vị `unit_price`). Phải tính toán lại từ các base metrics.

### Câu 47: So sánh SCD Type 1, Type 2, Type 3. Khi nào dùng cái nào và sự khác biệt trong triển khai?
**Trả lời chuẩn Senior:**
Slowly Changing Dimensions (SCD) xử lý sự thay đổi thuộc tính theo thời gian:
- **SCD Type 1 (Overwrite):** 
  - Ghi đè giá trị cũ bằng giá trị mới. Không lưu lịch sử.
  - *Usecase:* Sửa lỗi chính tả, hoặc các trường không quan trọng với business context quá khứ (ví dụ: số điện thoại hiện tại của KH).
- **SCD Type 2 (Add new row):** 
  - Tạo dòng mới với giá trị mới. Đóng dòng cũ bằng cách cập nhật ngày hết hạn (`expiration_date`) và cờ `is_current = FALSE`.
  - *Usecase:* Bắt buộc khi cần báo cáo chính xác tại một thời điểm trong quá khứ (ví dụ: Lịch sử thay đổi địa chỉ của `dim_customer` để phân bổ doanh thu đúng khu vực tại thời điểm bán).
- **SCD Type 3 (Add new column):** 
  - Thêm một cột mới (vd: `previous_city`) trên cùng một dòng để lưu giá trị cũ, và cập nhật giá trị mới vào cột `current_city`.
  - *Usecase:* Chỉ cần so sánh "hiện tại" và "ngay trước đó", hiếm dùng vì không scale nếu đổi nhiều lần.

### Câu 48: Mô tả thuật toán 5 bước thực hiện SCD Type 2 Merge bằng PySpark (với left_anti join).
**Trả lời chuẩn Senior:**
Để xử lý SCD2 hiệu quả (ví dụ `dim_customer`) trên Data Lake không hỗ trợ UPDATE từng dòng, ta áp dụng pattern:
1. **Identify New/Changed Records:** Đọc dữ liệu daily batch (Source) và lấy `dim_customer` hiện tại (Target, lọc `is_current = True`).
2. **Left Anti Join (Find new inserts):** Dùng Left Anti Join từ Source sang Target (dựa vào `customer_id` hoặc so sánh hash các cột thuộc tính) để lấy tập Khách hàng MỚI hoàn toàn.
3. **Inner Join (Find changed updates):** Dùng Inner Join giữa Source và Target để tìm các Khách hàng ĐÃ TỒN TẠI nhưng CÓ SỰ THAY ĐỔI (ví dụ: so sánh hash `address` khác nhau).
4. **Prepare Expired Records:** Đối với tập "Changed", lấy dòng cũ từ Target, cập nhật `is_current = False`, `expiration_date = current_date()`.
5. **Union & Overwrite (hoặc Merge):**
   - Tập `Insert` = (New Records) UNION (Changed Records với giá trị mới, `is_current = True`, `effective_date = current_date()`).
   - Gộp `Insert` với tập `Expired Records` và phần dữ liệu không thay đổi của Target.
   - Ghi đè (Overwrite) lại partition, hoặc sử dụng cơ chế `MERGE INTO` của Delta Lake/Iceberg.

### Câu 49: Đánh giá thiết kế Surrogate Key: MD5 Hash vs Auto-increment (Identity) vs UUID.
**Trả lời chuẩn Senior:**
Surrogate Keys (SK) rất quan trọng để cô lập DWH khỏi sự thay đổi của Source (Natural Keys).
- **Auto-increment (Sequence/Identity):**
  - *Ưu điểm:* Dễ hiểu, key là số nguyên (Integer) tốn ít dung lượng, JOIN rất nhanh.
  - *Nhược điểm:* Cực kỳ khó triển khai trong môi trường phân tán (Spark/Hadoop) vì cần một bộ đếm tập trung (global lock), gây nghẽn cổ chai.
- **UUID (Universally Unique Identifier):**
  - *Ưu điểm:* Hoàn toàn độc lập, không đụng độ, dễ sinh ra trên hệ thống phân tán.
  - *Nhược điểm:* Chuỗi string 36 ký tự, rất tốn dung lượng, làm tăng size của Fact table và chậm quá trình JOIN (String so sánh chậm hơn Integer).
- **MD5/SHA Hash (Business Key Hash):**
  - *Ưu điểm:* Deterministic (cùng 1 Natural Key luôn ra 1 Hash). Có thể tạo song song trên Spark mà không cần lookup bảng Dim cũ. Có thể cast về BigInt (MurmurHash) hoặc lưu dưới dạng chuỗi hexa. Rất phổ biến trong Data Vault và Lakehouse.
  - *Nhược điểm:* Xác suất đụng độ (Collision) tuy cực nhỏ nhưng vẫn có. Chi phí CPU để băm.

### Câu 50: Bảng `dim_date` cần thiết kế như thế nào cho hệ thống Fleet Maintenance có năm tài chính từ 01/10 đến 30/09?
**Trả lời chuẩn Senior:**
`dim_date` là bảng dimension tĩnh, thường pre-compute (tạo sẵn trước 10-20 năm) để tránh logic xử lý date phức tạp trong query.
Các cột cần thiết:
- `date_key` (INT, dạng `YYYYMMDD` vd: 20231001)
- `full_date` (DATE)
- `calendar_year`, `calendar_month`, `calendar_quarter`
- **Tùy biến Fiscal (Tài chính):** 
  - `fiscal_year`: Nếu `month >= 10`, fiscal year = `calendar_year + 1` (ví dụ 01/10/2023 thuộc fiscal_year 2024).
  - `fiscal_quarter`: Tháng 10,11,12 là `Q1`; Tháng 1,2,3 là `Q2`; 4,5,6 là `Q3`; 7,8,9 là `Q4`.
- **Cờ nghiệp vụ (Business flags):**
  - `is_weekend` (BOOLEAN)
  - `is_holiday` (BOOLEAN): Được maintain thủ công hoặc lấy từ API ngày lễ quốc gia.
  - Phục vụ báo cáo số ngày làm việc (working days) trừ đi cuối tuần và ngày lễ.

### Câu 51: Trình bày chi tiết 3 trường bắt buộc cho SCD Type 2: effective_date, expiration_date, is_current.
**Trả lời chuẩn Senior:**
Để quản lý timeline lịch sử trong SCD Type 2, mỗi record phải có:
1. **effective_date (hoặc valid_from):** Timestamp/Date chỉ ra thời điểm record bắt đầu có hiệu lực. Thường là thời gian của hệ thống nguồn (CDC) hoặc thời gian chạy batch (ETL load time).
2. **expiration_date (hoặc valid_to):** Thời điểm record bị thay thế bởi phiên bản mới hoặc bị xóa. 
   - Với record đang hiện hành, giá trị này thường được gán bằng một ngày vô cực trong tương lai (ví dụ: `9999-12-31`) để tránh null logic khi query `BETWEEN`.
3. **is_current (hoặc current_flag):** Cờ (BOOLEAN Y/N, 1/0) chỉ định bản ghi hiện tại. 
   - Rất quan trọng để tăng tốc độ truy vấn khi user chỉ muốn xem "dữ liệu mới nhất" (`WHERE is_current = 1`) thay vì phải check khoảng thời gian.

### Câu 52: So sánh kiến trúc Data Lake, Data Warehouse và Lakehouse.
**Trả lời chuẩn Senior:**
- **Data Warehouse (DWH):**
  - Schema-on-write (cấu trúc phải được định nghĩa trước khi nạp dữ liệu). Lưu dữ liệu có cấu trúc (Relational/Star schema).
  - Tối ưu cho BI, SQL analytis, tính nhất quán (ACID) cao, truy vấn nhanh.
  - *Nhược điểm:* Đắt đỏ, không lưu được unstructured data (hình ảnh, logs), khó áp dụng Machine Learning.
- **Data Lake (vd: HDFS, S3):**
  - Schema-on-read. Lưu mọi loại dữ liệu (structured, semi-structured, unstructured) dạng file raw (Parquet, CSV, JSON).
  - Rẻ, linh hoạt, rất tốt cho Data Science/ML.
  - *Nhược điểm:* Dễ biến thành "Data Swamp" (đầm lầy), không có ACID transaction (việc UPDATE/DELETE rất khó khăn), data quality thấp.
- **Lakehouse:**
  - Kết hợp ưu điểm của cả hai: Dùng object storage/HDFS rẻ (như Data Lake) nhưng phủ lên các format mở (Delta Lake, Apache Iceberg, Hudi) để hỗ trợ ACID, schema evolution và BI-level performance (như DWH).
  - Cung cấp một nguồn sự thật duy nhất (Single Source of Truth) cho cả BI và AI/ML.

### Câu 53: Delta Lake và Apache Iceberg giải quyết vấn đề gì trên Object Storage/HDFS? Giải thích Time Travel và Schema Evolution.
**Trả lời chuẩn Senior:**
Chúng cung cấp tính năng **ACID Transactions** cho Data Lake thông qua metadata layer (Transaction Logs/Manifest files):
1. **ACID trên file tĩnh:** Khi thực hiện `UPDATE/DELETE`, engine không sửa trực tiếp file Parquet cũ (immutable), mà tạo ra file Parquet mới và ghi nhận vào log. Readers luôn thấy một snapshot nhất quán. Ngăn ngừa dirty reads.
2. **Time Travel:** Nhờ việc giữ lại các file cũ và metadata log, ta có thể truy vấn dữ liệu tại một thời điểm trong quá khứ hoặc phục hồi nhầm lẫn. Vd: `SELECT * FROM table TIMESTAMP AS OF '2023-10-01'`.
3. **Schema Evolution:** Cho phép thay đổi cấu trúc bảng (thêm/đổi tên/đổi kiểu dữ liệu cột) mà không phải viết lại (rewrite) toàn bộ dữ liệu lịch sử. Engine sẽ tự xử lý (schema resolution) khi đọc file cũ thiếu cột (gán giá trị null).

### Câu 54: Data Quality trong pipeline ETL: Bạn sẽ triển khai kiểm tra chất lượng dữ liệu như thế nào (dùng Great Expectations hoặc Deequ)?
**Trả lời chuẩn Senior:**
Kiểm tra Data Quality (DQ) nên được nhúng trực tiếp vào các bước của ETL pipeline (ví dụ giữa Bronze và Silver layer):
- **Sử dụng Great Expectations (GE) / AWS Deequ (trên Spark):**
  - Định nghĩa các "Expectations" (quy tắc):
    - `expect_column_values_to_not_be_null('customer_id')`
    - `expect_column_values_to_be_unique('invoice_no')`
    - `expect_column_values_to_be_between('margin', min_value=0, max_value=100)`
- **Xử lý khi DQ check fail (Circuit Breaker pattern):**
  - *Cảnh báo (Warning):* Lỗi nhỏ (vd: 1% null) -> Log cảnh báo, gửi Slack, nhưng vẫn cho qua pipeline.
  - *Quarantine (Cách ly):* Đẩy các record lỗi (bad records) sang Dead Letter Queue (DLQ) / Quarantine table để điều tra, cho các bản ghi tốt tiếp tục.
  - *Fail/Halt:* Lỗi nghiêm trọng (vd: primary key bị null) -> Dừng (Fail) toàn bộ job ngay lập tức để tránh làm bẩn DWH.

### Câu 55: Chiến lược Partition trên HDFS: Tại sao dùng year/month/day cho time-series và nguy cơ của việc over-partitioning là gì?
**Trả lời chuẩn Senior:**
- **Chiến lược year/month/day:**
  - Hữu ích nhất cho dữ liệu chuỗi thời gian (như logs, telemetry từ xe fleet, giao dịch).
  - Cho phép Query Engine (Presto, Spark) thực hiện **Partition Pruning**: khi có `WHERE year=2023 AND month=10`, hệ thống chỉ liệt kê thư mục `year=2023/month=10/` và bỏ qua hàng ngàn thư mục khác, I/O giảm theo cấp số nhân.
- **Vấn đề Over-partitioning (Small Files Problem):**
  - Nếu chia quá nhỏ (ví dụ partition đến cấp `hour/minute` hoặc theo `customer_id` với hàng triệu KH), nó sẽ sinh ra hàng trăm ngàn file rất nhỏ (vài KB).
  - Làm cạn kiệt RAM của NameNode (mỗi file/block tốn 150 bytes).
  - Làm chậm trầm trọng tác vụ đọc vì overhead của việc open/close connection tới hàng ngàn file lớn hơn thời gian đọc I/O.
  - *Giải pháp:* Giữ dung lượng mỗi partition/file ở mức tối thiểu bằng Block size (128MB). Nếu dữ liệu ngày quá ít, chỉ partition theo month.

### Câu 56: Single Point of Failure của NameNode trong HDFS được giải quyết như thế nào (Kiến trúc HA)?
**Trả lời chuẩn Senior:**
HDFS High Availability (HA) giải quyết vấn đề bằng mô hình **Active/Standby NameNode**:
- **Hai NameNode:** 1 Active (phục vụ read/write) và 1 Standby (chờ sẵn sàng take over).
- **JournalNodes (Quorum):** Đảm bảo đồng bộ metadata. Mọi thay đổi không gian tên (edits log) từ Active NN được ghi vào đa số các JournalNodes (vd: 2/3). Standby NN liên tục đọc từ JournalNodes để áp dụng thay đổi, duy trì trạng thái đồng bộ với Active NN.
- **ZooKeeper Failover Controller (ZKFC):** Agent chạy trên mỗi NN, kết nối với ZooKeeper cluster. Nếu Active NN chết (mất heartbeat tới ZK), ZKFC sẽ tự động bầu chọn (elect) Standby NN lên làm Active (Automatic Failover).
- **Fencing:** Để tránh hội chứng "Split-Brain" (cả 2 NN đều tưởng mình đang Active và cùng ghi log), hệ thống sẽ dùng cơ chế fencing (vd cắt nguồn, revoke SSH) để cô lập NN cũ trước khi Standby NN lên Active.

### Câu 57: HDFS SafeMode là gì? Khi nào nó kích hoạt và làm sao để thoát khỏi SafeMode?
**Trả lời chuẩn Senior:**
- **SafeMode là gì?** Là trạng thái "chỉ đọc" (read-only) của HDFS. Không thể tạo, xóa, hay sửa block/file trong trạng thái này.
- **Kích hoạt khi nào:**
  1. Khi NameNode khởi động, nó tự động vào SafeMode để nạp FSImage từ disk vào RAM và chờ các DataNode gửi Block Reports.
  2. Nó tính toán tỷ lệ các blocks có sẵn. Chỉ khi nào tỷ lệ block an toàn (đạt số lượng replica tối thiểu) vượt qua ngưỡng cấu hình (thường là 99.9%), nó sẽ tự động thoát SafeMode sau vài chục giây.
  3. Kích hoạt thủ công bởi Administrator để bảo trì.
- **Nếu bị kẹt trong SafeMode (do mất DataNodes dẫn đến missing blocks):**
  - Kiểm tra cluster health và start lại các DataNode bị hỏng.
  - Nếu chấp nhận mất dữ liệu các block lỗi để hệ thống hoạt động lại, có thể force thoát bằng lệnh: `hdfs dfsadmin -safemode leave`.

### Câu 58: Thiết kế chỉ số Profit Margin (Biên lợi nhuận) trong DWH: Tính toán (Revenue - COGS) / Revenue. Nó là non-additive, giải quyết thế nào khi aggregate?
**Trả lời chuẩn Senior:**
Như đã nói ở Q46, `Profit Margin Ratio` là một **Non-additive measure**.
- Bạn KHÔNG THỂ lưu cột tỷ lệ `margin_ratio = 20%` ở dòng Hóa đơn A, `margin_ratio = 30%` ở dòng Hóa đơn B, rồi lấy tổng `SUM(margin_ratio)` khi gom nhóm theo Tháng. Điều đó sai hoàn toàn về mặt toán học.
- **Cách giải quyết:** 
  - Trong Fact table, lưu các thành phần cơ sở (Base/Additive measures): Doanh thu (`revenue`), Giá vốn (`cogs` - cost of goods sold).
  - Để tính tỷ lệ khi lên BI Tool hoặc ở tầng Data Mart / OLAP View, ta tạo ra một calculated metric / DAX measure:
    `Profit_Margin_% = (SUM(revenue) - SUM(cogs)) / SUM(revenue) * 100`
  - Đảm bảo việc chia (division) luôn diễn ra SAU KHI đã cộng gộp (aggregation) các trường tử số và mẫu số theo dimension được chọn.

### Câu 59: Slowly Changing Dimension Type 6 (Hybrid) là gì và ứng dụng khi nào?
**Trả lời chuẩn Senior:**
SCD Type 6 là sự kết hợp của Type 1 (ghi đè), Type 2 (lưu dòng lịch sử), và Type 3 (cột mới). (1+2+3 = 6).
- **Thiết kế:** Bảng Dim sẽ có dòng mới mỗi lần thay đổi (giống Type 2), nhưng đồng thời có thêm cột `current_state` (ví dụ `current_city`) và cột `historical_state` (`historical_city`). Khi có update, tạo dòng mới, và update lại cột `current_state` cho TOÀN BỘ các dòng lịch sử của ID đó (giống Type 1 overwrite).
- **Usecase:** Rất hiếm và phức tạp, nhưng cực kỳ hữu dụng khi người dùng muốn có khả năng phân tích báo cáo theo 2 góc nhìn ĐỒNG THỜI: 
  1. "Doanh thu phân bổ theo địa chỉ *tại thời điểm mua hàng* (historical)".
  2. "Doanh thu quá khứ gom nhóm theo địa chỉ *hiện tại* của khách hàng (current)".

### Câu 60: Tại sao Data Lineage và Metadata Management (vd: Apache Atlas, DataHub) lại thiết yếu cho Data Lakehouse cấp Enterprise?
**Trả lời chuẩn Senior:**
- **Data Lineage (Gia phả dữ liệu):** Theo dõi luồng dữ liệu từ nguồn (Source systems) qua các transformation pipelines (Airflow/Spark), tới các bảng DWH và cuối cùng là các BI Dashboard. 
  - *Root-cause Analysis:* Khi một dashboard bị sai số, Data Engineer có thể trace ngược lại xem bảng Fact nào bị lỗi, job Spark nào sinh ra nó, cột đó được map từ hệ thống Source nào.
  - *Impact Analysis:* Khi hệ thống Source (Backend) muốn đổi tên một cột, DE biết chính xác có bao nhiêu Downstream ETL jobs và Dashboards sẽ bị gãy để cảnh báo trước.
- **Metadata Management / Data Catalog:**
  - Tránh "Data Swamp": Cung cấp tính năng tìm kiếm (Google for Data) cho Data Analysts. Họ có thể tìm bảng `fact_parts_sales`, xem schema, ai là owner, định nghĩa kinh doanh của cột `revenue`, và data có được update đúng hạn không.
  - Đảm bảo tuân thủ bảo mật và PII (masking/governance).
