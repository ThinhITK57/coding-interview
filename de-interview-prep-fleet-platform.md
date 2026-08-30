# Data Engineer Interview Prep — Fleet Maintenance & Repair Data Platform

> Odoo trong tài liệu này được xem như **1 OLTP source system** (giống SAP/Salesforce) — không đi sâu vào cấu trúc module. Trọng tâm là dòng chảy dữ liệu và các quyết định kỹ thuật DE.

---

## 0. Mục tiêu kinh doanh (Business Goal — nói đầu tiên khi mở bài phỏng vấn)

Toàn bộ hệ thống được xây dựng để phục vụ **2 nguồn doanh thu chính**: **(1) bán dịch vụ sửa chữa/bảo dưỡng chăm sóc khách hàng sau bán xe**, và **(2) bán linh kiện thay thế**. Mọi quyết định kỹ thuật trong pipeline — từ việc thu thập telemetry, matching Head, đến cách mô hình hóa dữ liệu báo cáo — đều phục vụ mục tiêu này: giúp đúng khách hàng tìm đúng nơi sửa chữa nhanh nhất (tăng tỉ lệ chuyển đổi dịch vụ), và đo lường chính xác doanh thu/lợi nhuận từ 2 nguồn trên để ra quyết định kinh doanh.

---

## 1. Bối cảnh nghiệp vụ (business context — nói ngắn gọn khi mở đầu phỏng vấn)

Nền tảng bảo dưỡng/sửa chữa xe tải cho công ty logistics: xe tải gắn cảm biến theo dõi tuổi thọ linh kiện, chủ xe tìm trạm sửa chữa (Head) phù hợp theo linh kiện/giá/khoảng cách, đặt lịch, hệ thống vận hành nghiệp vụ sửa chữa trên Odoo, và toàn bộ dữ liệu giao dịch + vận hành + telemetry được tổng hợp thành báo cáo doanh thu/lợi nhuận/kho vận phục vụ ban lãnh đạo qua PowerBI.

---

## 2. Luồng dữ liệu tổng quát (data flow — đây là phần nên vẽ ra khi phỏng vấn)

**Bước 1 — Thu thập dữ liệu từ App (Ingestion)**
App trên xe tải gửi 2 luồng dữ liệu về hệ thống qua Kafka:
- Topic `truck-telemetry`: dữ liệu cảm biến tuổi thọ linh kiện
- Topic `repair-request`: yêu cầu sửa chữa chi tiết do chủ xe chủ động gửi

**Bước 2 — Stream Processing (ELT) bằng Spark**
Spark Structured Streaming consume 2 topic trên, thực hiện xử lý luồng theo mô hình ELT:
- **Extract:** đọc raw payload từ Kafka
- **Load:** ghi raw + dữ liệu đã chuẩn hóa xuống **HDFS (Data Lake)** để lưu trữ lịch sử, phục vụ phân tích/audit sau này
- **Transform:** bóc tách các trường cần thiết cho bước nghiệp vụ tiếp theo — cụ thể là **vị trí chủ xe** và **nội dung sửa chữa** — rồi đẩy kết quả này sang service tìm kiếm/matching Head phù hợp (linh kiện + giá + khoảng cách)

**Bước 3 — Matching Head (Redis, vai trò #1: Search/Serving Store, dữ liệu nguồn từ Odoo đồng bộ qua CDC)**
Odoo quản lý dữ liệu của **nhiều Head** — tồn kho linh kiện, giá, vị trí từng Head. Thay vì query trực tiếp Odoo mỗi lần chủ xe tìm kiếm (dễ làm quá tải DB vận hành), dữ liệu Head/tồn kho/giá được **đồng bộ liên tục qua Debezium CDC → Kafka topic `head-master-data-cdc` → sync vào Redis** (dùng Redis GEO cho truy vấn khoảng cách + Hash cho tồn kho/giá) làm lớp phục vụ tìm kiếm tốc độ cao. Service matching chỉ đọc từ Redis (không chạm Odoo), kết hợp với vị trí + nội dung sửa chữa đã trích xuất ở Bước 2 để trả về danh sách Head phù hợp.

**Bước 4 — Đặt lịch (Redis, vai trò #2: Concurrency Control)**
Sau khi có Head phù hợp, chủ xe chọn Head và đặt lịch hẹn. Vì nhiều chủ xe có thể cùng chọn 1 Head/khung giờ cùng lúc → dùng Redis atomic reservation (cùng pattern với bài toán đặt vé máy bay: giữ chỗ có TTL, chống overbooking) để đảm bảo tính nhất quán khi đặt lịch.

**Bước 5 — Giao dịch nghiệp vụ trong Odoo (Source System)**
Sau khi đặt lịch thành công, toàn bộ nghiệp vụ sửa chữa (lịch hẹn, work order, xuất kho linh kiện, hóa đơn) chạy trong Odoo — đây là hệ thống OLTP nguồn, không cần trình bày chi tiết module khi phỏng vấn DE.

**Bước 6 — Change Data Capture (Debezium)**
Debezium gắn vào Postgres (DB của Odoo) theo cơ chế log-based CDC (đọc WAL), bắt các thay đổi trên bảng giao dịch (đơn sửa chữa, tồn kho, khách hàng...) → publish vào Kafka CDC topics — **không query trực tiếp vào Odoo DB** để tránh ảnh hưởng hệ thống vận hành. Đây là **cùng 1 cơ chế CDC** đang phục vụ song song 2 mục đích: đồng bộ dữ liệu Head cho matching (Bước 3, gần real-time) và cấp dữ liệu cho báo cáo (Bước 7, batch) — 1 nguồn CDC, nhiều pattern tiêu thụ khác nhau.

**Bước 7 — Batch Processing & Data Modeling (Spark + Airflow)**
Airflow điều phối các DAG batch job định kỳ: Spark đọc dữ liệu từ HDFS (raw telemetry đã lưu ở Bước 2) + Kafka CDC topics (giao dịch từ Odoo) → làm sạch, join, tổng hợp thành các bảng fact/dimension chuẩn hóa (doanh thu dịch vụ sửa chữa, doanh thu bán linh kiện, tồn kho, khách hàng — bao gồm xử lý khách hàng mới/cũ/thường niên theo mô hình SCD).

**Bước 8 — Serving Layer cho BI (Redis, vai trò #3: Cache/Notify Layer + Backend Layer trung gian — CONFIRMED)**
Airflow điều phối Spark job tổng hợp (theo các tần suất tuần/tháng/quý/năm tài khóa/cuối năm — xem mục 4.5b) và đẩy kết quả vào Redis. Giữa Redis và PowerBI có **1 lớp backend riêng**, đóng 2 vai trò:
- Subscribe vào Redis (pub/sub hoặc keyspace notification) để biết ngay khi có dữ liệu tổng hợp mới.
- Duy trì kết nối **WebSocket** tới dashboard: khi Redis nhận dữ liệu mới, backend chủ động **đẩy (push)** dữ liệu xuống dashboard, thay vì dashboard phải chủ động request/query lại dữ liệu mỗi lần muốn refresh.

**Bước 9 — Consumption**
Dashboard nhận dữ liệu real-time qua WebSocket từ lớp backend, hiển thị báo cáo doanh thu (dịch vụ sửa chữa + bán linh kiện), tỉ suất lợi nhuận theo nhiều mốc thời gian, báo cáo kho vận cho ban lãnh đạo — không cần chờ dashboard tự poll dữ liệu.

---

## 3. Bảng stack công nghệ — vai trò & lý do chọn (chỉ giữ phần core DE)

| Công nghệ | Vai trò trong pipeline | Vì sao chọn |
|---|---|---|
| **Kafka** | Message bus trung tâm: nhận telemetry, repair-request, và CDC events từ Debezium | Throughput cao, decoupling giữa producer (app, Odoo) và consumer (Spark), cho phép nhiều consumer đọc độc lập (Spark streaming, các service khác) mà không ảnh hưởng nhau |
| **Debezium** | Log-based CDC, đọc trực tiếp write-ahead log của Postgres để bắt thay đổi | Không cần query polling vào Odoo DB (tránh tăng tải hệ thống vận hành), đảm bảo capture đầy đủ mọi thay đổi kể cả update/delete, độ trễ thấp gần real-time |
| **Spark (Structured Streaming + Batch)** | Streaming: xử lý ELT ngay khi dữ liệu vào Kafka (Bước 2). Batch: tổng hợp fact/dimension từ HDFS + CDC topics (Bước 6), điều phối bởi Airflow | Một engine xử lý được cả 2 mô hình (streaming + batch) giúp giảm số lượng công nghệ cần vận hành song song; scale ngang tốt cho khối lượng dữ liệu lớn |
| **HDFS (Hadoop)** | Data Lake — lưu raw + curated data dưới dạng **Parquet** (CONFIRMED, nhờ cơ chế lưu theo cột) | Columnar storage giúp Spark chỉ đọc đúng cột cần cho aggregation (thay vì scan cả row), nén tốt hơn, chi phí lưu trữ thấp cho dữ liệu khối lượng lớn, dài hạn (telemetry lịch sử) |
| **Postgres** | DB vận hành của Odoo — nguồn duy nhất cho Debezium CDC | Đây là DB có sẵn của hệ thống nghiệp vụ, không phải lựa chọn của tầng DE — DE chỉ tiêu thụ thay đổi từ đây |
| **Airflow** | Điều phối các DAG batch: tổng hợp báo cáo (weekly/monthly/quarterly/theo năm tài khóa/cuối năm — CONFIRMED), xử lý SCD khách hàng, đảm bảo thứ tự phụ thuộc giữa các job | Quản lý dependency rõ ràng (job tổng hợp doanh thu phải chạy sau khi CDC + telemetry đã sẵn sàng), retry/alerting khi job lỗi, nhiều lịch chạy song song ứng với từng loại báo cáo |
| **Redis** | (1) Serving store cho tìm kiếm/matching Head — dữ liệu đồng bộ liên tục từ Odoo qua CDC (GEO cho khoảng cách, Hash cho tồn kho/giá); (2) Concurrency control cho đặt lịch (atomic reservation, TTL); (3) Cache/notify layer — nhận dữ liệu tổng hợp từ Spark, phát tín hiệu (pub/sub) cho backend layer khi có dữ liệu mới | (1) Đọc cực nhanh phục vụ tìm kiếm real-time mà không chạm vào Odoo DB; (2) Single-thread atomic phù hợp bài toán tranh chấp giữ chỗ; (3) Tách vai trò lưu trữ dữ liệu mới nhất khỏi việc đẩy dữ liệu đi — backend layer mới là nơi chủ động push |
| **Backend layer (WebSocket) — CONFIRMED** | Đứng giữa Redis và dashboard: subscribe Redis, khi có dữ liệu mới thì chủ động push xuống dashboard qua WebSocket | PowerBI không có native connector cho Redis; lớp backend này giải quyết đồng thời 2 việc — (a) là cầu nối duy nhất giữa Redis và tầng hiển thị, (b) đổi mô hình pull (dashboard tự query) sang push (server đẩy khi có dữ liệu mới), giảm độ trễ hiển thị và giảm tải query lặp lại từ nhiều dashboard |
| **PowerBI / Dashboard** | Tầng consumption/reporting cuối, nhận dữ liệu qua WebSocket từ backend layer | Công cụ BI chuẩn doanh nghiệp cho ban lãnh đạo; dữ liệu tới theo cơ chế push nên dashboard luôn hiển thị số liệu mới nhất mà không cần tự refresh/query |
| **Python / SQL** | Ngôn ngữ viết Spark job, Airflow DAG, transform logic | Chuẩn công nghiệp cho hệ sinh thái Spark/Airflow, SQL dùng cho các bước transform/aggregation trong Spark SQL |

---

## 4. Các chủ đề kỹ thuật DE cần nắm chắc — kèm hướng trả lời

### 4.1 CDC & Log-based Change Capture
- **Câu hỏi khả năng cao:** "Vì sao dùng Debezium thay vì query định kỳ (polling) vào Postgres?"
- **Ý cần có trong câu trả lời:** Polling tạo tải lặp lại lên DB nguồn, có độ trễ theo chu kỳ poll, và dễ bỏ sót các thay đổi trung gian (update rồi update lại trong cùng chu kỳ). Debezium đọc trực tiếp WAL (Write-Ahead Log) của Postgres — capture được toàn bộ sự kiện insert/update/delete theo đúng thứ tự, gần như không tải thêm lên DB nguồn ngoài việc đọc log.
- **Câu hỏi vặn tiếp theo cần chuẩn bị:** "Nếu Debezium bị downtime, dữ liệu có bị mất không?" → Debezium lưu offset đã đọc trong Kafka Connect, khi restart sẽ tiếp tục từ offset cuối, miễn WAL chưa bị Postgres dọn (cần cấu hình `wal_keep_size`/replication slot đủ lớn để tránh mất log trước khi Debezium kịp đọc).

### 4.2 Batch vs Streaming — vì sao dùng cả hai (Lambda-style)
- **Streaming (Spark Structured Streaming):** dùng cho luồng ELT ngay khi telemetry/repair-request vào Kafka — cần xử lý gần real-time để phục vụ tìm kiếm Head kịp thời.
- **Batch (Spark batch job qua Airflow):** dùng cho tổng hợp báo cáo — không cần độ trễ thấp, ưu tiên xử lý khối lượng lớn, join nhiều nguồn (CDC + Data Lake) hiệu quả hơn theo lô.
- **Điểm mấu chốt khi trả lời:** không phải "dùng Spark vì nó mạnh", mà là **chọn đúng mô hình xử lý theo yêu cầu độ trễ của từng bài toán cụ thể** — đây là câu trả lời interviewer muốn nghe.

### 4.3 Slowly Changing Dimension (SCD) — khách hàng mới/cũ/thường niên
- Đây là bài toán kinh điển của Data Warehouse modeling, rất đáng để chuẩn bị kỹ vì đề bài của bạn có nhắc trực tiếp ("khách hàng mới, cũ, thường niên, chiến dịch giảm giá áp dụng đúng khách hàng đã đăng ký").
- **SCD Type 2** phù hợp nhất: mỗi khi trạng thái khách hàng thay đổi (từ "mới" → "thường niên", hoặc tham gia chiến dịch mới), tạo 1 bản ghi mới trong bảng dimension với `effective_date`/`end_date`, giữ lại lịch sử thay vì ghi đè — đảm bảo báo cáo doanh thu theo từng giai đoạn phản ánh đúng trạng thái khách hàng tại thời điểm đó.
- **Câu hỏi khả năng cao:** "Sao không update trực tiếp field trạng thái khách hàng?" → Vì sẽ làm sai lệch báo cáo lịch sử (vd: báo cáo doanh thu quý trước sẽ hiển thị sai nếu khách hàng đã đổi hạng ở quý này mà ghi đè lên bản ghi cũ).

### 4.4 Idempotency & At-least-once delivery
- Kafka/Debezium mặc định đảm bảo **at-least-once** (có thể nhận trùng message khi retry), không phải exactly-once.
- **Câu hỏi khả năng cao:** "Tính doanh thu có bị đếm trùng không nếu Kafka gửi lặp?"
- **Hướng trả lời:** Dùng khóa duy nhất từ CDC event (vd: `(table, primary_key, transaction_lsn)` của Debezium) làm idempotency key khi Spark ghi vào bảng tổng hợp — dùng `MERGE`/`upsert` thay vì `INSERT` thuần khi ghi kết quả batch.

### 4.5 Data Consistency & độ trễ chấp nhận được
- **Câu hỏi khả năng cao:** "Độ trễ giữa lúc giao dịch xảy ra trong Odoo và lúc xuất hiện trong báo cáo PowerBI là bao lâu?"
- **Hướng trả lời:** Phân biệt rõ 2 luồng — luồng streaming (telemetry/repair-request) cần độ trễ thấp (giây/phút) vì ảnh hưởng trực tiếp trải nghiệm tìm Head; luồng batch báo cáo tài chính chấp nhận độ trễ cao hơn, vì bản chất báo cáo doanh thu/lợi nhuận không cần real-time tuyệt đối, quan trọng là **đúng và đầy đủ** hơn là **nhanh**.

**4.5b Tần suất báo cáo tỉ suất lợi nhuận (CONFIRMED)**
Không chỉ 1 DAG "thường niên" — Airflow chạy nhiều DAG batch với tần suất khác nhau, mỗi DAG ứng với 1 mốc báo cáo tỉ suất lợi nhuận:
- Theo **tuần** (weekly)
- Theo **tháng** (monthly)
- Theo **quý** (quarterly)
- Theo **năm tài khóa** (fiscal year)
- **Cuối năm** (year-end)

Mỗi tần suất là 1 DAG/schedule riêng trong Airflow, cùng đọc từ fact/dimension đã chuẩn hóa (Bước 7) nhưng khác nhau ở window aggregation (rolling week/month/quarter/fiscal-year) — giúp ban lãnh đạo vừa theo dõi vận hành ngắn hạn (tuần/tháng) vừa có góc nhìn dài hạn (quý/năm tài khóa) từ cùng 1 nguồn dữ liệu.

### 4.6 Airflow DAG Dependency Design
- **Câu hỏi khả năng cao:** "DAG tổng hợp báo cáo doanh thu phụ thuộc vào những gì, xử lý sao nếu 1 nguồn chưa sẵn sàng?"
- **Hướng trả lời:** DAG tổng hợp cần đợi cả 2 nhánh — CDC data (giao dịch từ Odoo) và telemetry data (đã ELT xong trên HDFS) — dùng sensor/dependency task (`ExternalTaskSensor` hoặc kiểm tra file/partition đã sẵn sàng trên HDFS) trước khi chạy bước aggregation, tránh tổng hợp thiếu dữ liệu.

### 4.7 Data Modeling cho tầng Serving (Star Schema)
- Đề xuất mô hình fact/dimension cho mart phục vụ PowerBI, bám sát 2 nguồn doanh thu cốt lõi:
  - **Fact tables:** `fact_repair_service_revenue` (doanh thu từ dịch vụ sửa chữa/bảo dưỡng), `fact_parts_sales` (doanh thu bán linh kiện), `fact_inventory_movement` (xuất/nhập kho)
  - **Dimension tables:** `dim_customer` (SCD Type 2), `dim_head` (thông tin trạm sửa chữa), `dim_component` (linh kiện), `dim_date`
- **Vì sao dùng star schema:** tối ưu cho truy vấn BI (join ít bảng, dễ hiểu cho người làm báo cáo), phù hợp với cách PowerBI xây dựng model quan hệ; tách riêng 2 fact table theo 2 nguồn doanh thu giúp báo cáo phân tích được đâu là động lực tăng trưởng chính (dịch vụ hay bán linh kiện) thay vì gộp chung 1 con số doanh thu duy nhất.

### 4.8 Một nguồn CDC, nhiều mô hình tiêu thụ (Multi-consumer CDC pattern)
- **Điểm đáng nhấn mạnh khi phỏng vấn:** Cùng 1 luồng Debezium CDC từ Odoo được **2 hệ thống hạ nguồn tiêu thụ theo 2 cách khác nhau**: (1) sync gần real-time vào Redis để phục vụ matching Head (yêu cầu độ trễ thấp, chỉ cần dữ liệu mới nhất, không cần lưu lịch sử), và (2) batch load vào HDFS/Spark để phục vụ báo cáo (yêu cầu đầy đủ lịch sử, không cần độ trễ thấp nhưng cần chính xác tuyệt đối).
- **Câu hỏi khả năng cao:** "Vì sao không tách riêng 2 pipeline CDC cho 2 mục đích?"
- **Hướng trả lời:** Tách riêng CDC theo từng use-case sẽ nhân đôi tải đọc lên Postgres/WAL và khó đảm bảo 2 nguồn dữ liệu nhất quán với nhau theo thời gian. Dùng chung 1 Kafka CDC topic, để mỗi consumer (Redis sync job và Spark batch job) tự quyết định cách xử lý theo nhu cầu riêng — đây là lợi ích cốt lõi của kiến trúc pub-sub: 1 producer, nhiều consumer độc lập.

### 4.9 Push-based Serving Layer (Redis → Backend → WebSocket) — CONFIRMED
- **Câu hỏi khả năng cao:** "Vì sao không để PowerBI/dashboard tự query Redis mỗi lần cần refresh?"
- **Hướng trả lời:** Mô hình pull (dashboard tự request) có 2 vấn đề: (1) dữ liệu chỉ mới tại thời điểm query, giữa 2 lần refresh vẫn có độ trễ hiển thị; (2) nhiều dashboard cùng poll liên tục tạo tải lặp lại không cần thiết lên Redis. Thay vào đó, dùng mô hình **push**: 1 lớp backend đứng giữa Redis và dashboard, subscribe vào Redis (pub/sub) để biết ngay khi Spark/Airflow ghi xong dữ liệu tổng hợp mới; backend này giữ kết nối **WebSocket** với client và chủ động đẩy dữ liệu xuống ngay khi có, thay vì đợi client hỏi.
- **Lợi ích:** giảm độ trễ hiển thị (near real-time thay vì "làm mới theo chu kỳ"), giảm số lượng query trùng lặp lên Redis khi có nhiều dashboard/nhiều người xem cùng lúc, và tách rời rõ ràng vai trò: Redis chỉ lưu trạng thái mới nhất, backend layer chịu trách nhiệm phân phối.
- **Câu hỏi vặn tiếp theo cần chuẩn bị:** "Nếu dashboard mất kết nối WebSocket giữa chừng thì sao?" → Cần cơ chế reconnect + khi client kết nối lại, backend đọc lại state hiện tại từ Redis để đồng bộ ngay (không chỉ chờ event mới), tránh dashboard hiển thị dữ liệu cũ sau khi rớt mạng.

---

## 5. Trạng thái các điểm xác nhận (đã cập nhật)

**Đã xác nhận (CONFIRMED — đã đưa vào flow ở trên):**
- **Định dạng lưu trên HDFS:** Parquet, nhờ cơ chế lưu theo cột (columnar) — xem bảng stack mục 3 và Bước 2.
- **Tần suất báo cáo:** không chỉ thường niên — có các DAG theo tuần, tháng, quý, năm tài khóa, và cuối năm cho báo cáo tỉ suất lợi nhuận — xem mục 4.5b.
- **Cơ chế Redis → Dashboard:** có 1 lớp backend đứng giữa, đóng vai trò WebSocket server; khi Redis nhận dữ liệu tổng hợp mới (từ Spark + Airflow job), backend chủ động push xuống dashboard thay vì dashboard phải tự query — xem Bước 8-9 và mục 4.9.

**Còn cần xác nhận thêm:**
1. **Postgres:** vẫn đang giả định Postgres chỉ là DB vận hành của Odoo (nguồn CDC), không có thêm một Postgres/DWH riêng cho tầng phân tích trước khi Spark ghi vào Redis. Nếu thực tế có thêm 1 Postgres analytics/mart riêng ở giữa, cần bổ sung vào flow ở Bước 7-8.
