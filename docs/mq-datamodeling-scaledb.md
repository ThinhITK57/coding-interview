# 30 Khái Niệm Backend / Data Engineering — Message Queue, Data Modeling & Scale Database

> Mỗi mục theo format: **Bản chất → Ví dụ thực tế → Cách hoạt động → Khi nào dùng → Câu hỏi phỏng vấn**

---

# Phần 1: Message Queue (10 khái niệm)

## 1. Producer / Consumer
**Bản chất:** Producer là bên tạo/gửi message, Consumer là bên đọc/xử lý message. Chúng không gọi trực tiếp nhau — giao tiếp qua queue/topic, tách rời về thời gian (decoupling).

**Ví dụ:** Service đặt hàng (producer) publish event `OrderCreated` lên Kafka; service kho hàng, service gửi email, service thống kê (nhiều consumer độc lập) cùng đọc event đó.

**Cách hoạt động:** Producer gọi API `send()`/`publish()` ghi message vào broker (Kafka topic, RabbitMQ exchange...); Consumer chủ động `poll()` (Kafka) hoặc được broker `push` (RabbitMQ) message về xử lý.

**Khi nào dùng:** Khi cần tách rời hai service về thời gian xử lý và về khả năng chịu lỗi — producer không cần biết consumer có đang sống hay không.

**Câu hỏi phỏng vấn:** "Producer có cần biết consumer đang online không? Điều này ảnh hưởng gì đến thiết kế hệ thống?"

---

## 2. Topic + Partition
**Bản chất:** Topic là "kênh" logic chứa message cùng loại; Partition là cách chia nhỏ topic thành nhiều phần song song để tăng throughput và cho phép nhiều consumer xử lý đồng thời.

**Ví dụ:** Topic `user-events` chia thành 6 partition; message của cùng 1 `user_id` luôn rơi vào cùng 1 partition (dựa trên hash key) để đảm bảo thứ tự theo user.

**Cách hoạt động:** Producer chọn partition bằng cách hash key (hoặc round-robin nếu không có key). Mỗi partition là 1 log tuần tự, append-only, có thể nằm trên broker khác nhau.

**Khi nào dùng:** Cần tăng throughput ngang (horizontal scale) và cần đảm bảo ordering trong phạm vi 1 key cụ thể.

**Câu hỏi phỏng vấn:** "Tăng số partition sau khi hệ thống đã chạy có rủi ro gì?" (gợi ý: phá vỡ mapping key→partition cũ, ảnh hưởng ordering).

---

## 3. Consumer Group
**Bản chất:** Nhóm các consumer instance cùng chia nhau đọc một topic — mỗi partition chỉ được đọc bởi đúng 1 consumer trong group tại một thời điểm, giúp scale việc xử lý song song mà không đọc trùng.

**Ví dụ:** Topic có 6 partition, consumer group có 3 instance → mỗi instance đảm nhiệm 2 partition. Nếu 2 group khác nhau cùng đọc 1 topic, mỗi group nhận đủ toàn bộ message độc lập (pub/sub thật sự giữa các group).

**Cách hoạt động:** Kafka dùng cơ chế `rebalance` để phân chia lại partition khi consumer join/leave group; group coordinator theo dõi qua heartbeat.

**Khi nào dùng:** Khi cần scale consumer ngang, hoặc cần nhiều hệ thống độc lập cùng nhận toàn bộ 1 luồng event (mỗi hệ thống = 1 group riêng).

**Câu hỏi phỏng vấn:** "Điều gì xảy ra nếu số consumer trong group nhiều hơn số partition?" (dư consumer sẽ idle, không nhận được partition nào).

---

## 4. At-Least-Once
**Bản chất:** Một trong 3 mức đảm bảo delivery (at-most-once, at-least-once, exactly-once). At-least-once nghĩa là message chắc chắn được xử lý ít nhất 1 lần, nhưng có thể bị xử lý trùng lặp nếu consumer crash sau khi xử lý nhưng trước khi commit offset.

**Ví dụ:** Consumer xử lý xong message (đã ghi DB), nhưng crash trước khi commit offset → khi restart, Kafka gửi lại đúng message đó → DB bị ghi 2 lần nếu không có cơ chế chống trùng.

**Cách hoạt động:** Commit offset sau khi xử lý xong (không phải trước) để tránh mất message; đánh đổi là chấp nhận khả năng trùng.

**Khi nào dùng:** Hầu hết hệ thống thực tế (mặc định của Kafka) vì exactly-once tốn chi phí lớn; kết hợp với idempotent consumer (mục 9) để xử lý an toàn.

**Câu hỏi phỏng vấn:** "So sánh at-most-once, at-least-once, exactly-once — đánh đổi (trade-off) của từng loại là gì?"

---

## 5. Dead Letter Queue (DLQ)
**Bản chất:** Hàng đợi riêng chứa các message xử lý thất bại nhiều lần liên tiếp, để không làm nghẽn queue chính và cho phép debug/reprocess sau.

**Ví dụ:** Message có format JSON lỗi khiến consumer luôn throw exception → sau 3 lần retry thất bại, message được đẩy sang `orders-dlq` thay vì retry vô hạn (block toàn bộ partition phía sau nó).

**Cách hoạt động:** Consumer bắt exception, đếm số lần retry (lưu trong header message hoặc external store), khi vượt ngưỡng thì publish message đó sang topic/queue DLQ kèm lý do lỗi.

**Khi nào dùng:** Bất kỳ hệ thống production nào xử lý message bất đồng bộ — tránh 1 message lỗi (poison message) làm treo cả pipeline.

**Câu hỏi phỏng vấn:** "Nếu không có DLQ, 1 message lỗi vĩnh viễn (poison message) ảnh hưởng gì đến các message phía sau trong cùng partition?"

---

## 6. Backpressure
**Bản chất:** *(Xem chi tiết ở tài liệu Retry/Eviction/Backpressure trước đó)* Trong ngữ cảnh MQ, đây là cơ chế để consumer báo hiệu tốc độ xử lý của mình, tránh producer bắn dữ liệu nhanh hơn khả năng tiêu thụ.

**Ví dụ:** Kafka consumer tự nhiên tạo backpressure bằng cách `poll()` chậm lại khi đang bận xử lý — dữ liệu vẫn an toàn trên broker (disk), thể hiện qua consumer lag tăng.

**Cách hoạt động:** Pull-based (Kafka) hoặc prefetch limit (RabbitMQ `prefetch_count`) — consumer kiểm soát nhịp độ nhận thay vì bị broker dồn ép.

**Khi nào dùng:** Khi tốc độ producer và consumer lệch nhau không cố định (traffic spike).

**Câu hỏi phỏng vấn:** "Vì sao Kafka được coi là có backpressure tự nhiên hơn các MQ dạng push?"

---

## 7. Fan-Out
**Bản chất:** Một message được gửi đến nhiều consumer/queue độc lập để mỗi bên xử lý một nghiệp vụ khác nhau song song, thay vì 1 message chỉ được 1 consumer xử lý.

**Ví dụ:** Event `PaymentCompleted` fan-out tới 3 nơi: service gửi hóa đơn, service cập nhật loyalty points, service gửi notification — cả 3 chạy độc lập, không phụ thuộc nhau.

**Cách hoạt động:** Kafka: nhiều consumer group khác nhau cùng subscribe 1 topic → tự động fan-out. RabbitMQ: dùng `fanout exchange` để copy message sang nhiều queue.

**Khi nào dùng:** Khi 1 sự kiện nghiệp vụ cần kích hoạt nhiều luồng xử lý độc lập, không muốn service gốc phải biết/gọi trực tiếp từng service kia (giảm coupling).

**Câu hỏi phỏng vấn:** "Fan-out qua message queue khác gì so với việc service A gọi trực tiếp (đồng bộ) 3 service B, C, D?"

---

## 8. Message Ordering
**Bản chất:** Đảm bảo message được xử lý đúng theo thứ tự chúng được gửi — nhưng ordering chỉ được đảm bảo trong phạm vi 1 partition, không đảm bảo giữa các partition khác nhau.

**Ví dụ:** Các event `OrderCreated` → `OrderPaid` → `OrderShipped` của cùng 1 đơn hàng phải được xử lý đúng thứ tự; nếu dùng `order_id` làm partition key, cả 3 event luôn nằm cùng 1 partition → giữ đúng thứ tự.

**Cách hoạt động:** Producer dùng key nhất quán để hash vào cùng partition; consumer trong partition đó xử lý tuần tự (single-threaded per partition).

**Khi nào dùng:** Nghiệp vụ có state machine phụ thuộc thứ tự (đơn hàng, giao dịch tài chính, session log).

**Câu hỏi phỏng vấn:** "Tăng số consumer thread trong cùng 1 partition có giữ được ordering không? Vì sao?"

---

## 9. Idempotent Consumer
**Bản chất:** Consumer được thiết kế để xử lý cùng 1 message nhiều lần vẫn cho kết quả giống hệt xử lý 1 lần — bù trừ cho at-least-once delivery (mục 4) vốn có thể gửi trùng.

**Ví dụ:** Thay vì `UPDATE balance = balance + 100` (chạy 2 lần sẽ cộng nhầm 200), dùng `UPDATE balance = 100 WHERE tx_id = 'X' AND processed = false` kèm bảng lưu `tx_id` đã xử lý (dedup table) hoặc unique constraint.

**Cách hoạt động:** Lưu `message_id`/`idempotency_key` đã xử lý (DB, Redis) — trước khi xử lý, kiểm tra key đã tồn tại chưa; hoặc dùng thao tác tự nhiên idempotent (SET thay vì INCREMENT).

**Khi nào dùng:** Bắt buộc trong mọi hệ thống dùng at-least-once delivery, đặc biệt nghiệp vụ tài chính, đơn hàng.

**Câu hỏi phỏng vấn:** "Cho ví dụ một thao tác tự nhiên đã idempotent và một thao tác không idempotent, cách chuyển thao tác sau thành idempotent."

---

## 10. Offset Commit
**Bản chất:** Offset là vị trí (con trỏ) đánh dấu message cuối cùng consumer đã xử lý trong 1 partition. Commit offset là hành động lưu lại vị trí đó để nếu consumer restart, nó biết tiếp tục từ đâu.

**Ví dụ:** Consumer xử lý xong message ở offset 105 → commit offset = 106 (vị trí tiếp theo cần đọc). Nếu crash trước khi commit, khi restart sẽ đọc lại từ offset đã commit gần nhất (vd 100) → xử lý lại message 100-105 → đây chính là nguồn gốc của at-least-once.

**Cách hoạt động:** Auto-commit (theo interval, rủi ro mất/trùng) hoặc manual commit (`commitSync`/`commitAsync` — commit sau khi chắc chắn xử lý xong, an toàn hơn).

**Khi nào dùng:** Manual commit khi cần kiểm soát chặt at-least-once; auto-commit khi chấp nhận rủi ro nhỏ để đơn giản hóa code.

**Câu hỏi phỏng vấn:** "Commit offset trước khi xử lý message và sau khi xử lý message khác nhau thế nào về rủi ro mất dữ liệu?" (trước: at-most-once, dễ mất; sau: at-least-once, dễ trùng).

---

# Phần 2: Data Modeling (10 khái niệm)

## 1. Primary Key vs Foreign Key
**Bản chất:** Primary Key (PK) định danh duy nhất 1 record trong bảng. Foreign Key (FK) là cột tham chiếu đến PK của bảng khác, thể hiện mối quan hệ giữa 2 bảng.

**Ví dụ:** Bảng `orders.customer_id` (FK) trỏ đến `customers.id` (PK) — đảm bảo mỗi order luôn gắn với 1 customer có thật.

**Cách hoạt động:** DB dùng index tự động trên PK; FK được enforce qua constraint (referential integrity) — insert/update vi phạm sẽ bị DB từ chối, delete PK đang bị tham chiếu sẽ lỗi trừ khi có `ON DELETE CASCADE`.

**Khi nào dùng:** Luôn luôn có PK cho mọi bảng; dùng FK khi cần DB tự đảm bảo tính toàn vẹn tham chiếu thay vì để application tự kiểm tra.

**Câu hỏi phỏng vấn:** "Vì sao nhiều hệ thống lớn (microservices) lại tránh dùng FK giữa các bảng thuộc service khác nhau?"

---

## 2. One-to-Many
**Bản chất:** Một record ở bảng A có thể liên kết với nhiều record ở bảng B, nhưng mỗi record B chỉ thuộc về đúng 1 record A.

**Ví dụ:** 1 `customer` có nhiều `orders`, nhưng mỗi `order` chỉ thuộc về 1 `customer`.

**Cách hoạt động:** Đặt FK ở bảng "nhiều" (orders.customer_id), không cần bảng trung gian.

**Khi nào dùng:** Đây là quan hệ phổ biến nhất trong mô hình dữ liệu quan hệ — cha/con, chủ sở hữu/tài sản.

**Câu hỏi phỏng vấn:** "Làm sao query hiệu quả 'lấy customer kèm toàn bộ order' mà không bị N+1 query?"

---

## 3. Many-to-Many
**Bản chất:** Nhiều record ở bảng A có thể liên kết với nhiều record ở bảng B và ngược lại, cần một bảng trung gian (junction/pivot table) để biểu diễn.

**Ví dụ:** 1 `student` học nhiều `course`, 1 `course` có nhiều `student` → bảng trung gian `enrollments(student_id, course_id)`.

**Cách hoạt động:** Bảng trung gian chứa 2 FK trỏ về 2 bảng gốc, thường composite key (student_id, course_id) là PK của chính nó; có thể thêm cột dữ liệu riêng của quan hệ (vd `enrolled_at`, `grade`).

**Khi nào dùng:** Bất kỳ khi nào 2 thực thể có quan hệ "nhiều-nhiều" tự nhiên trong nghiệp vụ.

**Câu hỏi phỏng vấn:** "Bảng trung gian many-to-many có nên có PK riêng (auto-increment id) hay dùng composite key? Trade-off là gì?"

---

## 4. Composite Key
**Bản chất:** Khóa chính được tạo từ 2 hoặc nhiều cột kết hợp thay vì 1 cột đơn, dùng khi không có cột đơn nào tự nhiên đủ để định danh duy nhất 1 record.

**Ví dụ:** Bảng `order_items(order_id, product_id, quantity)` — cặp `(order_id, product_id)` làm composite PK vì 1 order không thể có 2 dòng cùng 1 product.

**Cách hoạt động:** DB tạo index kết hợp trên nhiều cột theo đúng thứ tự khai báo — thứ tự cột trong composite key ảnh hưởng đến hiệu quả query (cột đứng trước filter hiệu quả hơn).

**Khi nào dùng:** Bảng trung gian many-to-many (mục 3), hoặc dữ liệu time-series theo (device_id, timestamp).

**Câu hỏi phỏng vấn:** "Composite key và surrogate key (id auto-increment riêng) — khi nào chọn cái nào?"

---

## 5. Soft Delete
**Bản chất:** Thay vì xóa vật lý record khỏi DB (`DELETE`), đánh dấu record là "đã xóa" bằng 1 cột (`deleted_at`, `is_deleted`) — dữ liệu vẫn còn trong DB nhưng bị ẩn khỏi truy vấn thông thường.

**Ví dụ:** User "xóa" bài viết → chỉ set `deleted_at = NOW()`; mọi query mặc định thêm điều kiện `WHERE deleted_at IS NULL`.

**Cách hoạt động:** Cần discipline ở tầng application/ORM để luôn filter `deleted_at IS NULL`, hoặc dùng view/scope mặc định; cần cân nhắc unique constraint (vd email) phải loại trừ record đã soft-delete.

**Khi nào dùng:** Khi cần khả năng khôi phục, audit trail, hoặc tuân thủ quy định giữ dữ liệu; tránh dùng tràn lan vì làm phình bảng và phức tạp hóa mọi query.

**Câu hỏi phỏng vấn:** "Soft delete ảnh hưởng gì đến unique constraint và hiệu năng index theo thời gian?"

---

## 6. Polymorphic Associations
**Bản chất:** Một bảng có thể liên kết (FK) tới nhiều loại bảng khác nhau thông qua 1 cặp cột `(xxx_id, xxx_type)` thay vì FK cứng tới 1 bảng cụ thể.

**Ví dụ:** Bảng `comments(commentable_id, commentable_type)` có thể gắn vào `posts` hoặc `photos` — `commentable_type = 'Post'` hoặc `'Photo'`.

**Cách hoạt động:** `commentable_type` lưu tên bảng/model, `commentable_id` là id trong bảng đó — application tự resolve join thay vì DB enforce qua FK constraint thật (vì FK không thể trỏ động tới nhiều bảng).

**Khi nào dùng:** Khi 1 entity (comment, tag, attachment) cần gắn linh hoạt với nhiều loại entity khác — nhưng đánh đổi mất tính toàn vẹn tham chiếu từ DB.

**Câu hỏi phỏng vấn:** "Vì sao polymorphic association không thể có FK constraint thật? Rủi ro dữ liệu mồ côi (orphan record) xử lý thế nào?"

---

## 7. Enum vs Lookup Table
**Bản chất:** Hai cách biểu diễn tập giá trị cố định (vd trạng thái đơn hàng: pending/paid/shipped). Enum là kiểu dữ liệu cố định trong schema; Lookup table là 1 bảng riêng chứa danh sách giá trị hợp lệ, tham chiếu qua FK.

**Ví dụ:** `status ENUM('pending','paid','shipped')` (enum) vs `status_id FK → order_statuses(id, name)` (lookup table).

**Cách hoạt động:** Enum: DB validate trực tiếp ở tầng kiểu dữ liệu, thay đổi giá trị enum cần `ALTER TABLE` (có thể tốn kém trên bảng lớn ở một số DB). Lookup table: linh hoạt hơn, thêm giá trị mới chỉ cần `INSERT`, có thể lưu thêm metadata (màu sắc, thứ tự hiển thị).

**Khi nào dùng:** Enum khi tập giá trị thực sự ổn định, ít thay đổi, cần hiệu năng cao. Lookup table khi giá trị có thể mở rộng, cần metadata đi kèm, hoặc cần đa ngôn ngữ.

**Câu hỏi phỏng vấn:** "Thêm 1 giá trị mới vào enum trên bảng có hàng trăm triệu dòng có rủi ro gì so với lookup table?"

---

## 8. Audit Columns
**Bản chất:** Các cột chuẩn hóa để theo dõi lịch sử thay đổi của 1 record: `created_at`, `updated_at`, `created_by`, `updated_by` (và đôi khi `deleted_at`, `version`).

**Ví dụ:** Mọi bảng nghiệp vụ đều có `created_at`, `updated_at` để trả lời "record này tạo/sửa lúc nào, bởi ai" mà không cần tra log riêng.

**Cách hoạt động:** `created_at` set 1 lần khi insert; `updated_at` tự động cập nhật mỗi lần update (qua trigger, ORM hook, hoặc `ON UPDATE CURRENT_TIMESTAMP`); `version` dùng cho optimistic locking.

**Khi nào dùng:** Gần như bảng nghiệp vụ nào cũng nên có, đặc biệt hệ thống cần audit/debug/compliance.

**Câu hỏi phỏng vấn:** "Audit column khác gì Change Data Capture (CDC)/audit log table riêng? Khi nào cần cả hai?"

---

## 9. Schema Migrations
**Bản chất:** Cách quản lý thay đổi cấu trúc DB (thêm cột, đổi kiểu dữ liệu, thêm index...) một cách có version, có thể apply/rollback tuần tự, đồng bộ giữa các môi trường (dev/staging/prod).

**Ví dụ:** File `2024_08_01_add_status_to_orders.sql` được chạy tuần tự qua tool như Flyway/Alembic/Prisma Migrate, đảm bảo mọi môi trường có cùng schema.

**Cách hoạt động:** Migration tool lưu bảng `schema_migrations` đánh dấu migration nào đã chạy; mỗi migration có `up` (áp dụng) và `down` (rollback).

**Khi nào dùng:** Bắt buộc trong mọi dự án production có nhiều người/nhiều môi trường — tránh chỉnh schema thủ công qua GUI.

**Câu hỏi phỏng vấn:** "Migration thêm cột NOT NULL vào bảng lớn đang có traffic production — làm sao tránh downtime/lock table?" (gợi ý: thêm cột nullable trước, backfill dữ liệu theo batch, sau đó mới add constraint NOT NULL).

---

## 10. Precision & Data Types
**Bản chất:** Chọn đúng kiểu dữ liệu và độ chính xác (precision) cho từng loại giá trị — sai lựa chọn gây lỗi tính toán (đặc biệt số tiền) hoặc lãng phí storage.

**Ví dụ:** Dùng `FLOAT`/`DOUBLE` cho tiền tệ sẽ gây sai số làm tròn (0.1 + 0.2 ≠ 0.3 chính xác) → phải dùng `DECIMAL(19,4)` hoặc lưu số nguyên (cent/xu) thay vì số thực.

**Cách hoạt động:** `DECIMAL(p, s)` lưu số chính xác tuyệt đối (p = tổng chữ số, s = chữ số thập phân) bằng cách lưu dạng chuỗi số nguyên nội bộ thay vì binary floating-point như `FLOAT`.

**Khi nào dùng:** `DECIMAL` cho tiền tệ, số liệu tài chính, đo lường chính xác cao; `FLOAT/DOUBLE` chỉ dùng cho khoa học/thống kê chấp nhận sai số nhỏ.

**Câu hỏi phỏng vấn:** "Vì sao không nên dùng FLOAT để lưu số tiền? Cho ví dụ cụ thể lỗi làm tròn."

---

# Phần 3: 10 Ways to Scale a Database

## 1. Vertical Scaling
**Bản chất:** Tăng tài nguyên (CPU, RAM, disk I/O) của 1 máy chủ DB duy nhất thay vì thêm máy.

**Ví dụ:** Nâng cấp RDS instance từ `db.m5.large` lên `db.m5.4xlarge`.

**Cách hoạt động:** Đơn giản — chỉ cần thay đổi cấu hình instance, không cần sửa code/schema.

**Khi nào dùng:** Giai đoạn đầu, traffic chưa quá lớn — giải pháp nhanh nhất nhưng có trần vật lý (không thể scale vô hạn) và single point of failure.

**Câu hỏi phỏng vấn:** "Giới hạn lớn nhất của vertical scaling là gì so với horizontal scaling?"

---

## 2. Read Replicas
**Bản chất:** Tạo các bản sao chỉ-đọc (read-only) của DB chính, phân tải các truy vấn `SELECT` sang replica, giữ DB chính (primary/master) chỉ xử lý ghi.

**Ví dụ:** Ứng dụng đọc dashboard/báo cáo trỏ vào read replica, các thao tác `INSERT/UPDATE` đơn hàng vẫn đi vào primary.

**Cách hoạt động:** Replication bất đồng bộ (async) hoặc đồng bộ (sync) từ primary sang replica qua binlog/WAL; có replication lag — dữ liệu ở replica có thể trễ vài ms đến vài giây so với primary.

**Khi nào dùng:** Hệ thống có tỉ lệ đọc/ghi (read/write ratio) cao — đa số ứng dụng web thực tế.

**Câu hỏi phỏng vấn:** "Replication lag gây ra vấn đề gì? Cho ví dụ tình huống 'đọc thấy dữ liệu cũ' (read-your-writes problem) và cách xử lý."

---

## 3. Sharding
**Bản chất:** Chia dữ liệu ra nhiều DB instance độc lập (shard) theo 1 key nào đó (vd `user_id`), mỗi shard chỉ chứa 1 phần dữ liệu — khác read replica (chứa toàn bộ dữ liệu).

**Ví dụ:** User có `id % 4 == 0` vào shard 1, `== 1` vào shard 2... — mỗi shard là 1 DB server riêng, có cả khả năng đọc và ghi.

**Cách hoạt động:** Cần 1 tầng routing (application logic hoặc proxy) để biết query nào đi vào shard nào dựa trên shard key.

**Khi nào dùng:** Khi dữ liệu quá lớn để 1 DB xử lý (write throughput hoặc storage vượt giới hạn vertical scaling).

**Câu hỏi phỏng vấn:** "Sharding gây khó khăn gì cho JOIN và transaction xuyên nhiều shard? Chọn shard key sai gây hậu quả gì (hot shard)?"

---

## 4. Caching Layer
**Bản chất:** Lưu kết quả truy vấn/dữ liệu hay dùng vào 1 tầng nhớ nhanh (in-memory, vd Redis) để giảm số lần phải chạm vào DB.

**Ví dụ:** Cache thông tin profile user trong Redis với TTL 5 phút, giảm 90% query `SELECT` lặp lại tới DB chính.

**Cách hoạt động:** Cache-aside (application check cache trước, miss thì query DB rồi ghi lại cache), write-through, hoặc write-behind — cần chiến lược invalidation khi dữ liệu gốc thay đổi.

**Khi nào dùng:** Dữ liệu đọc nhiều, ghi ít, chấp nhận độ trễ nhỏ (eventual consistency) giữa cache và DB.

**Câu hỏi phỏng vấn:** "Cache invalidation khó ở điểm nào? Giải thích vấn đề cache stampede và cách phòng tránh."

---

## 5. Connection Pooling
**Bản chất:** Duy trì sẵn 1 tập kết nối DB đã mở (pool), tái sử dụng thay vì mở/đóng connection mới cho mỗi request — vì tạo connection DB tốn chi phí (TCP handshake, auth).

**Ví dụ:** PgBouncer/HikariCP giữ pool 20 kết nối, hàng trăm request ứng dụng chia nhau dùng chung, trả lại pool sau khi dùng xong thay vì disconnect.

**Cách hoạt động:** Pool quản lý vòng đời connection: `checkout` khi cần, `return` khi xong; nếu pool cạn, request phải chờ hoặc bị từ chối.

**Khi nào dùng:** Luôn luôn cần trong production — đặc biệt quan trọng khi có nhiều instance ứng dụng cùng kết nối 1 DB (dễ vượt giới hạn `max_connections` của DB).

**Câu hỏi phỏng vấn:** "Điều gì xảy ra nếu connection pool bị cạn kiệt do 1 vài query chạy chậm (liên hệ tới retry storm)?"

---

## 6. Denormalization
**Bản chất:** Chủ động phá vỡ chuẩn hóa (thêm dữ liệu trùng lặp có kiểm soát) để giảm số lượng JOIN cần thiết, đổi lấy tốc độ đọc nhanh hơn.

**Ví dụ:** Lưu thẳng `customer_name` vào bảng `orders` thay vì luôn phải JOIN với bảng `customers` — đọc nhanh hơn nhưng phải đồng bộ khi `customer_name` đổi.

**Cách hoạt động:** Thêm cột trùng lặp/redundant, cập nhật đồng thời ở nhiều nơi khi dữ liệu gốc thay đổi (qua application logic hoặc trigger).

**Khi nào dùng:** Hệ thống đọc nhiều, cần tốc độ cao, chấp nhận đánh đổi phức tạp hóa việc ghi và rủi ro dữ liệu không nhất quán tạm thời.

**Câu hỏi phỏng vấn:** "Denormalization đánh đổi điều gì so với chuẩn hóa (normalization)? Cho ví dụ tình huống nên và không nên dùng."

---

## 7. Partitioning
**Bản chất:** Chia 1 bảng lớn thành nhiều partition vật lý nhỏ hơn trong cùng 1 DB instance (khác sharding — sharding chia ra nhiều instance khác nhau).

**Ví dụ:** Bảng `logs` partition theo tháng (`logs_2026_01`, `logs_2026_02`...) — query có filter theo thời gian chỉ cần quét đúng partition liên quan (partition pruning).

**Cách hoạt động:** DB tự động route query/insert vào đúng partition dựa trên partition key (range, list, hash partitioning).

**Khi nào dùng:** Bảng cực lớn trong 1 DB, cần tăng tốc query có filter theo cột partition key, dễ archive/xóa dữ liệu cũ theo partition.

**Câu hỏi phỏng vấn:** "Partitioning và Sharding khác nhau ở điểm nào? Khi nào chọn cái này thay vì cái kia?"

---

## 8. CQRS (Command Query Responsibility Segregation)
**Bản chất:** Tách riêng mô hình dữ liệu/luồng xử lý cho ghi (command) và đọc (query) — dùng 2 model, thậm chí 2 DB khác nhau, tối ưu riêng biệt cho từng mục đích.

**Ví dụ:** Ghi đơn hàng vào Postgres (chuẩn hóa, transaction chặt); đồng thời đẩy sang Elasticsearch một model đã denormalize sẵn để phục vụ tìm kiếm/dashboard nhanh.

**Cách hoạt động:** Command side xử lý business logic + ghi vào write model; sync (qua event/CDC) sang read model được tối ưu riêng cho truy vấn.

**Khi nào dùng:** Hệ thống có yêu cầu đọc và ghi khác biệt rõ rệt về hình dạng dữ liệu hoặc tải (vd hệ thống có nhiều dashboard phức tạp bên cạnh nghiệp vụ transaction).

**Câu hỏi phỏng vấn:** "CQRS đánh đổi gì về độ phức tạp hệ thống và tính nhất quán (eventual consistency) giữa 2 model?"

---

## 9. Materialized Views
**Bản chất:** Kết quả của 1 query phức tạp (thường có JOIN/aggregate) được lưu sẵn vật lý như 1 bảng, thay vì tính toán lại mỗi lần query (khác view thường — chỉ là query được đặt tên, luôn tính lại).

**Ví dụ:** `daily_revenue_summary` là materialized view tổng hợp doanh thu theo ngày từ bảng `orders` — dashboard đọc thẳng view này thay vì aggregate hàng triệu dòng mỗi lần load.

**Cách hoạt động:** Cần `REFRESH` định kỳ (batch, theo lịch) hoặc trigger để cập nhật lại dữ liệu — dữ liệu trong view có thể "cũ" cho tới lần refresh tiếp theo.

**Khi nào dùng:** Query aggregate nặng, chạy lặp lại nhiều lần, chấp nhận dữ liệu không real-time tuyệt đối.

**Câu hỏi phỏng vấn:** "Materialized view khác gì caching layer (Redis)? Khi nào chọn cái nào?"

---

## 10. Archiving Cold Data
**Bản chất:** Di chuyển dữ liệu ít truy cập (cold data — vd đơn hàng đã hoàn tất >2 năm) sang nơi lưu trữ rẻ hơn/riêng biệt, giữ DB chính (hot path) gọn nhẹ và nhanh.

**Ví dụ:** Đơn hàng >2 năm được export sang S3/BigQuery dạng Parquet, xóa khỏi bảng `orders` chính — chỉ giữ lại dữ liệu "nóng" (hot data) trong DB transaction.

**Cách hoạt động:** Batch job định kỳ (vd hàng đêm) quét dữ liệu đủ điều kiện cold, copy sang kho lưu trữ rẻ (data warehouse/object storage), sau đó xóa khỏi DB chính — thường kết hợp với partitioning (mục 7) để archive nguyên cả partition cho nhanh.

**Khi nào dùng:** Bảng tăng trưởng không giới hạn theo thời gian (log, transaction, event) — giữ hiệu năng DB chính ổn định dài hạn thay vì để bảng phình to vô tận.

**Câu hỏi phỏng vấn:** "Thiết kế chiến lược archive cho 1 bảng orders hàng trăm triệu dòng mà không ảnh hưởng traffic production đang chạy."

---

# Tổng kết: các khái niệm này nối với nhau thế nào

- **Message Queue** giải quyết bài toán decoupling theo thời gian giữa các service — và tự nó cần backpressure/idempotency để không trở thành nguồn gây quá tải (liên hệ trực tiếp tới tài liệu Retry/Eviction/Backpressure trước đó).
- **Data Modeling** là nền tảng đúng đắn của schema — nếu sai ngay từ đầu (composite key sai, thiếu audit column, dùng FLOAT cho tiền), mọi chiến lược scale phía sau đều phải vá lỗi thay vì tối ưu.
- **Scale Database** là tập hợp chiến lược khi dữ liệu/tải vượt quá khả năng 1 model đơn giản — đi từ dễ nhất (vertical scaling) đến phức tạp nhất (sharding, CQRS), và hầu hết đều dựa trên nền data modeling tốt ở Phần 2.

Ba phần này, cộng với tài liệu Retry/Eviction/Backpressure trước, tạo thành một bức tranh khá đầy đủ về cách một hệ thống backend/data engineering vận hành đúng dưới tải thực tế — đúng chủ đề hay bị hỏi sâu trong vòng system design.
