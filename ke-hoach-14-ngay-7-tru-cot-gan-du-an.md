# KẾ HOẠCH 14 NGÀY (BẢN VIẾT LẠI) — THEO 7 TRỤ CỘT ỨNG DỤNG, GẮN VỚI DỰ ÁN CÁ NHÂN

*Bản này thay thế bản 14 ngày cũ. Khác biệt cốt lõi: bản cũ luyện **trả lời đúng lý thuyết**, bản này luyện **năng lực ứng dụng** — đúng cách một buổi phỏng vấn Middle Data Engineer thực sự diễn ra. Kiến thức nền (DB internals, Hadoop config, normalization, thống kê) không học riêng nữa mà được nhúng làm nguyên liệu cho từng bài tập bên dưới.*

## 7 TRỤ CỘT & PHÂN BỔ 14 NGÀY

| Trụ cột | Ngày | Vì sao xếp thứ tự này |
|---|---|---|
| 1. Hiểu tường tận dự án cá nhân | 1-2 | Làm nền tảng đầu tiên — mọi trụ cột sau đều "quay lại" dùng chính dự án này làm ví dụ |
| 2. Code cơ bản (MapReduce, DSA, SQL) | 3-4 | Kiểm tra nền tảng kỹ thuật thuần túy trước khi vào bài toán ứng dụng |
| 3. Giải thuật/bài tập khai phá dữ liệu | 5 | Cầu nối giữa code cơ bản và tư duy hệ thống lớn hơn |
| 4. Data Warehouse Design (schema + tiêu chí A/B/C) | 6-7 | Bài tập ứng dụng đầu tiên, phạm vi vừa (1 hệ thống), dễ luyện trước khi vào scope lớn hơn |
| 5. System Design gắn với dự án | 8-9 | Mở rộng quy mô lên toàn bộ pipeline/hệ thống |
| 6. Architecture Design + Tool Justification | 10-11 | Đòi hỏi tổng hợp mọi thứ đã học để so sánh, đánh đổi công cụ |
| 7. Tối ưu hóa vấn đề (broken → optimized) | 12 | Bài test khó nhất — cần đủ nền tảng từ 6 trụ cột trước mới làm tốt |
| Tổng hợp & Mock Interview | 13-14 | Nối tất cả 7 trụ cột thành 1 buổi phỏng vấn mô phỏng hoàn chỉnh |

**Nhịp mỗi ngày**: Sáng = học/luyện kỹ thuật mới trong trụ cột. Chiều = áp dụng ngay vào 1 dự án cụ thể trong CV của bạn. Tối 30 phút = Feynman (tự giảng lại thành tiếng), ghi vào sổ tay lõi.

---

## NGÀY 1-2 — TRỤ CỘT 1: HIỂU TƯỜNG TẬN DỰ ÁN CÁ NHÂN

**Mục tiêu**: với **mỗi dự án chính** trong CV (5-6 dự án: VLM Auto-Labeling, Student Behavioral Streaming/Neo4j, OCR Invoice, RAG/LLM Chat, Enterprise Fleet CDC), phải trả lời được rành mạch trong 60-90 giây mỗi khung sau, không cần nhìn giấy:

**Khung 5 lớp để đào sâu MỖI dự án** (dùng lại khung Output→Outcome→Impact đã có, mở rộng thêm 2 lớp):
1. **Bối cảnh & bài toán gốc**: vấn đề nghiệp vụ ban đầu là gì, tại sao cần giải quyết.
2. **Quyết định thiết kế & lý do (Why)**: tại sao chọn công nghệ/kiến trúc này, **đã cân nhắc phương án nào khác và loại bỏ vì sao** — đây là phần dễ bị hỏi vặn nhất.
3. **Output**: đã xây được gì (kỹ thuật cụ thể).
4. **Outcome**: con số thay đổi đo lường được (latency, throughput, accuracy, tải hệ thống...).
5. **Impact**: "vậy thì sao" — ai hưởng lợi, quyết định gì trở nên khả thi, rủi ro gì được giảm.

**Ngày 1 (sáng+chiều)**: Làm khung 5 lớp cho 3 dự án đầu tiên — ưu tiên 2 dự án mạnh nhất, dễ kể chuyện nhất: **RAG/LLM Chat** (hybrid retrieval + Ray Core) và **CDC Pipeline** (Debezium/Odoo).

**Ngày 2 (sáng+chiều)**: Làm tiếp 2-3 dự án còn lại (OCR Invoice, Neo4j Streaming, VLM Auto-Labeling).

**Câu hỏi vặn cần tự chuẩn bị cho MỖI dự án** (interviewer Middle/Senior luôn hỏi ít nhất 1 trong số này):
- "Nếu làm lại, bạn sẽ thay đổi gì?"
- "Phần nào của hệ thống này bạn không tự tin nhất / biết còn hạn chế?"
- "Nếu dữ liệu tăng 10 lần, phần nào sẽ vỡ trước tiên?"

**Test cuối Ngày 2**: Ghi âm tự kể lại toàn bộ 5-6 dự án theo khung 5 lớp, mỗi dự án đúng 60-90 giây, tổng không quá 8 phút — đây là độ dài chuẩn cho phần "giới thiệu kinh nghiệm" đầu buổi phỏng vấn.

---

## NGÀY 3-4 — TRỤ CỘT 2: CODE CƠ BẢN (MAPREDUCE, DSA, SQL)

### Ngày 3 sáng — MapReduce (code tay, không chỉ lý thuyết)

Viết bằng Python mô phỏng đúng 3 pha Map → Shuffle → Reduce cho 1 bài toán bám sát dự án của bạn (word count kinh điển interviewer sẽ thấy nhàm — dùng bài toán thực tế hơn):

```python
# Bài toán: đếm số lượng sự kiện theo event_type từ log Kafka (liên hệ trực tiếp dự án Neo4j Streaming)
def map_phase(log_lines):
    mapped = []
    for line in log_lines:
        event_type = line.split(",")[1]   # giả định format: timestamp,event_type,user_id
        mapped.append((event_type, 1))
    return mapped

def shuffle_phase(mapped):
    shuffled = {}
    for key, value in mapped:
        shuffled.setdefault(key, []).append(value)
    return shuffled

def reduce_phase(shuffled):
    reduced = {}
    for key, values in shuffled.items():
        reduced[key] = sum(values)
    return reduced

logs = ["1723000,DISPLAYED,u1", "1723001,MASTERED,u2", "1723002,DISPLAYED,u3"]
result = reduce_phase(shuffle_phase(map_phase(logs)))
print(result)  # {'DISPLAYED': 2, 'MASTERED': 1}
```

**Câu hỏi tự đặt để hiểu sâu hơn code**: "Nếu dữ liệu log lớn tới mức không load hết vào RAM 1 máy, Map phase chạy ở đâu? Shuffle phase (bước tốn kém nhất trong MapReduce thật) làm gì khác với đoạn code demo trên?" → Trả lời: Map chạy song song trên nhiều node theo từng block dữ liệu (data locality — đúng lý do dùng HDFS ở Câu 1 cũ); Shuffle thật phải ghi dữ liệu trung gian ra đĩa và truyền qua mạng giữa các node (network I/O là bottleneck chính, đây là lý do Spark tối ưu hơn MapReduce nhờ giữ dữ liệu trong RAM giữa các stage thay vì ghi đĩa mỗi lần shuffle).

### Ngày 3 chiều — Data Structures + 1 giải thuật bất kỳ

- Ôn nhanh lại Big-O + code tay (đã có trong bản kế hoạch trước — giữ nguyên): dedupe bằng `set`, top-K bằng `heapq`, Node cho linked list, duyệt cây đệ quy.
- Chọn code tay **1 giải thuật sắp xếp** (QuickSort hoặc MergeSort) từ đầu, giải thích được độ phức tạp trung bình/xấu nhất.
- Liên hệ trực tiếp: "MinHash LSH dùng cấu trúc dữ liệu nào để nhóm các tài liệu tương tự vào cùng bucket?" (hash table/dict) — ôn lại đúng dự án MinHash dedupe của bạn bằng góc nhìn cấu trúc dữ liệu.

### Ngày 4 — SQL thực hành viết tay (không nhìn tài liệu cũ)

- Mở lại 2 file SQL đã có trước đó (window functions, CTE, backfill) — nhưng lần này **gõ lại từ đầu không nhìn đáp án**, chỉ nhìn đề bài.
- Luyện 3-4 bài mức trung bình trong 15-20 phút/bài (đúng thời lượng 1 câu live-coding SQL trong phỏng vấn thật): Top-N mỗi nhóm bằng `ROW_NUMBER`, phát hiện trùng lặp, CTE đệ quy đơn giản.

**Test cuối Ngày 4**: giải 1 bài SQL mới hoàn toàn (không có trong tài liệu cũ) trong 20 phút, tự nghĩ đề dựa trên domain quen thuộc (vd "Top 5 khách hàng theo doanh thu mỗi quý, kèm % tăng trưởng so với quý trước").

---

## NGÀY 5 — TRỤ CỘT 3: GIẢI THUẬT / BÀI TẬP KHAI PHÁ DỮ LIỆU

Đây là nhóm bài toán đứng giữa "code cơ bản" và "system design" — thường được hỏi dạng: "Viết thuật toán để giải quyết X" với X là 1 vấn đề khai phá dữ liệu thực tế.

**3 bài luyện cụ thể (đều liên hệ trực tiếp dự án của bạn):**

1. **Phát hiện outlier trong stream dữ liệu (sliding window + Z-score)** — liên hệ dự án Kafka telemetry:
```python
from collections import deque
import statistics

def detect_anomaly(stream, window_size=20, z_threshold=3):
    window = deque(maxlen=window_size)
    anomalies = []
    for value in stream:
        if len(window) >= window_size // 2:
            mean = statistics.mean(window)
            stdev = statistics.stdev(window) if len(window) > 1 else 1
            z_score = (value - mean) / stdev if stdev > 0 else 0
            if abs(z_score) > z_threshold:
                anomalies.append(value)
        window.append(value)
    return anomalies
```
Giải thích được: tại sao dùng sliding window thay vì tính trên toàn bộ lịch sử (thích nghi với thay đổi hành vi theo thời gian — concept drift), và trade-off giữa `window_size` lớn/nhỏ.

2. **Đếm tần suất Top-K trong dữ liệu lớn không load hết vào RAM** — liên hệ ý tưởng Count-Min Sketch hoặc đơn giản hóa bằng heap:
- Giải thích ý tưởng: dùng min-heap kích thước K, duyệt qua dữ liệu (streaming, không load hết), chỉ giữ K phần tử lớn nhất — độ phức tạp O(n log k) thay vì O(n log n) nếu sort toàn bộ.

3. **Thuật toán dedupe near-duplicate đơn giản hóa (liên hệ trực tiếp dự án MinHash LSH)**:
- Không cần code lại MinHash đầy đủ — chỉ cần code tay 1 bản đơn giản hóa: shingling (chia văn bản thành n-gram) + Jaccard similarity giữa 2 tập, giải thích tại sao cần LSH khi có hàng triệu tài liệu (so sánh từng cặp là O(n²), không khả thi).

**Test cuối ngày**: chọn 1 trong 3 bài, tự giải lại từ đầu không xem code mẫu, giải thích được độ phức tạp và ít nhất 1 trade-off của thuật toán.

---

## NGÀY 6-7 — TRỤ CỘT 4: THIẾT KẾ DATA WAREHOUSE THEO SCHEMA + TIÊU CHÍ A/B/C

Đây là dạng bài tập rất phổ biến trong phỏng vấn Middle DE: "Cho schema OLTP sau, thiết kế Data Warehouse đáp ứng các tiêu chí A, B, C."

### Ngày 6 — Luyện với đề tự tạo dựa trên domain quen thuộc (OCR Invoice)

**Đề tự đặt**: Cho schema OLTP đơn giản `invoices(invoice_id, vendor_id, invoice_date, amount, status)`, `vendors(vendor_id, name, region)`. Thiết kế Data Warehouse đáp ứng:
- **Tiêu chí A**: Báo cáo tổng chi phí theo vùng (region) theo tháng/quý/năm.
- **Tiêu chí B**: Theo dõi lịch sử thay đổi vùng của vendor (vendor có thể đổi region theo thời gian, cần biết vendor thuộc region nào tại thời điểm phát sinh hóa đơn).
- **Tiêu chí C**: Hỗ trợ truy vấn nhanh cho dashboard real-time (độ trễ thấp).

**Tự giải theo từng tiêu chí**:
- A → Star Schema với `fact_invoice` + `dim_vendor` + `dim_date`.
- B → SCD Type 2 cho `dim_vendor` (đúng kỹ thuật bạn đã làm ở dự án Enterprise Fleet — liên hệ trực tiếp).
- C → Pre-aggregated rollup table hoặc Materialized View (đã có trong tài liệu SQL trước, ôn lại).

### Ngày 7 — Luyện thêm 1 đề khác domain (không dùng lại OCR để tránh học vẹt 1 kiểu)

**Đề tự đặt**: Cho schema `orders`, `order_items`, `products`, `customers`. Thiết kế DW đáp ứng: (A) phân tích giỏ hàng — sản phẩm nào thường mua cùng nhau; (B) Customer Lifetime Value theo cohort; (C) dữ liệu đến trễ (late-arriving orders) không được làm sai lệch báo cáo đã chốt.

- (C) là phần khó nhất — liên hệ đúng khái niệm watermark bạn đã dùng trong Spark Structured Streaming, áp dụng tương tự vào ngữ cảnh batch DW.

**Test cuối Ngày 7**: trình bày miệng (ghi âm) toàn bộ thiết kế DW của cả 2 đề trong 5-7 phút mỗi đề, có vẽ được sơ đồ Star Schema trên giấy.

---

## NGÀY 8-9 — TRỤ CỘT 5: SYSTEM DESIGN GẮN VỚI DỰ ÁN CÁ NHÂN

Khác Trụ cột 4 (thiết kế 1 phần dữ liệu), đây là thiết kế **toàn bộ pipeline/hệ thống** — quy mô lớn hơn, cần ước lượng tài nguyên, bàn kiến trúc.

**Khung trả lời chuẩn cho mọi đề system design** (luyện thuộc khung này, áp dụng cho mọi bài):
```
1. Làm rõ yêu cầu (functional + non-functional: throughput, latency, độ tin cậy cần thiết)
2. Ước lượng quy mô (back-of-envelope: bao nhiêu event/giây, bao nhiêu GB/ngày)
3. Thiết kế kiến trúc tổng thể (vẽ sơ đồ high-level: nguồn → ingest → xử lý → lưu trữ → serving)
4. Đi sâu 1-2 thành phần quan trọng nhất
5. Thảo luận trade-off & điểm có thể vỡ khi scale
```

**Ngày 8 — Luyện "redesign" chính 2 dự án đã làm, nhưng ở QUY MÔ LỚN HƠN 10 LẦN**:
- "Pipeline OCR của bạn xử lý 40.000 hóa đơn — nếu phải xử lý 4 triệu hóa đơn/ngày, thiết kế thay đổi thế nào?" → điểm cần bàn: horizontal scaling OCR workers, hàng đợi (queue) chống nghẽn, dead-letter-queue cho hóa đơn lỗi OCR, giới hạn nào của kiến trúc cũ sẽ vỡ trước (Airflow single scheduler? PaddleOCR chạy CPU-bound?).
- "Pipeline CDC của bạn stream 7 bảng Odoo — nếu công ty có 200 bảng cần CDC, kiến trúc thay đổi gì?" → điểm cần bàn: Debezium connector per-table hay per-database, Kafka topic partitioning strategy, schema registry để quản lý schema evolution ở quy mô lớn.

**Ngày 9 — Luyện đề mới hoàn toàn chưa từng làm** (để test khả năng áp dụng, không chỉ nhớ lại):
- Đề gợi ý: "Thiết kế hệ thống thu thập & xử lý log clickstream cho app có 5 triệu user, cần dashboard real-time độ trễ dưới 1 phút và dữ liệu lịch sử phục vụ báo cáo hàng ngày." — đề này buộc bạn kết hợp cả batch + streaming (Lambda/Kappa Architecture), đúng chuyên môn bạn có nhưng ở bài toán mới.

**Test cuối Ngày 9**: trình bày 1 đề bất kỳ trong 30-40 phút theo đúng khung 5 bước, ghi âm, tự nghe lại xem có bỏ sót bước nào không.

---

## NGÀY 10-11 — TRỤ CỘT 6: ARCHITECTURE DESIGN + TOOL JUSTIFICATION ("TẠI SAO DÙNG X MÀ KHÔNG DÙNG Y")

Đây chính là mở rộng của Câu 1 cũ ("tại sao dùng HDFS") — luyện phản xạ so sánh công cụ có hệ thống, không chỉ riêng HDFS.

### Ngày 10 — Lập bảng so sánh cho ĐÚNG các công cụ bạn đã dùng trong CV (ưu tiên, vì khả năng cao bị hỏi)

Với mỗi cặp công cụ, tự viết ra 3-4 dòng: khi nào dùng cái này, khi nào dùng cái kia, trade-off chính.

| Cặp cần so sánh | Trade-off chính cần nói được |
|---|---|
| HDFS vs Object Storage (S3) | Data locality vs tách biệt storage/compute, chi phí vận hành on-prem vs cloud |
| Kafka vs RabbitMQ | Kafka: log-based, replay được, throughput cao, phù hợp streaming/CDC. RabbitMQ: message queue truyền thống, phù hợp task queue hơn là event streaming |
| Spark Structured Streaming vs Flink | Spark: micro-batch (độ trễ vài trăm ms - vài giây), hệ sinh thái rộng. Flink: true streaming (độ trễ ms), tốt hơn cho use case cực nhạy latency |
| Neo4j vs mô hình quan hệ cho bài toán Graph | Truy vấn quan hệ nhiều-nhiều nhiều tầng (multi-hop) trong Neo4j là O(1) theo mỗi bước nhảy cạnh, trong khi RDBMS cần nhiều JOIN tốn kém khi độ sâu quan hệ tăng |
| LanceDB (dense) vs BM25s (sparse) trong RAG | Dense: bắt ngữ nghĩa nhưng có thể bỏ lỡ khớp từ khóa chính xác (tên riêng, mã số). Sparse: khớp từ khóa chính xác nhưng không hiểu ngữ nghĩa — đây là lý do bạn dùng Hybrid (RRF) |
| Airflow vs công cụ orchestration khác (Dagster/Prefect) | Airflow: hệ sinh thái lớn, mature, nhưng scheduler có thể là điểm nghẽn ở quy mô cực lớn; các công cụ mới hơn có mô hình lập trình khai báo tốt hơn — chỉ cần biết khái niệm, không cần thực chiến |

### Ngày 11 — Luyện trả lời dạng "kiến trúc tổng thể" của 1 dự án, nêu rõ vì sao chọn từng công cụ trong chuỗi

Chọn dự án phức tạp nhất (Enterprise Fleet: Kafka + PySpark + HDFS + Airflow + Redis) — luyện trình bày **toàn bộ chuỗi công cụ** liền mạch, mỗi công cụ giải thích đúng 1 câu "tại sao đây, không phải cái khác":
> "Tôi dùng Kafka làm buffer trung gian vì cần decouple giữa nguồn phát telemetry và tốc độ xử lý của Spark, tránh nguồn bị nghẽn khi consumer chậm. Tôi dùng HDFS làm nơi lưu trữ lâu dài vì cần data locality khi Spark batch job chạy lại lịch sử. Tôi dùng Redis Pub/Sub thay vì để dashboard polling database vì cần độ trễ dưới 15ms mà polling không đáp ứng được..."

**Test cuối Ngày 11**: chọn ngẫu nhiên 3 công cụ bất kỳ trong CV của bạn, giải thích "tại sao không dùng X thay thế" trong vòng 60 giây/công cụ, không chuẩn bị trước (test phản xạ thật).

---

## NGÀY 12 — TRỤ CỘT 7: TỐI ƯU HÓA VẤN ĐỀ (BROKEN → OPTIMIZED)

Đây là dạng bài khó nhất — interviewer đưa ra 1 thiết kế/đoạn code có vấn đề, yêu cầu bạn tìm ra điểm yếu và đề xuất tối ưu. **Cách luyện tốt nhất: tự tạo ra vấn đề "chưa tốt" từ chính kinh nghiệm của bạn, rồi tự sửa** — đúng như bạn đề xuất.

**3 bài luyện tự tạo (dựa trên chính CV của bạn, cố tình viết phiên bản "kém" trước):**

1. **Phiên bản kém của Backfill** (đã luyện ở các câu hỏi trước — ôn lại):
```sql
-- Phiên bản kém: DELETE + INSERT rời rạc, không transaction, không idempotent
DELETE FROM fact_daily_sales WHERE sales_date = '2026-08-01';
INSERT INTO fact_daily_sales SELECT ... FROM staging_sales_raw WHERE sales_date = '2026-08-01';
```
→ Tự liệt kê vấn đề: không atomic (crash giữa chừng mất dữ liệu), không xử lý concurrent write, không idempotent nếu chạy lại 2 lần → Tự sửa bằng `REPLACE WHERE`/partition swap đã học ở các câu trước.

2. **Phiên bản kém của OCR Pipeline** (tự tạo dựa trên dự án thật của bạn):
```text
Phiên bản kém: xử lý từng hóa đơn 1 API call riêng lẻ, không retry khi lỗi mạng,
không phân biệt lỗi tạm thời (network timeout) và lỗi vĩnh viễn (ảnh hỏng),
không có Dead Letter Queue cho hóa đơn xử lý lỗi.
```
→ Tự liệt kê vấn đề: throughput thấp (network round-trip overhead mỗi request), không có cơ chế phục hồi khi lỗi tạm thời, hóa đơn lỗi vĩnh viễn có thể làm nghẽn toàn bộ pipeline nếu cứ retry vô hạn.
→ Tự sửa: batching 15-20 mẫu/API call (đúng bạn đã làm thật), retry với exponential backoff cho lỗi tạm thời, route sang Dead Letter Queue riêng cho lỗi vĩnh viễn để không chặn các hóa đơn khác.

3. **Phiên bản kém của Watermarking** (streaming):
```text
Phiên bản kém: không có watermark, giữ state vô hạn cho mọi window,
hoặc watermark quá ngắn (1 phút) gây mất nhiều dữ liệu hợp lệ.
```
→ Tự sửa: chọn watermark dựa trên phân tích độ trễ thực tế nguồn dữ liệu (đã luyện ở câu hỏi cũ), theo dõi `numRowsDroppedByWatermark` để điều chỉnh.

**Test cuối ngày**: tự tạo **1 bài "broken → optimized" MỚI hoàn toàn** không nằm trong 3 bài trên, dựa trên 1 phần bất kỳ của CV, tự giải trong 15-20 phút.

---

## NGÀY 13-14 — TỔNG HỢP: MOCK INTERVIEW ĐẦY ĐỦ 7 TRỤ CỘT

### Ngày 13 — Mock Interview dài (90-120 phút), mô phỏng đúng 1 buổi phỏng vấn thật, đủ 7 phần:
1. Giới thiệu bản thân + 1-2 dự án theo khung 5 lớp (5-8 phút)
2. 1 câu code cơ bản (MapReduce hoặc DSA hoặc SQL) — live coding thật, có tính giờ
3. 1 bài giải thuật khai phá dữ liệu
4. 1 đề Data Warehouse Design với tiêu chí
5. 1 đề System Design gắn dự án
6. 2-3 câu "tại sao dùng công cụ này không dùng công cụ khác"
7. 1 bài "broken → optimized"

Ghi âm toàn bộ, nghe lại, đánh dấu chính xác chỗ yếu.

### Ngày 14 — Vá điểm yếu + tổng ôn nhẹ (không học mới)
- Sáng: chỉ ôn đúng phần bị đánh dấu yếu từ Ngày 13.
- Chiều: đọc lướt sổ tay lõi 1 lượt, đọc lại bảng tóm tắt bên dưới.
- Tối: nghỉ ngơi, ngủ đủ — không nhồi thêm nội dung mới sát ngày phỏng vấn.

---

## BẢNG TÓM TẮT 14 NGÀY (BẢN MỚI)

```
Ngày 1-2:   Hiểu tường tận dự án — khung 5 lớp (Bối cảnh→Why→Output→Outcome→Impact) cho 5-6 dự án
Ngày 3-4:   Code cơ bản — MapReduce tay, DSA + Big-O, SQL live-coding không xem đáp án cũ
Ngày 5:     Giải thuật khai phá dữ liệu — Anomaly detection, Top-K streaming, dedupe/similarity
Ngày 6-7:   Data Warehouse Design theo tiêu chí A/B/C — luyện 2 đề khác domain
Ngày 8-9:   System Design gắn dự án — redesign dự án cũ ở quy mô x10 + 1 đề hoàn toàn mới
Ngày 10-11: Tool Justification — bảng so sánh công cụ CV + trình bày kiến trúc tổng thể 1 dự án
Ngày 12:    Tối ưu hóa — tự tạo 3 bài "broken → optimized" từ chính dự án, tự sửa
Ngày 13:    Mock Interview đầy đủ 7 phần, 90-120 phút, ghi âm
Ngày 14:    Vá điểm yếu buổi sáng, tổng ôn nhẹ buổi chiều, KHÔNG học mới, nghỉ ngơi

Nguyên tắc xuyên suốt: Sáng học/luyện kỹ thuật → Chiều áp dụng vào dự án CV thật →
Tối 30' Feynman. Luôn output "tại sao" và "vậy thì sao" cho mọi câu trả lời kỹ thuật.
```
