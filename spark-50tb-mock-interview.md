# KỊCH BẢN PHỎNG VẤN MẪU (MOCK INTERVIEW): TỐI ƯU HÓA SPARK JOB 50TB

Tài liệu này đóng vai trò như một kịch bản hội thoại thực tế giữa **Người phỏng vấn (Interviewer)** và **Ứng viên (Candidate)**. Kịch bản này giúp bạn hình dung cách trình bày giải pháp một cách mạch lạc, phản xạ trước các câu hỏi đào sâu (follow-up questions), và thể hiện tư duy thiết kế hệ thống dữ liệu ở quy mô lớn.

---

## PHẦN 1: MỞ ĐẦU - ĐỀ BÀI VÀ PHẢN XẠ ĐẦU TIÊN

**Interviewer:**  
*“Tôi có một bài toán thực tế thế này. Chúng tôi có một tập dữ liệu thô khoảng 50 Terabytes. Trong đó có một cột chứa thông tin ngày tháng ở dạng chuỗi (String) định dạng `YYYY-MM-DD`. Nhiệm vụ của bạn là viết một Spark Job để tách cột này thành 3 cột mới: `year`, `month`, và `day`. Bạn sẽ thiết kế và tối ưu hóa job này như thế nào để nó chạy nhanh nhất và ổn định nhất?”*

**Candidate (Phản xạ đầu tiên - Luôn đặt câu hỏi làm rõ yêu cầu trước khi trả lời):**  
*“Dạ, đây là một bài toán xử lý dữ liệu quy mô lớn rất thú vị. Trước khi đưa ra phương án thiết kế cụ thể, em xin phép được làm rõ một số điểm về dữ liệu đầu vào và đầu ra để định hình giải pháp:*

1.  **Định dạng và Layout dữ liệu đầu vào:** 50TB dữ liệu này hiện đang lưu dưới dạng thô nào (CSV, JSON hay Parquet/ORC)? Nó đã được phân vùng (partition) sẵn trên đĩa theo một cột nào khác chưa?
2.  **Yêu cầu dữ liệu đầu ra:** Chúng ta cần ghi toàn bộ tập dữ liệu gốc kèm theo 3 cột mới, hay chỉ cần trích xuất một bảng nhỏ gồm ID và 3 cột ngày tháng này thôi? Dữ liệu đầu ra sẽ được ghi xuống đâu và phục vụ cho mục đích gì ở downstream?
3.  **Hạ tầng Cluster:** Hệ thống cluster hiện tại của mình chạy trên nền tảng nào (YARN on-premise hay Kubernetes trên Cloud)? Tài nguyên có bị giới hạn hay không?”*

**Interviewer:**  
*“Câu hỏi rất tốt. 
1. Dữ liệu gốc đang ở dạng Parquet, đã partition theo `country` và `device_type`.
2. Đầu ra yêu cầu ghi lại **toàn bộ dữ liệu gốc** cộng thêm 3 cột mới, ghi đè (overwrite) vào một thư mục Parquet khác trên S3 để làm tầng Gold cho đội Data Analyst truy vấn qua Athena.
3. Chúng ta chạy trên AWS EMR với khả năng auto-scaling.”*

---

## PHẦN 2: TRÌNH BÀY GIẢI PHÁP THEO THỨ TỰ CÓ HỆ THỐNG (5 TẦNG)

**Candidate:**  
*“Dạ rõ ạ. Vì yêu cầu là ghi lại toàn bộ dữ liệu gốc + 3 cột mới, đây sẽ là một job nặng về I/O (I/O bound) vì chúng ta phải đọc 50TB và ghi ra một lượng dữ liệu tương đương hoặc lớn hơn trên S3. Em xin trình bày chiến lược tối ưu hóa theo 5 tầng cốt lõi:*

### Tầng 1: Tối ưu hóa Đọc Dữ Liệu (Read Strategy)
Vì dữ liệu đầu vào đã là **Parquet (Columnar Storage)**, đây là một lợi thế lớn. 
- Mặc định Spark sẽ chia các file Parquet thành các partition 128MB. Với 50TB, chúng ta sẽ có khoảng 400.000 partitions, tương đương 400.000 tasks. Số lượng tasks quá lớn này sẽ gây áp lực khủng khiếp lên RAM của Spark Driver trong việc lên lịch (scheduling overhead) và quản lý lineage.
- Giải pháp: Em sẽ điều chỉnh cấu hình `spark.sql.files.maxPartitionBytes` tăng lên **256MB hoặc 512MB**. Điều này giúp gộp các file nhỏ lại khi đọc, giảm tổng số tasks xuống còn khoảng 100.000 - 200.000 tasks, giúp Driver hoạt động nhẹ nhàng hơn rất nhiều.
- Đồng thời tận dụng **Column Pruning** nếu downstream chỉ cần một số cột, nhưng vì đề bài yêu cầu lấy toàn bộ cột nên Spark sẽ phải scan hết các cột.

### Tầng 2: Phép Biến Đổi Tối Ưu (Transformation Layer)
- Phép tách chuỗi `YYYY-MM-DD` sang 3 cột là một **Narrow Transformation** (phép biến đổi hẹp). Nghĩa là dữ liệu dòng nào xử lý dòng đó, hoàn toàn không phát sinh **Shuffle** (không cần di chuyển dữ liệu qua mạng). Do đó, job này có khả năng scale-out tuyến tính rất tốt.
- Em sẽ **tuyệt đối tránh dùng Python UDF** vì nó sẽ bắt dữ liệu serialize qua lại giữa JVM và Python Process qua socket, làm chậm hệ thống đi hàng chục lần.
- Thay vào đó, em dùng các hàm native của Spark SQL: `to_date()`, `year()`, `month()`, và `dayofmonth()` trực tiếp trên DataFrame API. Các hàm này sẽ được **Tungsten Engine** biên dịch trực tiếp thành Java bytecode tối ưu (Whole-Stage Code Generation) và chạy trực tiếp trên bộ nhớ Off-heap.

### Tầng 3: Tối ưu hóa Ghi Dữ Liệu (Write Strategy)
Đây là tầng rất dễ bị bỏ qua nhưng lại quyết định hiệu năng của cả hệ thống downstream:
- Ghi 50TB ra S3 nếu không kiểm soát số lượng file sẽ tạo ra hàng trăm ngàn file nhỏ (small files problem), làm Athena sau này quét cực kỳ chậm do latency của HTTP GET request trên S3.
- Em sẽ thực hiện ghi dữ liệu kết hợp phân vùng: `.write.partitionBy("year", "month").parquet(...)`.
- Để tránh tạo ra các file quá nhỏ trong từng thư mục năm/tháng, em sẽ cấu hình thêm option `spark.sql.files.maxRecordsPerFile` (ví dụ: khoảng 5 triệu đến 10 triệu dòng/file) để Spark tự động chia file có dung lượng tối ưu từ 100MB-256MB.

### Tầng 4: Cấu Hình Tài Nguyên Cluster (Resource Tuning)
- Em sẽ cấu hình Executor theo quy tắc **"5 Cores/Executor"** (`spark.executor.cores=5`). Đây là điểm cân bằng lý tưởng giúp tối đa hóa throughput đọc ghi HDFS/S3 mà không bị nghẽn Garbage Collection (GC) của Java.
- Dung lượng RAM cho mỗi Executor sẽ set khoảng **16GB đến 32GB** tùy thuộc vào độ rộng của schema gốc để tránh lỗi Out Of Memory (OOM).
- Driver Memory cần set tối thiểu **8GB đến 16GB** vì nó phải quản lý metadata của hơn 100.000 tasks.

### Tầng 5: Tính Ổn Định và Khả Năng Tự Phục Hồi (Reliability)
Với quy mô 200.000 tasks chạy trên một cluster lớn, xác suất có một vài node bị lỗi mạng hoặc đĩa cứng bị chậm dẫn đến "straggler tasks" (task chạy lẹt đẹt kéo dài cả tiếng) là gần như 100%.
- Em sẽ bật **Speculative Execution** (`spark.speculation=true`). Khi Spark phát hiện có task chạy chậm hơn bất thường so với trung bình, nó sẽ tự động chạy một bản sao của task đó trên node khác. Task nào xong trước sẽ được lấy kết quả.
- Bật **Adaptive Query Execution (AQE)** để Spark tự động gộp các partition nhỏ sau khi ghi nếu có biến động dữ liệu.”*

---

## PHẦN 3: CÁC CÂU HỎI ĐÀO SÂU (FOLLOW-UP QUESTIONS)

### Câu hỏi 1 (Về Quy Mô Dữ Liệu)
**Interviewer:**  
*“Tốt lắm. Vậy nếu dữ liệu của tôi không phải 50TB mà chỉ là **100 Gigabytes**, bạn có thay đổi gì trong thiết kế này không?”*

**Candidate:**  
*“Dạ có, thay đổi rất lớn ạ. Đây chính là tư duy tránh 'Mổ gà dùng dao mổ trâu'.
- Với 100GB dữ liệu, việc khởi động một EMR cluster lớn sẽ tốn thời gian hơn cả thời gian chạy tính toán thực tế.
- Em sẽ đề xuất **không dùng Spark cluster**. Thay vào đó, em có thể dùng **DuckDB** hoặc **Polars/Pandas** (đọc dạng chunk) chạy trên một máy ảo đơn lẻ (ví dụ instance `m5.4xlarge` có 16 cores, 64GB RAM). DuckDB xử lý dữ liệu dạng cột trên một máy cực kỳ nhanh và chi phí rẻ hơn Spark rất nhiều.
- Nếu bắt buộc dùng Spark vì hệ thống chung, em chỉ chạy ở chế độ **Standalone / Local mode** (`local[*]`) trên 1 node duy nhất, set `spark.sql.shuffle.partitions` xuống khoảng 10-20 thay vì để mặc định, để tránh overhead tạo task không cần thiết.”*

---

### Câu hỏi 2 (Về Thay Đổi Yêu Cầu - Bổ Sung Wide Transformation)
**Interviewer:**  
*“Nếu bây giờ yêu cầu thay đổi: Không chỉ ghi lại dữ liệu, mà tôi muốn bạn tính tổng doanh thu (`total_revenue`) group by theo `year` và `month` từ 50TB dữ liệu đó. Bạn sẽ thay đổi chiến lược như thế nào?”*

**Candidate:**  
*“Dạ, khi yêu cầu có thêm **Group By**, job này đã chuyển từ *Narrow Transformation* sang *Wide Transformation*. Chúng ta bắt buộc phải đối mặt với **Shuffle** - dữ liệu phải gom theo năm/tháng về các executor để tính tổng. Chiến lược tối ưu lúc này sẽ là:

1.  **Map-Side Aggregation (Pre-aggregation):** May mắn là DataFrame API của Spark SQL tự động thực hiện pre-aggregation (tính tổng cục bộ trên từng executor trước khi shuffle qua mạng).
2.  **Tối ưu Shuffle Partitions:** Tham số mặc định `spark.sql.shuffle.partitions=200` chắc chắn sẽ gây OOM vì 50TB chia cho 200 partition thì mỗi partition nặng tới 250GB (vượt quá RAM executor). Em sẽ tăng tham số này lên **2000 hoặc 4000** để mỗi partition sau shuffle chỉ nặng khoảng 10-20GB.
3.  **Xử lý Lệch Dữ Liệu (Data Skew):** Có thể có những năm/tháng có lượng giao dịch đột biến (ví dụ tháng khuyến mãi). Em sẽ bật tính năng tự động tối ưu hóa Skew Join/Aggregation của AQE bằng cấu hình:
    `spark.sql.adaptive.skewJoin.enabled=true`.
    Nếu AQE không xử lý hết, em sẽ áp dụng kỹ thuật **Salting** (thêm khóa ngẫu nhiên vào group by key để phân tán dữ liệu bị lệch sang nhiều partition khác nhau, sau đó group by lần 2 để ra kết quả cuối).”*

---

### Câu hỏi 3 (Về Lỗi Thực Tế - Out Of Memory)
**Interviewer:**  
*“Trong quá trình chạy job 50TB này, nếu bạn thấy log báo lỗi `Container killed by YARN for exceeding memory limits`. Bạn sẽ debug và sửa như thế nào?”*

**Candidate:**  
*“Lỗi này rất kinh điển trong môi trường YARN. Nó nghĩa là tổng lượng RAM thực tế tiến trình Executor tiêu thụ (bao gồm cả JVM Heap và Off-heap memory) đã vượt quá mức YARN cấp phát cho container đó. Cách em xử lý:

1.  **Kiểm tra xem có rò rỉ bộ nhớ ở Off-heap không:** Có thể do cấu hình `spark.memory.offHeap.enabled=true` nhưng set size quá lớn, hoặc do dùng các thư viện C/Python bên ngoài tiêu thụ RAM ngoài JVM.
2.  **Tăng Memory Overhead:** Em sẽ tăng cấu hình `spark.executor.memoryOverhead` (mặc định là 10% memory của executor). Em sẽ tăng lên 15% hoặc 20% để cho phép Executor có thêm khoảng trống bộ nhớ đệm.
3.  **Kiểm tra Data Skew:** Vào Spark UI, xem tab *Stages*, sắp xếp các task theo *Task Duration* hoặc *Shuffle Read Size*. Nếu có 1 vài task chạy cực lâu và ngốn bộ nhớ gấp nhiều lần các task khác, đó là do lệch dữ liệu. Em sẽ áp dụng Salting hoặc repartition lại dữ liệu đầu vào.
4.  **Giảm số Cores per Executor:** Nếu đang để 5 cores, em có thể giảm xuống 4 hoặc 3 cores để giảm số lượng tasks chạy song song trong cùng 1 executor JVM, qua đó giảm lượng RAM tiêu thụ đồng thời.”*

---

## PHẦN 4: ĐÁNH GIÁ TIÊU CHÍ THÀNH CÔNG (INTERVIEWER CHECKLIST)

Để đạt điểm tối đa trong mắt nhà tuyển dụng, bạn cần thể hiện được:

*   [x] **Tư duy thực tế (Pragmatism):** Không cuồng công cụ. Sẵn sàng nói "100GB thì không dùng Spark" để chứng minh mình quan tâm đến chi phí và độ phức tạp của hệ thống.
*   [x] **Hiểu sâu bản chất phần cứng:** Biết giới hạn 5 cores/executor là do I/O bottleneck và GC pauses.
*   [x] **Hiểu cơ chế lưu trữ phân tán:** Biết tác hại của "small files problem" trên S3/HDFS và có giải pháp cấu hình cụ thể để khống chế kích thước file đầu ra.
*   [x] **Không học vẹt config:** Giải thích được *tại sao* tăng `maxPartitionBytes` lại giúp Driver chạy nhẹ hơn (giảm số lượng metadata và task scheduling).
