# 50 KỊCH BẢN PHỎNG VẤN DATA ENGINEER (CHUẨN GRAB × KIOTVIET)

> **TÀI LIỆU KHÔNG KHOAN NHƯỢNG (UNFORGIVING INTERVIEW PLAYBOOK)**
> 
> Tài liệu này được biên soạn dưới dạng kịch bản đối thoại trực diện. Mỗi kịch bản mô phỏng cách người phỏng vấn (Interviewer - INT) tìm cách bắt bẻ, đưa bạn vào bẫy, hoặc thử thách giới hạn kiến thức, và cách bạn (Candidate - CAND) phản xạ để bảo vệ giải pháp thiết kế, chứng minh năng lực bao quát toàn diện nhưng không dài dòng tự mãn.

---

## MỤC LỤC
*   [PHẦN 1: Cấu Trúc Dữ Liệu, Giải Thuật & SQL (Kịch bản 1 - 15)](#phần-1-cấu-trúc-dữ-liệu-giải-thuật--sql)
*   [PHẦN 2: Tối Ưu Hóa Hệ Thống Phân Tán - Spark & Hadoop (Kịch bản 16 - 30)](#phần-2-tối-ưu-hóa-hệ-thống-phân-tán---spark--hadoop)
*   [PHẦN 3: Thiết Kế Data Warehouse & Data Mart (Kịch bản 31 - 40)](#phần-3-thiết-kế-data-warehouse--data-mart)
*   [PHẦN 4: System Design & Kiến Trúc Pipeline E2E (Kịch bản 41 - 50)](#phần-4-system-design--kiến-trúc-pipeline-e2e)

---

## PHẦN 1: CẤU TRÚC DỮ LIỆU, GIẢI THUẬT & SQL

### Kịch bản 1: Tối ưu độ phức tạp thời gian (Time Complexity Optimization)
- **INT:** *"Cho một mảng số nguyên. Hãy tìm hai số có tổng bằng một số target cho trước. Viết code ngay lập tức."*
- **CAND:** *(Không vội viết code)* *"Dạ, dữ liệu đầu vào đã được sắp xếp chưa? Mảng có chứa số trùng lặp không, và yêu cầu trả về chỉ số (index) hay giá trị?"*
- **INT:** *"Mảng chưa được sắp xếp. Có trùng lặp. Trả về chỉ số của cặp đầu tiên. Độ phức tạp mong muốn là gì?"*
- **CAND:** *"Nếu dùng brute force (2 vòng lặp lồng nhau), độ phức tạp là $O(n^2)$ về thời gian và $O(1)$ về không gian. Để tối ưu thời gian xuống $O(n)$, em sẽ dùng một Hash Map để lưu vết giá trị đã qua và index của nó. Chi phí đánh đổi là $O(n)$ không gian bộ nhớ."*
- **INT:** *"Viết code đi. Nếu RAM cực kỳ giới hạn và không thể dùng Hash Map thì sao?"*
- **CAND:** *"Nếu RAM giới hạn $O(1)$ space, em sẽ sắp xếp mảng trước bằng Heap Sort mất $O(n \log n)$ thời gian, sau đó dùng kỹ thuật Two Pointers (hai con trỏ) quét từ hai đầu vào mất $O(n)$. Tổng chi phí thời gian lúc này là $O(n \log n)$, không gian $O(1)$."*

---

### Kịch bản 2: Xử lý chuỗi dữ liệu lớn (Memory-Efficient String Parsing)
- **INT:** *"Bạn có một log file thô 10GB. Bạn cần tìm 10 IP xuất hiện nhiều nhất. Bạn viết code Python thế nào để không bị crash RAM?"*
- **CAND:** *"10GB vượt quá RAM của các máy ảo thông thường nếu nạp cả file. Em sẽ không dùng `file.read()`. Thay vào đó, em dùng generator để stream từng dòng một. Sử dụng `collections.Counter` để đếm tần suất IP dưới dạng hash map. Counter chỉ lưu khóa là các IP duy nhất (IPv4 tối đa chỉ có $2^{32}$ địa chỉ, thực tế nhỏ hơn nhiều) nên RAM tiêu thụ sẽ rất nhỏ và ổn định."*
- **INT:** *"Nếu Counter vẫn vượt quá RAM cho phép của bạn (ví dụ RAM giới hạn chỉ 256MB)?"*
- **CAND:** *"Em sẽ áp dụng thuật toán **External Merge Sort**. Chia file 10GB thành 40 file nhỏ 250MB, đọc ghi tuần tự qua đĩa. Sort từng file nhỏ trong memory rồi dùng con trỏ k-way merge để ghép lại tuần tự, đếm tần suất IP mà không bao giờ giữ quá 256MB dữ liệu trong RAM."*

---

### Kịch bản 3: SQL Window Functions nâng cao
- **INT:** *"Tôi muốn bạn tìm khách hàng có lượng đơn hàng tăng trưởng liên tục trong 3 tháng gần nhất từ bảng `sales`. Viết câu query."*
- **CAND:** *"Để xác định tính tăng trưởng liên tục, em sẽ dùng hàm `LAG()` để lấy doanh thu của 2 tháng trước đó, sau đó so sánh theo điều kiện hình bậc thang."*
- **INT:** *"Viết query đi. Chú ý trường hợp tháng đó khách hàng không mua hàng (không có dòng dữ liệu)."*
- **CAND:** *(Viết query)*  
  ```sql
  WITH monthly_sales AS (
    SELECT customer_id, DATE_TRUNC('month', order_date) as month, SUM(amount) as revenue
    FROM sales GROUP BY 1, 2
  ),
  lagged_sales AS (
    SELECT customer_id, revenue,
           LAG(revenue, 1) OVER(PARTITION BY customer_id ORDER BY month) as prev_1,
           LAG(revenue, 2) OVER(PARTITION BY customer_id ORDER BY month) as prev_2
    FROM monthly_sales
  )
  SELECT DISTINCT customer_id FROM lagged_sales
  WHERE revenue > prev_1 AND prev_1 > prev_2 AND prev_1 IS NOT NULL AND prev_2 IS NOT NULL;
  ```
- **INT:** *"Query này sai nếu khách hàng bỏ cách 1 tháng. Làm sao điền giá trị 0 cho các tháng không có giao dịch trước khi dùng Window Function?"*
- **CAND:** *"Em cần tạo một danh sách đầy đủ tất cả các tháng bằng `cross join` giữa tập khách hàng duy nhất và danh sách tháng sinh tự động (dùng `generate_series`), sau đó `LEFT JOIN` với bảng doanh thu thực tế và dùng `COALESCE(revenue, 0)` để lấp đầy các tháng trống."*

---

### Kịch bản 4: Thuật toán đồ thị tìm Dependency (DAG Validation)
- **INT:** *"Làm thế nào để phát hiện một DAG (đồ thị có hướng không chu trình) trong Airflow có bị vòng lặp vô hạn (circular dependency) hay không?"*
- **CAND:** *"Bản chất là tìm chu trình trong đồ thị có hướng (Detect Cycle in a Directed Graph). Thuật toán tối ưu nhất là sử dụng **DFS (Depth First Search)** kết hợp tô màu các đỉnh (Trắng: chưa thăm, Xám: đang thăm trong nhánh hiện tại, Đen: đã thăm xong). Nếu trong quá trình DFS đi qua một đỉnh có màu Xám, chứng tỏ đồ thị có chu trình."*
- **INT:** *"Độ phức tạp là bao nhiêu? Có thuật toán nào khác không?"*
- **CAND:** *"Độ phức tạp là $O(V + E)$ với V là số đỉnh (tasks) và E là số cạnh (dependencies). Một thuật toán khác là **Kahn's Algorithm** (dựa trên Queue và In-degree - bán bậc vào). Nếu sau khi duyệt hết đồ thị mà số lượng node trong danh sách topo sort ít hơn tổng số node ban đầu, đồ thị có chu trình."*

---

### Kịch bản 5: SQL Query Optimization (Index vs Table Scan)
- **INT:** *"Câu query này chạy mất 10 phút: `SELECT * FROM orders WHERE DATE(created_at) = '2026-07-17'`. Bảng có index trên cột `created_at`. Tại sao?"*
- **CAND:** *"Do query sử dụng hàm `DATE(created_at)`. Việc áp dụng hàm lên cột trong điều kiện `WHERE` sẽ vô hiệu hóa index (Index Suppression), khiến Database Engine phải thực hiện Full Table Scan và tính toán hàm `DATE` cho từng dòng dữ liệu."*
- **INT:** *"Sửa thế nào?"*
- **CAND:** *"Em sẽ viết lại dưới dạng so sánh khoảng giá trị để Database sử dụng được Index Range Scan:*
  ```sql
  SELECT * FROM orders 
  WHERE created_at >= '2026-07-17 00:00:00' 
    AND created_at < '2026-07-18 00:00:00';
  ```

---

### Kịch bản 6: Cấu trúc dữ liệu cho Real-time Deduplication
- **INT:** *"Bạn cần lọc trùng (deduplicate) ID người dùng cho một stream dữ liệu 1 tỷ events/ngày trong thời gian thực. RAM giới hạn 1GB. Dùng cấu trúc dữ liệu nào?"*
- **CAND:** *"1 tỷ ID dạng string nếu lưu trong Set thông thường sẽ ngốn khoảng 8-16GB RAM. Em sẽ dùng **Bloom Filter**. Đây là cấu trúc dữ liệu xác suất sử dụng bit array và nhiều hàm hash. Nó cực kỳ tiết kiệm bộ nhớ (1 tỷ ID chỉ cần khoảng 1-2GB RAM) với đánh đổi là có tỉ lệ dương tính giả (false positive - báo trùng nhưng thực tế không trùng), nhưng hoàn toàn không có âm tính giả (false negative)."*
- **INT:** *"Nếu không chấp nhận tỉ lệ sai sót (yêu cầu độ chính xác 100%)?"*
- **CAND:** *"Nếu bắt buộc chính xác 100%, em sẽ dùng **Redis HyperLogLog** để đếm unique (nếu chỉ cần đếm cardinality). Còn nếu cần lọc trùng chính xác từng record để ghi xuống, em phải kết hợp Bloom Filter làm bộ lọc nhanh tầng đầu, và một đĩa lưu trữ dạng Key-Value (ví dụ RocksDB/Redis) làm tầng kiểm tra cuối cùng."*

---

### Kịch bản 7: Thuật toán Merge K Sorted Lists (Data Ingestion)
- **INT:** *"Bạn nhận dữ liệu log từ 10 servers khác nhau, mỗi server đã tự sort log theo timestamp. Hãy merge chúng thành 1 stream duy nhất đã sort."*
- **CAND:** *"Đây là bài toán **Merge K Sorted Lists**. Em sẽ sử dụng cấu trúc dữ liệu **Min-Heap (Priority Queue)**. 
  1. Nạp phần tử đầu tiên của cả 10 server vào Min-Heap (Heap size = 10).
  2. Lấy phần tử nhỏ nhất ra khỏi Heap ghi vào output.
  3. Lấy phần tử tiếp theo từ server có phần tử vừa bị lấy ra nạp vào Heap.
  4. Lặp lại cho đến khi hết dữ liệu."*
- **INT:** *"Độ phức tạp thời gian của giải pháp này?"*
- **CAND:** *"Độ phức tạp là $O(N \log K)$ với N là tổng số phần tử của 10 servers, và K là số lượng servers ($K=10$). Bộ nhớ tiêu thụ cực nhỏ, chỉ bằng $O(K)$."*

---

### Kịch bản 8: SQL Subquery vs JOIN Performance
- **INT:** *"Query A: `SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)`. Query B: `SELECT DISTINCT u.* FROM users u JOIN orders o ON u.id = o.user_id`. Query nào tốt hơn?"*
- **CAND:** *"Hầu hết các RDBMS hiện đại (Optimizer thông minh) sẽ biên dịch cả hai về cùng một execution plan (thường là Semi-Join). Tuy nhiên, nếu Optimizer không tối ưu:
  - Query A (IN subquery) tốt hơn nếu bảng `orders` cực lớn và có nhiều dòng trùng lặp `user_id`, vì nó dừng quét ngay khi tìm thấy dòng match đầu tiên (Semi-join).
  - Query B (JOIN + DISTINCT) tệ hơn vì nó thực hiện phép Join vật lý tạo ra tập dữ liệu trùng lặp khổng lồ trước, rồi mới tốn thêm chi phí Sort/Hash để loại bỏ trùng lặp (`DISTINCT`)."*

---

### Kịch bản 9: Kiểm soát lỗi tràn bộ nhớ đệm (Stack Overflow / Recursion)
- **INT:** *"Bạn viết một hàm đệ quy để duyệt data lineage của các bảng. Dữ liệu lineage có độ sâu 10,000 cấp. Chương trình bị báo lỗi Stack Overflow. Xử lý thế nào?"*
- **CAND:** *"Nguyên nhân là do mỗi lần gọi đệ quy, một stack frame mới được đẩy vào Call Stack của hệ thống, vượt quá giới hạn của ngôn ngữ. Em có 2 cách xử lý:
  - Cách 1: Khử đệ quy bằng cách chuyển sang thuật toán lặp (Iterative) sử dụng một cấu trúc dữ liệu **Stack tự định nghĩa lưu trên Heap memory** (nơi có dung lượng lớn hơn Call Stack rất nhiều).
  - Cách 2: Nếu dùng ngôn ngữ hỗ trợ, tối ưu hóa bằng **Tail Recursion (Đệ quy đuôi)** để compiler tự động tái sử dụng lại stack frame hiện tại."*

---

### Kịch bản 10: SQL handling NULL values
- **INT:** *"Bảng `employees` có 10 dòng. Cột `bonus` có 3 dòng chứa giá trị `NULL`. Kết quả của câu query này trả về mấy dòng: `SELECT * FROM employees WHERE bonus <> 1000`?"*
- **CAND:** *"Query trả về ít hơn 7 dòng (chỉ những dòng có bonus khác 1000 và **không phải NULL**). Trong SQL, mọi phép so sánh với `NULL` (ví dụ: `NULL <> 1000` hoặc `NULL = 1000`) đều trả về kết quả là `UNKNOWN` chứ không phải `TRUE`. Do đó các dòng có bonus là `NULL` sẽ bị loại bỏ khỏi kết quả."*
- **INT:** *"Làm sao để lấy được cả các dòng NULL?"*
- **CAND:** *"Phải viết rõ điều kiện kiểm tra NULL:*
  ```sql
  WHERE bonus <> 1000 OR bonus IS NULL;
  -- Hoặc dùng COALESCE:
  WHERE COALESCE(bonus, 0) <> 1000;
  ```"

---

### Kịch bản 11: Binary Search trên dữ liệu phân tán
- **INT:** *"Bạn có một danh sách sorted chứa 10 tỷ dòng lưu trên HDFS. Bạn cần tìm kiếm một giá trị cụ thể. Bạn có dùng Binary Search được không? Tại sao?"*
- **CAND:** *"Về mặt lý thuyết là được ($O(\log n)$), nhưng thực tế trên hệ thống phân tán thì **không nên**. Binary Search yêu cầu truy cập ngẫu nhiên (Random Access) dựa trên index phần tử giữa. Trên HDFS, việc truy cập ngẫu nhiên qua các block dữ liệu nằm rải rác trên nhiều DataNode qua mạng sẽ sinh ra overhead rất lớn. Thay vào đó, ta nên sử dụng **Index-based search** (như lưu index dạng B-Tree hoặc dùng LSM-tree ở tầng trên để chỉ ra chính xác block chứa dữ liệu cần tìm)."*

---

### Kịch bản 12: SQL Joins on Non-Key Columns
- **INT:** *"Điều gì xảy ra khi bạn thực hiện JOIN hai bảng lớn trên một cột không có index và có nhiều giá trị trùng lặp (nhiều-nhiều)?"*
- **CAND:** *"Database Engine sẽ bị nghẽn nghiêm trọng:
  1. Nó không dùng được Index Nested Loop Join, buộc phải chuyển sang **Hash Join** hoặc **Sort-Merge Join** (tốn nhiều RAM và CPU để build hash table).
  2. Phép Join nhiều-nhiều (Many-to-Many) sẽ tạo ra hiện tượng bùng nổ dữ liệu (Cartesian product cục bộ cho các giá trị trùng lặp), làm tăng đột biến số lượng dòng ghi ra bộ nhớ đệm và disk."*

---

### Kịch bản 13: Memory-efficient Cache Eviction Algorithm
- **INT:** *"Hãy thiết kế một bộ nhớ đệm (Cache) giới hạn kích thước N. Khi đầy, cần loại bỏ phần tử ít được sử dụng nhất (LRU - Least Recently Used). Dùng cấu trúc dữ liệu nào để các thao tác `get` và `put` đều đạt $O(1)$?"*
- **CAND:** *"Em sẽ kết hợp 2 cấu trúc dữ liệu:
  1. Một **Hash Map**: Giúp tìm kiếm phần tử nhanh với độ phức tạp $O(1)$.
  2. Một **Doubly Linked List (Danh sách liên kết kép)**: Giúp duy trì thứ tự sử dụng của các phần tử. Khi một phần tử được truy cập hoặc thêm mới, ta di chuyển nó lên đầu danh sách trong $O(1)$. Phần tử cuối danh sách chính là LRU và có thể xóa đi trong $O(1)$."*

---

### Kịch bản 14: SQL Aggregation vs Subquery Performance
- **INT:** *"Viết query tìm ID của các đơn hàng có số tiền lớn hơn số tiền trung bình của tất cả các đơn hàng trong ngày hôm đó."*
- **CAND:** *"Em sẽ dùng Window Function thay vì viết Subquery để tránh việc Database quét bảng `orders` hai lần:*
  ```sql
  WITH orders_with_avg AS (
    SELECT order_id, amount,
           AVG(amount) OVER(PARTITION BY DATE(created_at)) as daily_avg
    FROM orders
  )
  SELECT order_id FROM orders_with_avg WHERE amount > daily_avg;
  ```"

---

### Kịch bản 15: Phân tích tiệm cận thời gian (Amortized Complexity)
- **INT:** *"Cấu trúc dữ liệu Dynamic Array (như `list` trong Python hoặc `ArrayList` trong Java) có độ phức tạp khi append phần tử cuối là bao nhiêu?"*
- **CAND:** *"Độ phức tạp là **$O(1)$ Amortized (trung bình tích lũy)**. 
  - Hầu hết các lần append chỉ mất $O(1)$ vì mảng vẫn còn chỗ trống.
  - Tuy nhiên, khi mảng bị đầy, hệ thống phải cấp phát một mảng mới lớn gấp đôi và copy toàn bộ $n$ phần tử cũ sang mảng mới, mất $O(n)$ thời gian. Vì việc này xảy ra rất thưa thớt (sau mỗi $n$ lần append), nên chi phí trung bình chia đều cho mỗi lần append vẫn là $O(1)$."*

---

## PHẦN 2: TỐI ƯU HÓA HỆ THỐNG PHÂN TÁN - SPARK & HADOOP

### Kịch bản 16: Tối ưu hóa Spark Job bị tràn bộ nhớ (Spill to Disk)
- **INT:** *"Trên Spark UI, bạn thấy thông số: `Spill (Memory): 10GB`, `Spill (Disk): 2GB`. Điều này nghĩa là gì và bạn xử lý thế nào?"*
- **CAND:** *"Nghĩa là trong quá trình thực hiện Wide Transformation (như GroupBy hoặc Join), dung lượng dữ liệu của một partition vượt quá lượng RAM được cấp phát cho một execution task. Spark buộc phải tuần tự hóa (serialize) dữ liệu trong RAM ghi tạm xuống disk để giải phóng bộ nhớ, sau đó đọc lại. Việc này làm giảm hiệu năng hệ thống nghiêm trọng do nghẽn Disk I/O.
  - Giải pháp 1: Tăng số lượng `spark.sql.shuffle.partitions` lên để chia nhỏ dung lượng mỗi partition.
  - Giải pháp 2: Tăng RAM cho mỗi executor (`spark.executor.memory`).
  - Giải pháp 3: Kiểm tra xem có bị lệch dữ liệu (Data Skew) ở một vài task chạy chậm hay không."*

---

### Kịch bản 17: Tại sao GroupByKey lại tệ hơn ReduceByKey?
- **INT:** *"Tại sao trong RDD API, tôi luôn bị cấm dùng `groupByKey` mà phải thay bằng `reduceByKey`?"*
- **CAND:** *"Vì `groupByKey` không thực hiện gom cụm dữ liệu tại chỗ (Map-Side Combination). Nó sẽ truyền toàn bộ các cặp Key-Value thô qua mạng (Shuffle) đến Executor đích, gây nghẽn băng thông mạng cực kỳ nặng.
  - Ngược lại, `reduceByKey` tự động thực hiện gộp dữ liệu cục bộ trên từng partition trước khi shuffle (Map-side reduction). Lượng dữ liệu truyền qua mạng lúc này giảm đi hàng chục lần."*

---

### Kịch bản 18: Adaptive Query Execution (AQE) trong thực tế
- **INT:** *"AQE tối ưu hóa truy vấn như thế nào? Kể tên 3 tính năng tự động của nó."*
- **CAND:** *"AQE tối ưu hóa kế hoạch thực thi trực tiếp lúc chạy (Runtime Plan Optimization) thông qua việc phân tích thống kê từ các stage đã hoàn thành:
  1. **Coalescing Post-Shuffle Partitions**: Tự động gộp các partition quá nhỏ sau khi shuffle để giảm số lượng task con chạy sau.
  2. **Convert Sort-Merge Join to Broadcast Join**: Nếu sau khi filter, một bảng thực tế nhỏ hơn ngưỡng quy định, AQE đổi thuật toán join mà không cần chạy shuffle.
  3. **Skew Join Optimization**: Tự động phát hiện các partition bị lệch (skewed) và chia nhỏ chúng để xử lý song song."*

---

### Kịch bản 19: Spark Memory Overhead Configuration
- **INT:** *"Executor của bạn có `spark.executor.memory = 16GB`. Bạn chạy job và bị YARN kill container. Bạn tăng memory lên 20GB vẫn bị kill. Tại sao?"*
- **CAND:** *"Lỗi này do tiến trình Off-heap memory (như Overhead memory hoặc bộ nhớ của các tiến trình Python UDF ngoài JVM) vượt quá giới hạn YARN container. YARN giám sát cả vùng RAM vật lý (Physical memory) của cả container chứ không chỉ JVM Heap.
  - Giải pháp: Không chỉ tăng `spark.executor.memory` mà phải tăng **`spark.executor.memoryOverhead`** (mặc định chỉ 10% RAM Executor). Em sẽ nâng cấu hình này lên 20-30% hoặc kiểm tra lại xem có sử dụng thư viện C/C++ hoặc PySpark UDF không kiểm soát RAM hay không."*

---

### Kịch bản 20: Broadcast Join Memory Limitations
- **INT:** *"Tôi muốn ép Spark sử dụng Broadcast Join cho một bảng nặng 5GB. Cấu hình thế nào và rủi ro là gì?"*
- **CAND:** *"Cấu hình bằng cách tăng `spark.sql.autoBroadcastJoinThreshold` lên 5GB (hoặc dùng hint `broadcast()`).
  - **Rủi ro cực lớn**: Spark Driver sẽ phải tải toàn bộ bảng 5GB này về bộ nhớ của Driver trước để serialize và broadcast sang tất cả các executor. Nếu Driver Memory không được set tối thiểu trên 10GB, Driver sẽ bị crash **Out Of Memory (OOM)** ngay lập tức. Mỗi executor cũng phải chứa thêm 5GB RAM tĩnh cho bảng này, làm giảm RAM khả dụng cho các tác vụ tính toán khác."*

---

### Kịch bản 21: Bản chất vật lý của HDFS Block Size
- **INT:** *"Tại sao HDFS lại để block size mặc định rất lớn (128MB hoặc 256MB) trong khi hệ điều hành thông thường chỉ để block size 4KB?"*
- **CAND:** *"HDFS thiết kế để lưu trữ và xử lý các file cực kỳ lớn (Big Data). 
  - Nếu chọn block size nhỏ (ví dụ 4KB), một file 1TB sẽ sinh ra hàng trăm triệu blocks.
  - Toàn bộ metadata quản lý danh sách block này phải lưu trên RAM của **NameNode**. Lượng metadata khổng lồ này sẽ làm sập RAM NameNode ngay lập tức. Ngoài ra block size lớn giúp giảm thiểu thời gian tìm kiếm đĩa (Disk Seek Time) khi đọc ghi tuần tự."*

---

### Kịch bản 22: MapReduce Word Count Map-side Combine
- **INT:** *"Trong lập trình MapReduce truyền thống, vai trò của `Combiner` là gì? Khi nào không thể dùng Combiner?"*
- **CAND:** *"Combiner đóng vai trò là một 'Mini-Reducer' chạy ngay sau bước Map trên cùng một node để giảm thiểu dung lượng dữ liệu ghi ra đĩa và gửi qua mạng.
  - **Điều kiện sử dụng**: Hàm tính toán phải có tính chất giao hoán (commutative) và kết hợp (associative).
  - *Ví dụ*: Tính SUM, MAX, MIN dùng được Combiner. Tính **AVERAGE (Trung bình cộng)** thì **KHÔNG** dùng được Combiner trực tiếp vì $\text{Average}(A, B, C) \neq \text{Average}(\text{Average}(A, B), C)$."*

---

### Kịch bản 23: Kryo Serializer vs Java Serializer
- **INT:** *"Kryo Serializer giúp tăng tốc Spark Job như thế nào? Tại sao mặc định Spark không dùng Kryo?"*
- **CAND:** *"Kryo serialize dữ liệu thành dạng nhị phân siêu nhỏ (nhỏ hơn 2-10x so với Java) và nhanh hơn nhiều, giúp giảm dung lượng dữ liệu truyền qua mạng (Shuffle) và ghi xuống đĩa tạm.
  - Mặc định Spark không bật vì Kryo yêu cầu lập trình viên phải đăng ký trước (register) các custom classes của họ với Kryo để đạt hiệu quả cao nhất. Nếu không đăng ký, Kryo phải lưu thêm toàn bộ tên class kèm theo dữ liệu, làm mất đi lợi thế dung lượng."*

---

### Kịch bản 24: Spark Lineage Graph & RDD Fault Tolerance
- **INT:** *"Nếu một node tính toán của Spark bị chết giữa chừng, Spark làm thế nào để khôi phục lại dữ liệu bị mất mà không cần chạy lại từ đầu nguồn?"*
- **CAND:** *"Spark dựa vào đồ thị **DAG Lineage (Phả hệ dữ liệu)**. Mỗi DataFrame/RDD đều lưu lại lịch sử các bước biến đổi của nó (ví dụ: Read $\rightarrow$ Filter $\rightarrow$ Map).
  - Khi một partition dữ liệu trên Executor bị mất, Driver chỉ cần gửi task tính toán lại **đúng partition bị mất đó** dựa trên lineage của nó, các partition khác đã hoàn thành không bị ảnh hưởng."*

---

### Kịch bản 25: Dynamic Partition Insufficient Resources
- **INT:** *"Bạn bật Dynamic Allocation. Job của bạn cần chạy 100 executors. Hệ thống cấp phát động tăng dần số lượng executor, nhưng sau đó đột ngột giảm và treo job. Tại sao?"*
- **CAND:** *"Đây thường là do lỗi tranh chấp tài nguyên trên Cluster Manager (như YARN queue). 
  - YARN thu hồi lại các container của Spark vì có ứng dụng có độ ưu tiên cao hơn nhảy vào.
  - Khi mất Executor, Spark phải tính toán lại các partition bị mất. Việc tính toán lại này lại yêu cầu tài nguyên, tạo ra vòng lặp vô hạn (resource starvation) khiến job bị treo."*

---

### Kịch bản 26: MapReduce Word Count Python Streaming Bottleneck
- **INT:** *"Khi viết MapReduce bằng Python sử dụng Hadoop Streaming, bottleneck lớn nhất nằm ở đâu?"*
- **CAND:** *"Bottleneck nằm ở **I/O Overhead** giữa JVM của Hadoop và Python Process. Hadoop Streaming giao tiếp với Python qua Standard Input/Output (sys.stdin/sys.stdout) dưới dạng văn bản thô (Text). Việc serialize dữ liệu thành string để truyền qua pipe và deserialize ngược lại trong Python tốn cực kỳ nhiều CPU."*

---

### Kịch bản 27: HDFS NameNode Federation vs High Availability
- **INT:** *"Phân biệt HDFS HA và HDFS Federation."*
- **CAND:** *
  - **HDFS HA (High Availability)**: Chạy 2 NameNode (Active/Standby) để tránh Single Point of Failure. Chúng chia sẻ cùng một không gian tên (Namespace).
  - **HDFS Federation**: Cho phép chạy nhiều NameNode độc lập để scale-out Namespace. Mỗi NameNode quản lý một phần thư mục riêng (ví dụ NN1 quản lý `/user`, NN2 quản lý `/data`), dùng chung các DataNode ở bên dưới để lưu trữ blocks."*

---

### Kịch bản 28: Spark Caching Eviction Policies
- **INT:** *"Nếu bạn cache một DataFrame bằng `.cache()` nhưng RAM của executor bị đầy, Spark sẽ làm gì?"*
- **CAND:** *"Mặc định `.cache()` sử dụng `MEMORY_AND_DISK`. Nếu RAM đầy, Spark sẽ tự động đẩy các partition cũ nhất (theo thuật toán LRU) ghi xuống đĩa cứng (Disk) của executor node. Job vẫn tiếp tục chạy bình thường, chỉ bị giảm hiệu năng khi cần đọc lại các partition trên Disk."*

---

### Kịch bản 29: Spark Job Speculation Side-effects
- **INT:** *"Tại sao chúng ta phải tắt Speculative Execution (`spark.speculation=false`) khi Spark Job thực hiện ghi dữ liệu trực tiếp vào cơ sở dữ liệu quan hệ (RDBMS)?"*
- **CAND:** *"Vì Speculative Execution sẽ chạy 2 task bản sao song song cho cùng một partition dữ liệu. Nếu task ghi vào RDBMS, cả hai bản sao sẽ thực hiện ghi đồng thời $\rightarrow$ dẫn đến **trùng lặp dữ liệu** hoặc xung đột khóa (Primary Key Conflict) trong Database."*

---

### Kịch bản 30: YARN Schedulers (FIFO vs Capacity vs Fair)
- **INT:** *"Trong môi trường production dùng chung, YARN Scheduler nào tốt nhất để đảm bảo các job nhỏ không bị nghẽn bởi các job lớn?"*
- **CAND:** *"Nên dùng **Fair Scheduler** (hoặc **Capacity Scheduler** được chia queue). 
  - Fair Scheduler tự động phân chia tài nguyên đều cho các ứng dụng đang chạy. Khi có job nhỏ nhảy vào, hệ thống sẽ thu hồi bớt một phần container của job lớn để cấp cho job nhỏ, đảm bảo job nhỏ hoàn thành nhanh chóng."*

---

## PHẦN 3: THIẾT KẾ DATA WAREHOUSE & DATA MART

### Kịch bản 31: Thiết kế Surrogate Key vs Natural Key
- **INT:** *"Tại sao trong Data Warehouse, chúng ta luôn phải tạo ra các khóa thay thế (Surrogate Keys) dạng tự tăng (BigInt) cho các bảng Dimension, thay vì dùng trực tiếp Khóa tự nhiên (Natural Keys) từ DB nguồn?"*
- **CAND:** *"Có 3 lý do cốt lõi:
  1. **Hiệu năng**: Join trên khóa kiểu số (BigInt) nhanh hơn rất nhiều so với kiểu chuỗi (String) hoặc khóa ghép phức tạp.
  2. **Quản lý lịch sử thay đổi (SCD Type 2)**: Một khách hàng thay đổi địa chỉ sẽ sinh ra dòng mới trong bảng Dim. Nếu dùng Natural Key (ví dụ `customer_id`), ta không thể lưu 2 dòng cho cùng một ID. Surrogate Key cho phép làm điều này.
  3. **Tích hợp nguồn**: Nếu ta gộp dữ liệu từ 2 hệ thống nguồn khác nhau có chung dải ID, Surrogate Key giúp tránh xung đột trùng ID."*

---

### Kịch bản 32: Thiết kế bảng Fact (Fact Table Grain)
- **INT:** *"Làm thế nào để thiết kế một bảng Fact bán hàng chứa cả giao dịch chi tiết (mức dòng sản phẩm) và chi phí vận chuyển (mức hóa đơn)?"*
- **CAND:** *"Đây là lỗi **Mismatch Grain (Sai lệch mức độ chi tiết)**. Bắt buộc phải đưa bảng Fact về cùng một Grain thấp nhất (nhỏ nhất).
  - Giải pháp: Phân bổ (allocate) chi phí vận chuyển của hóa đơn xuống từng dòng sản phẩm (ví dụ phân bổ theo tỷ lệ giá trị sản phẩm). Cả hóa đơn và dòng sản phẩm lúc này có chung grain."*

---

### Kịch bản 33: Tối ưu hóa SCD Type 2 ở quy mô lớn
- **INT:** *"Làm thế nào để thực hiện cập nhật SCD Type 2 hàng ngày trên bảng Dimension khách hàng có 500 triệu dòng bằng Spark?"*
- **CAND:** *"Nếu dùng update từng dòng truyền thống sẽ cực kỳ chậm.
  - Giải pháp tối ưu: Sử dụng **Delta Lake / Iceberg** và thực hiện câu lệnh `MERGE INTO`. Spark sẽ tối ưu hóa việc so sánh hash của các trường cần track, tự động ghi đè các dòng cũ thành `is_current = false` và append các dòng mới trong một giao dịch ACID duy nhất."*

---

### Kịch bản 34: Star Schema vs Snowflake Schema Performance
- **INT:** *"Tại sao các công cụ phân tích (BI Tools) luôn yêu cầu Star Schema thay vì Snowflake Schema?"*
- **CAND:** *"Vì Star Schema đã phi chuẩn hóa (denormalize) các bảng Dimension. Khi BI Tool sinh câu truy vấn SQL, nó chỉ cần thực hiện phép Join 1 cấp giữa bảng Fact và bảng Dimension. Snowflake Schema chuẩn hóa dữ liệu $\rightarrow$ yêu cầu Join nhiều cấp, gây suy giảm hiệu năng nghiêm trọng khi số lượng dòng của bảng Fact lên tới hàng tỷ dòng."*

---

### Kịch bản 35: Xử lý dữ liệu đến muộn (Late-arriving Data) trong Fact
- **INT:** *"Một giao dịch bán hàng xảy ra ngày hôm qua nhưng hôm nay mới được nạp vào hệ thống. Làm sao để ghi nhận vào bảng Fact mà không làm hỏng báo cáo theo thời gian?"*
- **CAND:** *"Trong bảng Fact, ta có 2 cột ngày tháng:
  1. `sales_date_key` (Ngày xảy ra giao dịch vật lý - trỏ tới bảng Dim_Date của hôm qua).
  2. `ingested_timestamp` (Thời gian nạp dữ liệu thực tế - lưu thời điểm hôm nay).
  Báo cáo doanh thu tài chính sẽ dựa vào `sales_date_key` để đảm bảo tính chính xác của ngày xảy ra giao dịch."*

---

### Kịch bản 36: Thiết kế Semi-additive Facts
- **INT:** *"Trường `inventory_balance` (Số dư kho) trong bảng Fact kho hàng thuộc loại gì? Làm thế nào để tính tổng của nó?"*
- **CAND:** *"Đây là một **Semi-additive Fact (Chỉ số cộng một phần)**. Ta có thể cộng số dư kho theo Dimension Sản phẩm hoặc Cửa hàng, nhưng **không thể** cộng dồn theo Dimension Thời gian (không thể lấy số dư của ngày 1 + ngày 2 + ngày 3 để ra số dư cả tháng).
  - Giải pháp: Khi tính tổng theo thời gian, ta phải dùng hàm lấy giá trị cuối kỳ (ví dụ `LAST_VALUE`) hoặc trung bình cộng."*

---

### Kịch bản 37: Thiết kế Junk Dimension
- **INT:** *"Bạn có 10 cột cờ chỉ thị (flags) trạng thái đơn hàng (như `is_returned`, `is_shipped`, `is_cancelled`...). Bạn có nên để chúng trực tiếp trong bảng Fact không?"*
- **CAND:** *"Không. Để quá nhiều cột cờ trong bảng Fact sẽ làm tăng kích thước chiều rộng của bảng Fact (ngốn dung lượng I/O).
  - Giải pháp: Gom tất cả 10 cột cờ này vào một bảng Dimension duy nhất gọi là **Junk Dimension** chứa tất cả các tổ hợp trạng thái khả thi. Bảng Fact lúc này chỉ cần lưu duy nhất 1 cột khóa ngoại trỏ tới Junk Dimension này."*

---

### Kịch bản 38: Thiết kế Accumulating Snapshot Fact Table
- **INT:** *"Thiết kế bảng Fact thế nào để theo dõi một quy trình có nhiều bước (ví dụ: Đặt hàng $\rightarrow$ Thanh toán $\rightarrow$ Đóng gói $\rightarrow$ Vận chuyển $\rightarrow$ Giao hàng)?"*
- **CAND:** *"Sử dụng **Accumulating Snapshot Fact Table**. Mỗi dòng trong bảng Fact đại diện cho một đơn hàng, chứa nhiều cột ngày tháng tương ứng với từng cột mốc của quy trình. Dòng dữ liệu này sẽ liên tục được cập nhật (updated) khi đơn hàng chuyển sang trạng thái mới."*

---

### Kịch bản 39: Tránh lỗi Chasm Trap trong Data Mart
- **INT:** *"Lỗi Chasm Trap xảy ra khi nào trong thiết kế Data Warehouse?"*
- **CAND:** *"Xảy ra khi ta thực hiện Join một bảng Dimension với 2 bảng Fact độc lập có mối quan hệ một-nhiều (ví dụ: Dim_Customer join với Fact_Orders và Fact_Support_Tickets). Phép join kép này tạo ra tích đề-các (Cartesian product) làm sai lệch hoàn toàn kết quả tính tổng (`SUM`) của cả 2 bảng Fact."*

---

### Kịch bản 40: Thiết kế Outrigger Dimension
- **INT:** *"Khi nào thì được phép Join giữa hai bảng Dimension với nhau thay vì Join qua bảng Fact?"*
- **CAND:** *"Khi thiết kế một **Outrigger Dimension**. Đây là trường hợp một bảng Dimension quá lớn và chứa một tập thông tin tham chiếu con cũng thay đổi chậm (ví dụ: `Dim_Store` chứa thông tin địa lý tham chiếu tới bảng `Dim_Geography`). Để tránh phình to bảng, ta cho phép `Dim_Store` chứa khóa ngoại trỏ tới `Dim_Geography`."*

---

## PHẦN 4: SYSTEM DESIGN & KIẾN TRÚC PIPELINE E2E

### Kịch bản 41: Thiết kế CDC Pipeline OLTP sang OLAP
- **INT:** *"Thiết kế một hệ thống đồng bộ dữ liệu thời gian thực từ Database nguồn MySQL (OLTP) sang BigQuery (OLAP). Yêu cầu không làm ảnh hưởng hiệu năng MySQL."*
- **CAND:** *"Em thiết kế hệ thống CDC sử dụng kiến trúc sau:
  1. **Debezium** đọc trực tiếp MySQL `Binlog` từ đĩa cứng (không query trực tiếp SQL).
  2. Sự kiện thay đổi được đẩy vào **Kafka**.
  3. **Apache Flink / Spark Streaming** đọc từ Kafka, parse schema và ghi tuần tự vào BigQuery sử dụng Storage Write API (Streaming inserts)."*

---

### Kịch bản 42: Lựa chọn Orchestrator - Airflow vs Prefect
- **INT:** *"Tại sao bạn lại chọn Airflow thay vì Prefect cho dự án data platform tại KiotViet?"*
- **CAND:** *"Airflow là tiêu chuẩn công nghiệp với hệ sinh thái kết nối (Providers) khổng lồ, hỗ trợ Scheduler HA từ bản 2.0. Nó cực kỳ mạnh cho kiến trúc static/batch. Tuy nhiên, nếu dự án đòi hỏi xử lý luồng dữ liệu động (dynamic/event-driven) hoặc chạy lightweight dạng serverless, Prefect sẽ tốt hơn do không bị ràng buộc bởi cơ chế parse DAG liên tục của Airflow."*

---

### Kịch bản 43: Thiết kế cơ chế Idempotent cho API Ingestion Pipeline
- **INT:** *"Bạn crawl dữ liệu từ API của đối tác mỗi giờ. Đôi khi API trả về trùng dữ liệu. Thiết kế thế nào để pipeline của bạn chạy lại 10 lần vẫn không bị trùng dữ liệu ở đích?"*
- **CAND:** *"Em sẽ thiết kế cơ chế **Upsert** dựa trên khóa chính tự nhiên của API (ví dụ: `transaction_id`).
  - Khi lưu trữ ở tầng Raw (S3): Ghi đè tệp tin theo giờ (`overwrite` mode trên partition `hour=HH`).
  - Khi chuyển đổi sang DW: Sử dụng câu lệnh `MERGE INTO` kiểm tra khóa chính. Nếu trùng thì cập nhật, chưa có thì insert."*

---

### Kịch bản 44: Xử lý Schema Evolution trong Kafka
- **INT:** *"Dưới database nguồn thêm 1 cột mới. Làm sao để Kafka consumer phía sau không bị lỗi khi đọc dữ liệu cũ?"*
- **CAND:** *"Sử dụng **Confluent Schema Registry** kết hợp định dạng **Avro / Protobuf**. Cấu hình Schema Compatibility ở chế độ `BACKWARD` hoặc `FULL`. Khi đó, consumer cũ vẫn đọc được dữ liệu mới (bỏ qua cột mới), và consumer mới đọc được dữ liệu cũ (điền giá trị default cho cột thiếu)."*

---

### Kịch bản 45: Thiết kế Backfill Strategy cho Pipeline 3 năm
- **INT:** *"Bạn cần tính toán lại 1 cột chỉ số cho toàn bộ dữ liệu 3 năm qua. Làm thế nào để chạy backfill nhanh nhất mà không làm sập Cluster?"*
- **CAND:** *"
  1. Tránh chạy một Spark job duy nhất cho cả 3 năm (gây OOM).
  2. Em viết Airflow DAG hỗ trợ tham số hóa ngày chạy (`execution_date`).
  3. Sử dụng tính năng **Dynamic Task Mapping** hoặc chia nhỏ thời gian chạy thành từng tháng một, lập lịch cho chạy song song tối đa N tasks (ví dụ: `max_active_runs=5`) để kiểm soát tải của cluster."*

---

### Kịch bản 46: Data Lakehouse - Delta Lake vs Parquet thuần
- **INT:** *"Tại sao Data Lakehouse (như Delta Lake) lại hỗ trợ ACID transaction trên môi trường Object Storage vốn không hỗ trợ ghi đè nguyên tử?"*
- **CAND:** *"Nhờ cơ chế **Transaction Log (Delta Log)**. Khi có thay đổi, Delta Lake ghi các file Parquet mới, đồng thời tạo một file commit log dạng JSON lưu thông tin phiên bản. Các câu lệnh đọc dữ liệu sẽ dựa vào file JSON này để biết file Parquet nào là hợp lệ tại phiên bản hiện tại, bỏ qua các file cũ hoặc chưa commit xong."*

---

### Kịch bản 47: Thiết kế cơ chế Cảnh báo (Alerting Rules) cho SLA
- **INT:** *"Làm thế nào để phát hiện một Daily ETL job bị treo không hoàn thành trước 8 giờ sáng để báo động cho On-call engineer?"*
- **CAND:** *
  - Sử dụng tính năng **SLA Miss Callback** của Airflow. Ta khai báo tham số `sla=timedelta(hours=6)` cho DAG.
  - Hoặc thiết lập một PromQL rule trong Prometheus kiểm tra trạng thái của job trong DB:
    `airflow_dag_run_duration_seconds{dag_id="daily_etl"} > 21600` (6 tiếng) $\rightarrow$ trigger PagerDuty/Slack alert."*

---

### Kịch bản 48: Deploy ML Model - Batch Scoring Pipeline
- **INT:** *"Làm thế nào để áp dụng một ML model (dự báo churn) cho 10 triệu khách hàng hàng tuần sử dụng Spark?"*
- **CAND:** *"Em sẽ đóng gói mô hình ML dưới dạng **Spark UDF** (hoặc tốt nhất là dùng thư viện MLLib hỗ trợ phân tán). Spark sẽ phân phối mô hình đến các Executor, mỗi executor load mô hình vào RAM một lần duy nhất (`foreachPartition`) và chạy dự báo song song trên các partition khách hàng."*

---

### Kịch bản 49: Lambda vs Kappa Architecture
- **INT:** *"Khi nào bạn chọn kiến trúc Kappa thay vì Lambda?"*
- **CAND:** *"Em chọn kiến trúc Kappa khi toàn bộ logic xử lý thời gian thực (Real-time) và chạy lại lịch sử (Batch) có thể quy về **một bộ code duy nhất** chạy trên công cụ Stream Processing (như Flink). Lambda phức tạp hơn vì phải duy trì 2 bộ code riêng cho tầng Batch (Spark) và Speed (Storm/Flink)."*

---

### Kịch bản 50: Cost Optimization on Cloud Data Platform
- **INT:** *"Chi phí AWS EMR của dự án tăng vọt gấp 3 lần tháng trước. Là Data Engineer, bạn kiểm tra những điểm nào để cắt giảm chi phí?"*
- **CAND:** *"Em sẽ kiểm tra 4 điểm:
  1. **Idle Time**: Cụm EMR có tự động tắt sau khi xong job không? (Cấu hình Auto-termination policy).
  2. **Instance Types**: Có dùng nhầm instance đắt tiền cho job nhẹ không? Thay thế Master/Core nodes bằng Spot Instances.
  3. **Data Spill**: Có job nào bị tràn bộ nhớ ra đĩa tạm nhiều gây kéo dài thời gian chạy? (Nâng cấu hình partitions để tối ưu thời gian).
  4. **S3 Storage**: Bật Lifecycle Policies trên S3 để tự động chuyển dữ liệu Raw/Staging cũ sang Glacier."*
