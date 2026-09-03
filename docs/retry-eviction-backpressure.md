# Retry, Eviction & Backpressure — Ba mặt của cùng một vấn đề: hệ thống dưới tải

> Ba chủ đề này thoạt nhìn thuộc ba tầng khác nhau (application logic, orchestration, streaming), nhưng thực ra đều xoay quanh **cùng một câu hỏi**: *khi hệ thống quá tải, nó phản ứng bằng cách tự hồi phục hay tự làm nặng thêm chính nó?* Đọc theo thứ tự dưới đây, bạn sẽ thấy chúng nối vào nhau thành một câu chuyện duy nhất.

---

## 1. Retry có thể làm incident nặng hơn như thế nào?

### Bản chất
Retry được thiết kế để xử lý lỗi *thoáng qua* (transient failure). Nhưng khi lỗi là do **quá tải** (không phải lỗi ngẫu nhiên), retry không giúp gì cả — nó đổ thêm dầu vào lửa. Đây gọi là **retry storm** (hoặc retry amplification).

Cơ chế:
1. Service B chậm lại vì quá tải (CPU, DB connection pool cạn, GC pause...).
2. Service A gọi B, timeout, retry ngay lập tức.
3. Request gốc **vẫn còn đang được B xử lý** (B chưa chết, chỉ chậm) → giờ B nhận thêm request retry chồng lên request cũ → tải tăng gấp đôi, gấp ba.
4. B càng chậm hơn → A càng timeout nhiều hơn → càng retry nhiều hơn.
5. Nếu A có nhiều instance, tất cả cùng retry cùng lúc → **thundering herd**.
6. Nếu B là DB, connection pool bị retry lấp đầy → những request hợp lệ khác cũng không có connection để chạy → outage cục bộ biến thành outage toàn hệ thống (cascading failure).

### Ví dụ thực tế
- Một service gọi Postgres, connection pool 20 kết nối. DB chậm do 1 query nặng. Timeout 2s, retry 3 lần không backoff → mỗi request gốc tạo ra 4 kết nối liên tiếp. Pool cạn kiệt trong vài giây, toàn bộ endpoint khác dùng chung pool cũng chết theo — dù bản thân chúng không liên quan gì đến query nặng kia.
- AWS, Google đều từng công bố case study nội bộ về **retry storm** là nguyên nhân biến một sự cố nhỏ (1 node chậm) thành outage diện rộng.

### Cách phòng tránh (đây là phần hay bị hỏi trong phỏng vấn)
| Kỹ thuật | Vai trò |
|---|---|
| **Exponential backoff** | Giãn thời gian giữa các lần retry (1s, 2s, 4s, 8s...) để hệ thống có thời gian hồi phục |
| **Jitter** (random offset) | Tránh việc hàng nghìn client cùng backoff giống hệt nhau rồi cùng retry lại đúng 1 thời điểm |
| **Retry budget / retry ratio** | Giới hạn tổng số retry trong một cửa sổ thời gian (vd: không quá 10% traffic là retry) |
| **Circuit breaker** | Khi tỉ lệ lỗi vượt ngưỡng, ngắt hẳn việc gọi tiếp trong một khoảng thời gian, fail fast thay vì retry |
| **Idempotency key** | Đảm bảo retry không gây side-effect kép (double charge, double insert) |
| **Timeout hợp lý + deadline propagation** | Truyền deadline xuyên suốt chuỗi gọi để không có service nào retry "vô nghĩa" khi request gốc đã hết hạn |

### Câu hỏi phỏng vấn thường gặp
- "Retry và circuit breaker khác nhau ở điểm nào, dùng cùng nhau thế nào?"
- "Tại sao exponential backoff cần thêm jitter?"
- "Retry ở tầng nào là nguy hiểm nhất: client, load balancer, hay message queue consumer? Vì sao?"
- "Làm sao phát hiện một hệ thống đang bị retry storm qua metrics?" (gợi ý: tỉ lệ request/giây tăng đột biến nhưng success rate giảm, connection pool saturation, error rate hình chữ J)

---

## 2. Nghịch lý Eviction trong Kubernetes

### Bản chất nghịch lý
Kubelet có cơ chế **node-pressure eviction**: khi node sắp cạn tài nguyên (memory, disk, PID), kubelet chủ động **giết pod** để giải phóng tài nguyên, tránh để OOM killer hệ điều hành giết bừa bãi (kể cả process hệ thống).

Nghe thì hợp lý — nhưng **hành động "cứu" này lại thường làm tình hình tệ hơn trong ngắn hạn**, vì:

1. **Eviction là phản ứng trễ (reactive), không phải phòng ngừa.** Khi kubelet quyết định evict, node *đã* trong tình trạng pressure — nghĩa là hệ thống vốn đã căng thẳng, và bản thân việc evict (đọc trạng thái pod, gọi container runtime để dừng, ghi log, cập nhật API server) lại **tốn thêm CPU/IO** ngay lúc tài nguyên đang khan hiếm nhất.
2. **Pod bị evict không biến mất — nó di cư.** Nếu pod thuộc Deployment/ReplicaSet, scheduler sẽ tạo pod mới ngay lập tức và đặt nó lên **một node khác**. Nếu cluster đang bị áp lực tải chung (không chỉ 1 node), pod này rất có thể lại làm node tiếp theo rơi vào pressure → **evict lan chuỗi** (cascading eviction) khắp cluster, giống hệt domino.
3. **Retry storm phiên bản hạ tầng.** Khi pod bị evict, các client đang gọi đến nó (qua Service/Endpoint) nhận connection reset → client retry → tải dồn sang các pod còn lại của cùng Deployment → các pod đó dễ bị đẩy vào pressure tiếp theo → bị evict tiếp. Đây chính là retry storm ở mục 1, nhưng xảy ra ở tầng orchestration.
4. **Hysteresis / flapping.** Kubelet evict để đưa resource usage xuống dưới ngưỡng `eviction-hard`, nhưng nếu ứng dụng có memory leak hoặc traffic pattern gây tăng nhanh, usage vọt trở lại ngưỡng eviction chỉ sau vài phút → pod mới bị evict lần nữa → vòng lặp evict-reschedule-evict, gọi là **eviction flapping**, làm giảm effective capacity của cluster nhiều hơn cả khi không evict gì.
5. **Ưu tiên (priority/QoS class) làm lệch hướng "cứu".** Kubelet evict theo thứ tự QoS: `BestEffort` → `Burstable` → `Guaranteed`. Nếu một team đặt sai resource requests/limits (đa số pod là Burstable "trá hình"), hệ thống evict nhầm pod quan trọng trong khi pod rác tài nguyên vẫn sống — "cứu" nhầm chỗ.

Tóm gọn nghịch lý: **cơ chế được thiết kế để bảo vệ node lại chính là nguồn phát tán tải sang các node khác**, biến một sự cố cục bộ (1 node pressure) thành sự cố lan rộng (nhiều node pressure) — hệt logic retry storm nhưng ở lớp hạ tầng.

### Ví dụ thực tế
- Một node bị memory pressure do 1 pod leak. Kubelet evict đúng pod đó — nhưng ReplicaSet lập tức respawn pod mới trên node khác đang gần đầy tải cho batch job ban đêm → node đó cũng rơi vào pressure → evict tiếp → trong vòng 10 phút, 5/20 node của cluster luân phiên bị evict, tổng capacity thực tế giảm mạnh dù tổng tài nguyên vật lý không đổi.

### Cách giảm thiểu
- Đặt **resource requests/limits chính xác** (đúng QoS class mong muốn), tránh Burstable "ẩn danh".
- Dùng **Pod Disruption Budget (PDB)** để giới hạn số pod bị gián đoạn cùng lúc.
- Cấu hình **eviction threshold có buffer** (soft eviction + grace period) thay vì chỉ hard threshold, để có thời gian phản ứng/scale trước khi kubelet phải ra tay.
- **Horizontal Pod Autoscaler + Cluster Autoscaler** phối hợp để tài nguyên co giãn trước khi chạm ngưỡng pressure, thay vì để eviction là tuyến phòng thủ đầu tiên.
- Giám sát `node_memory_pressure`, `container_oom_events_total`, eviction event trong `kubectl get events` như một cảnh báo sớm, không phải khi đã evict mới biết.

### Câu hỏi phỏng vấn thường gặp
- "Phân biệt node-pressure eviction và OOM kill của container runtime."
- "QoS class trong K8s ảnh hưởng đến thứ tự eviction như thế nào?"
- "Tại sao eviction có thể gây cascading failure toàn cluster?"
- "PDB giúp gì trong tình huống eviction hàng loạt?"

---

## 3. Backpressure — giải thích kỹ

### Bản chất
Backpressure là cơ chế để **bên tiêu thụ (consumer) báo ngược lại cho bên sản xuất (producer) rằng: "chậm lại, tôi đang quá tải"**, thay vì consumer cứ nhận và âm thầm sập, hoặc producer cứ bắn dữ liệu vào một hàng đợi phình to vô hạn cho tới khi hết bộ nhớ.

Nói cách khác: backpressure biến "quá tải" từ một **sự kiện bất ngờ (crash)** thành một **tín hiệu có thể xử lý được (signal)**.

Không có backpressure, hệ thống sẽ rơi vào một trong hai kịch bản xấu:
- **Unbounded buffering**: queue/buffer phình to không giới hạn → OOM.
- **Silent drop**: dữ liệu bị rớt không kiểm soát, mất dữ liệu mà không ai biết.

### Backpressure hoạt động ở nhiều tầng — đây là phần hay bị nhầm lẫn

| Tầng | Cơ chế backpressure |
|---|---|
| **TCP** | Sliding window: receiver quảng bá "window size" còn lại, sender không gửi vượt quá đó |
| **Kafka** | Không có "push" thật sự — consumer **pull** dữ liệu theo tốc độ của chính nó (`poll()`), tự nhiên tạo backpressure: nếu consumer chậm, nó chỉ đơn giản là pull chậm lại. Lag tăng nhưng broker không bị sập |
| **RabbitMQ / message queue có prefetch** | `prefetch_count` giới hạn số message chưa ack mà broker gửi cho consumer — consumer báo hiệu "đủ rồi" bằng cách không ack thêm |
| **Reactive Streams (RxJava, Project Reactor, Akka Streams)** | Có protocol chuẩn: subscriber gọi `request(n)` để nói với publisher "gửi cho tôi đúng n phần tử", publisher không được gửi vượt quá |
| **HTTP** | HTTP 429 (Too Many Requests) + header `Retry-After` là backpressure ở tầng ứng dụng — server chủ động từ chối và ra lệnh chờ |
| **Node.js Streams** | `.write()` trả về `false` khi buffer nội bộ đầy, writer phải chờ event `'drain'` mới ghi tiếp |
| **Load balancer / API Gateway** | Rate limiting, token bucket, connection limit — về bản chất là backpressure áp đặt từ bên ngoài khi bản thân service không tự làm được |

### Kafka cụ thể hoạt động thế nào (vì đây là trọng tâm hay hỏi)
Kafka **không đẩy dữ liệu vào consumer** như một số MQ khác. Consumer chủ động gọi `poll()` theo nhịp riêng của nó:
- Nếu consumer xử lý chậm (vd: đang ghi DB, gọi API ngoài), nó đơn giản là **gọi poll() thưa hơn**.
- Message vẫn nằm an toàn trên broker (disk-backed, không mất), **consumer lag** (khoảng cách giữa offset mới nhất và offset đã commit) tăng lên — đây chính là *chỉ số đo backpressure* của Kafka.
- Vì vậy Kafka "miễn nhiễm" tự nhiên với kiểu OOM do buffer phình to ở producer→broker; nhưng **downstream của consumer** (vd: DB nó ghi vào) vẫn có thể bị quá tải nếu consumer cố gắng bắt kịp lag bằng cách tăng batch size/song song hoá vô tội vạ — đây là lúc bài học "retry storm" và "eviction paradox" ở trên quay lại: cố gắng "bù tải" một cách nóng vội thường tạo ra tải mới lớn hơn tải cũ.

### Khi nào cần thiết kế backpressure rõ ràng
- Producer và consumer có tốc độ lệch nhau đáng kể và lệch **không cố định** (traffic spike theo giờ, theo sự kiện).
- Hệ thống streaming thời gian thực (xử lý sensor data, clickstream, log pipeline).
- Bất kỳ chỗ nào có hàng đợi ở giữa hai service với khả năng xử lý khác nhau.
- Khi chi phí của việc **mất dữ liệu** hoặc **sập hệ thống** cao hơn chi phí của việc **chậm lại có kiểm soát**.

### Chiến lược xử lý khi backpressure kích hoạt
1. **Slow down producer** (lý tưởng nhất, nhưng cần cơ chế 2 chiều — vd reactive streams, TCP).
2. **Buffer có giới hạn (bounded queue)** + chính sách khi đầy: block, drop-oldest, drop-newest.
3. **Load shedding**: chủ động từ chối một phần request ít quan trọng để giữ hệ thống sống cho phần quan trọng (vd ưu tiên request thanh toán, bỏ qua request logging).
4. **Scale out consumer** — nhưng đây không phải backpressure, đây là giải quyết nguyên nhân; cần kết hợp cả hai.

### Câu hỏi phỏng vấn thường gặp
- "Backpressure khác gì với rate limiting?" (rate limiting là giới hạn áp từ ngoài vào theo policy cố định; backpressure là tín hiệu động, phát sinh từ chính trạng thái quá tải thực tế của consumer)
- "Tại sao Kafka được coi là có backpressure tự nhiên còn nhiều MQ dạng push thì không?"
- "Nếu consumer lag tăng liên tục không dừng, bạn debug và xử lý thế nào?"
- "Thiết kế một hệ thống ingest dữ liệu từ IoT device, hàng triệu event/giây — backpressure ở đâu, xử lý ra sao?"

---

## Sợi dây nối cả ba chủ đề lại với nhau

```
Hệ thống quá tải cục bộ (1 node/1 service chậm)
        │
        ▼
KHÔNG có backpressure → dữ liệu/traffic vẫn dồn vào như cũ
        │
        ▼
Client timeout → RETRY (không backoff/jitter) → tải tăng gấp bội
        │
        ▼
Node/pod bị đẩy vào resource pressure → K8s EVICT pod để "cứu"
        │
        ▼
Pod evict bị reschedule sang node khác / client retry sang pod còn lại
        │
        ▼
Node/pod tiếp theo cũng bị pressure → evict tiếp → CASCADING FAILURE
```

Cả ba cơ chế phòng thủ đúng cách (backoff+jitter+circuit breaker cho retry; PDB+resource request chuẩn cho eviction; backpressure/rate limit cho luồng dữ liệu) đều nhằm một mục đích chung: **biến quá tải cục bộ thành một tín hiệu được kiểm soát, thay vì để nó lan tỏa thành phản ứng dây chuyền**.

Đây cũng chính là mạch tư duy hay được hỏi xoáy trong vòng system design phỏng vấn Data Engineer/Backend: *"Hệ thống của bạn phản ứng thế nào khi một thành phần chậm lại?"* — câu trả lời tốt luôn đi từ backpressure → retry policy → resource isolation, chứ không phải chỉ nói "em sẽ scale thêm".
