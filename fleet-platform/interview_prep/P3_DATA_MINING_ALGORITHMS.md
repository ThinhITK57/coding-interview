# P3 — GIẢI THUẬT KHAI PHÁ DỮ LIỆU (Trụ cột 3 — Ngày 5)
*6 giải thuật: Code Python đầy đủ + Phân tích Complexity + Trade-off + Liên hệ dự án*

---

## Bài 1: ANOMALY DETECTION — Sliding Window + Z-Score
*Liên hệ: Phát hiện nhiệt độ động cơ bất thường từ Kafka telemetry stream*

### Bài toán
Cho stream số liệu cảm biến nhiệt độ xe tải (°C). Phát hiện giá trị bất thường (vượt 3 độ lệch chuẩn so với trung bình cửa sổ gần nhất).

```python
from collections import deque
import statistics

def detect_anomaly(stream, window_size=20, z_threshold=3.0):
    """
    Sliding Window Z-Score Anomaly Detection
    - window: giữ N giá trị gần nhất
    - z_score = (value - mean) / stdev
    - |z_score| > threshold → anomaly
    
    Time:  O(n × w) nếu tính mean/stdev mỗi lần, hoặc O(n) nếu dùng running stats
    Space: O(w) — chỉ giữ window_size phần tử
    """
    window = deque(maxlen=window_size)
    anomalies = []
    
    for i, value in enumerate(stream):
        if len(window) >= window_size // 2:  # Cần đủ data để tính statistics
            mean = statistics.mean(window)
            stdev = statistics.stdev(window) if len(window) > 1 else 1
            z_score = (value - mean) / stdev if stdev > 0 else 0
            
            if abs(z_score) > z_threshold:
                anomalies.append({
                    'index': i,
                    'value': value,
                    'z_score': round(z_score, 2),
                    'mean': round(mean, 2),
                    'stdev': round(stdev, 2)
                })
        
        window.append(value)
    return anomalies

# Test: cảm biến engine_temp — giá trị 200°C là bất thường
engine_temps = [85, 87, 86, 88, 85, 87, 86, 84, 88, 86,
                85, 87, 200, 86, 85, 87, 86, 88, 85, 87,  # 200 = anomaly!
                86, 84, 85, 87, -10, 86, 85]                # -10 = anomaly!

results = detect_anomaly(engine_temps, window_size=10, z_threshold=3)
for r in results:
    print(f"⚠️ Anomaly at index {r['index']}: value={r['value']}°C, "
          f"z_score={r['z_score']}, mean={r['mean']}°C")
```

### Trade-off: Window Size
| Window Size | Ưu điểm | Nhược điểm |
|---|---|---|
| Nhỏ (5-10) | Phản ứng nhanh với thay đổi | Dễ false positive (ít data → stdev không ổn định) |
| Lớn (50-100) | Ổn định, ít false positive | Phản ứng chậm với concept drift (hành vi thay đổi từ từ) |

**Phỏng vấn hỏi**: "Tại sao sliding window thay vì tính trên toàn bộ lịch sử?"
→ Vì dữ liệu cảm biến thay đổi theo mùa/thời tiết. Mùa hè nhiệt độ trung bình cao hơn mùa đông. Nếu tính mean/stdev trên toàn bộ lịch sử, ngưỡng sẽ không thích nghi (concept drift).

---

## Bài 2: TOP-K STREAMING — Min-Heap
*Liên hệ: Top 10 khách hàng doanh thu cao nhất trên tập dữ liệu không vừa RAM*

```python
import heapq

def top_k_streaming(data_stream, k, key_fn):
    """
    Streaming Top-K sử dụng Min-Heap kích thước K.
    
    Ý tưởng: Giữ K phần tử lớn nhất. Khi phần tử mới > min(heap) → thay thế.
    
    Time:  O(n log k) — n phần tử, mỗi phần tử heappush/heapreplace O(log k)
    Space: O(k) — chỉ giữ k phần tử trong RAM
    
    So sánh:
    - Sort toàn bộ: O(n log n) time, O(n) space ← KHÔNG KHẢ THI nếu n quá lớn
    - Min-Heap:     O(n log k) time, O(k) space ← khả thi với mọi n
    """
    min_heap = []
    
    for item in data_stream:
        score = key_fn(item)
        entry = (score, item)
        
        if len(min_heap) < k:
            heapq.heappush(min_heap, entry)
        elif score > min_heap[0][0]:
            heapq.heapreplace(min_heap, entry)
    
    return sorted(min_heap, key=lambda x: -x[0])

# Giả lập stream doanh thu khách hàng (không thể load hết vào RAM)
def revenue_stream():
    """Generator — đọc từng record, không load hết"""
    data = [
        {"cust": "C01", "revenue": 5_000_000},
        {"cust": "C02", "revenue": 12_000_000},
        {"cust": "C03", "revenue": 800_000},
        {"cust": "C04", "revenue": 25_000_000},
        {"cust": "C05", "revenue": 3_000_000},
        {"cust": "C06", "revenue": 18_000_000},
        {"cust": "C07", "revenue": 7_500_000},
    ]
    for d in data:
        yield d

results = top_k_streaming(revenue_stream(), k=3, key_fn=lambda x: x["revenue"])
for score, item in results:
    print(f"  {item['cust']}: {score:,}đ")
# C04: 25,000,000đ, C06: 18,000,000đ, C02: 12,000,000đ
```

---

## Bài 3: NEAR-DUPLICATE DETECTION — Shingling + Jaccard Similarity
*Liên hệ: MinHash LSH trong dự án RAG (dedup trước khi embedding)*

```python
def shingle(text, n=3):
    """Chia text thành tập n-gram (n ký tự liên tiếp)"""
    text = text.lower().replace(" ", "")
    return set(text[i:i+n] for i in range(len(text) - n + 1))

def jaccard_similarity(set_a, set_b):
    """
    Jaccard = |A ∩ B| / |A ∪ B|
    = 0 → hoàn toàn khác nhau
    = 1 → hoàn toàn giống nhau
    """
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0

# Test: 2 tài liệu gần giống nhau
doc1 = "Hệ thống sửa chữa xe tải bảo dưỡng định kỳ"
doc2 = "Hệ thống sửa chữa xe tải bảo dưỡng hàng tháng"  # Chỉ khác 2 từ cuối
doc3 = "Báo cáo doanh thu quý 3 năm tài khóa 2026"       # Hoàn toàn khác

s1, s2, s3 = shingle(doc1), shingle(doc2), shingle(doc3)
print(f"doc1 vs doc2: {jaccard_similarity(s1, s2):.2f}")  # ~0.70 (near-duplicate!)
print(f"doc1 vs doc3: {jaccard_similarity(s1, s3):.2f}")  # ~0.05 (khác nhau)
```

### Tại sao cần LSH khi có hàng triệu tài liệu?

| Approach | Complexity | 1 triệu docs |
|---|---|---|
| Pairwise Jaccard | **O(n²)** | 500 tỷ phép so sánh — **KHÔNG KHẢ THI** |
| MinHash + LSH | **O(n)** | Hash mỗi doc → nhóm vào bands → chỉ so sánh trong cùng bucket |

**Ý tưởng MinHash LSH** (không cần code đầy đủ, chỉ cần giải thích):
1. Mỗi document → tính k MinHash values (k random hash functions)
2. Chia k values thành b bands × r rows
3. Nếu 2 docs có cùng hash ở BẤT KỲ band nào → candidate pair → so sánh chi tiết
4. Probability of match = 1 - (1 - s^r)^b (s = true Jaccard similarity)

---

## Bài 4: BLOOM FILTER — Probabilistic Membership Test
*Liên hệ: Kafka deduplication — kiểm tra message_id đã xử lý chưa trước khi INSERT*

```python
import hashlib

class BloomFilter:
    """
    Probabilistic data structure:
    - Trả lời "CHẮC CHẮN KHÔNG CÓ" hoặc "CÓ THỂ CÓ"
    - KHÔNG BAO GIỜ False Negative (nếu nói "không có" → chắc chắn không có)
    - CÓ THỂ False Positive (nếu nói "có thể có" → cần verify lại)
    
    Space: O(m) bits — nhỏ hơn HashSet rất nhiều
    Time:  O(k) per operation — k = số hash functions
    """
    def __init__(self, size=1000, num_hashes=3):
        self.size = size
        self.num_hashes = num_hashes
        self.bit_array = [0] * size
    
    def _hashes(self, item):
        """Tạo k hash values cho 1 item"""
        positions = []
        for i in range(self.num_hashes):
            h = hashlib.md5(f"{item}_{i}".encode()).hexdigest()
            positions.append(int(h, 16) % self.size)
        return positions
    
    def add(self, item):
        for pos in self._hashes(item):
            self.bit_array[pos] = 1
    
    def might_contain(self, item):
        """True = CÓ THỂ CÓ (hoặc False Positive). False = CHẮC CHẮN KHÔNG CÓ."""
        return all(self.bit_array[pos] == 1 for pos in self._hashes(item))

# Test: kiểm tra CDC message_id đã xử lý chưa
bf = BloomFilter(size=10000, num_hashes=5)

# Đã xử lý 3 messages
bf.add("msg-001")
bf.add("msg-002")
bf.add("msg-003")

print(bf.might_contain("msg-001"))  # True  → đã xử lý (hoặc false positive)
print(bf.might_contain("msg-004"))  # False → CHẮC CHẮN chưa xử lý → an toàn INSERT
```

### False Positive Rate
$$P(\text{false positive}) \approx \left(1 - e^{-kn/m}\right)^k$$
- $m$ = kích thước bit array, $n$ = số phần tử đã thêm, $k$ = số hash functions
- Ví dụ: m=10000, n=1000, k=7 → P ≈ **0.8%** (rất thấp!)

**Khi nào dùng Bloom Filter thay vì HashSet?**
| | HashSet | Bloom Filter |
|---|---|---|
| Space | O(n) — lưu toàn bộ items | O(m) bits — **nhỏ hơn 10-100x** |
| False Positive | 0% | ~1% (adjustable) |
| False Negative | 0% | **0%** |
| Use case | RAM đủ | **Tỷ message IDs, RAM giới hạn** |

---

## Bài 5: HYPERLOGLOG — Ước Lượng Cardinality
*Liên hệ: Đếm unique customers trên tập dữ liệu quá lớn cho COUNT(DISTINCT)*

```python
import hashlib
import math

def count_leading_zeros(binary_str):
    """Đếm số bit 0 đầu tiên"""
    count = 0
    for bit in binary_str:
        if bit == '0':
            count += 1
        else:
            break
    return count

def hyperloglog_estimate(data_stream, num_buckets=64):
    """
    Ý tưởng HyperLogLog (đơn giản hóa):
    1. Hash mỗi element → binary string
    2. Dùng vài bit đầu chọn bucket
    3. Đếm leading zeros phần còn lại → ước lượng cardinality
    4. "Nếu thấy leading zeros = k → đã thấy khoảng 2^k unique values"
    
    Space: O(m) — m buckets × vài bytes = **chỉ ~12KB cho ước lượng tỷ unique values!**
    Error: ~1.04 / sqrt(m) → 64 buckets ≈ 13% error, 16384 buckets ≈ 0.8% error
    """
    buckets = [0] * num_buckets
    
    for item in data_stream:
        h = int(hashlib.md5(str(item).encode()).hexdigest(), 16)
        binary = bin(h)[2:].zfill(128)
        
        bucket_idx = h % num_buckets
        remaining = binary[int(math.log2(num_buckets)):]
        zeros = count_leading_zeros(remaining)
        
        buckets[bucket_idx] = max(buckets[bucket_idx], zeros + 1)
    
    # Harmonic mean estimation
    alpha = 0.7213 / (1 + 1.079 / num_buckets)
    raw_estimate = alpha * num_buckets ** 2 / sum(2 ** (-b) for b in buckets)
    
    return int(raw_estimate)

# Test: 100,000 elements nhưng chỉ 50,000 unique
import random
data = [random.randint(1, 50000) for _ in range(100000)]

actual = len(set(data))
estimated = hyperloglog_estimate(data, num_buckets=256)
error = abs(estimated - actual) / actual * 100

print(f"Actual unique: {actual}")
print(f"HLL estimate:  {estimated}")
print(f"Error:         {error:.1f}%")
```

**Redis HyperLogLog (production-ready):**
```
PFADD unique_customers "C001" "C002" "C003" "C001"   # Thêm (tự dedupe)
PFCOUNT unique_customers                              # ≈ 3 (ước lượng)
# Chỉ tốn 12KB RAM dù đếm HÀNG TỶ unique values!
```

**Khi nào dùng HLL thay vì COUNT(DISTINCT)?**
- Dataset > 100 triệu unique values → `COUNT(DISTINCT)` tốn hàng GB RAM
- Chấp nhận sai số ~0.8% → dùng HLL chỉ tốn 12KB

---

## Bài 6: MOVING AVERAGE — SMA vs EMA
*Liên hệ: Smoothing sensor data cho monitoring alert threshold*

```python
from collections import deque

def simple_moving_average(data, window_size):
    """
    SMA: Trung bình cộng đơn giản của N giá trị gần nhất
    Mỗi giá trị có trọng số BẰNG NHAU
    """
    window = deque(maxlen=window_size)
    results = []
    for value in data:
        window.append(value)
        results.append(sum(window) / len(window))
    return results

def exponential_moving_average(data, alpha=0.3):
    """
    EMA: Trọng số GIẢM DẦN theo thời gian (giá trị mới được coi trọng hơn)
    EMA_t = α × value_t + (1 - α) × EMA_{t-1}
    α lớn (0.8) → phản ứng nhanh, nhạy với noise
    α nhỏ (0.1) → phản ứng chậm, smooth hơn
    """
    results = [data[0]]  # EMA_0 = giá trị đầu tiên
    for value in data[1:]:
        ema = alpha * value + (1 - alpha) * results[-1]
        results.append(ema)
    return results

# Test: sensor nhiệt độ có noise
temps = [85, 87, 86, 88, 200, 86, 85, 87, 86, 84]
sma = simple_moving_average(temps, window_size=3)
ema = exponential_moving_average(temps, alpha=0.3)

for i, (t, s, e) in enumerate(zip(temps, sma, ema)):
    print(f"  t={i}: raw={t:3d}°C  SMA={s:.0f}°C  EMA={e:.0f}°C")
```

**Trade-off SMA vs EMA:**
| | SMA | EMA |
|---|---|---|
| Trọng số | Bằng nhau | Giảm dần (exponential decay) |
| Phản ứng spike | Chậm (phải đợi spike ra khỏi window) | Nhanh hơn (α điều chỉnh) |
| Use case | Báo cáo trend dài hạn | Real-time alerting, monitoring |

---

## 📋 CHECKLIST ÔN TẬP TRỤ CỘT 3

- [ ] Code Anomaly Detection từ đầu, giải thích concept drift
- [ ] Code Top-K Min-Heap, giải thích O(n log k) vs O(n log n)
- [ ] Giải thích Jaccard + tại sao cần LSH cho n lớn (O(n²) → O(n))
- [ ] Code Bloom Filter, nói được False Positive vs False Negative
- [ ] Giải thích HyperLogLog idea (leading zeros), biết Redis PFADD/PFCOUNT
- [ ] Phân biệt SMA vs EMA, biết khi nào dùng cái nào
