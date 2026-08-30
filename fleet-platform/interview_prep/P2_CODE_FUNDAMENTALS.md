# P2 — CODE CƠ BẢN: MAPREDUCE, DSA, SORTING, SQL (Trụ cột 2 — Ngày 3-4)
*Mỗi bài: Đề → Code đáp án Python → Phân tích Complexity → Liên hệ dự án*

---

## PHẦN A: MAPREDUCE — CODE TAY 3 PHA (Map → Shuffle → Reduce)

### Bài MR-1: Đếm doanh thu theo trạm dịch vụ (Liên hệ Fleet invoices)

**Đề**: Cho danh sách hóa đơn dạng `"head_id,customer_id,amount"`. Tính tổng doanh thu mỗi trạm.

```python
def map_phase(records):
    """Emit (head_id, amount) cho mỗi hóa đơn"""
    mapped = []
    for record in records:
        parts = record.split(",")
        head_id = parts[0]
        amount = float(parts[2])
        mapped.append((head_id, amount))
    return mapped

def shuffle_phase(mapped):
    """Gom tất cả amounts theo cùng head_id"""
    shuffled = {}
    for key, value in mapped:
        shuffled.setdefault(key, []).append(value)
    return shuffled

def reduce_phase(shuffled):
    """Tổng hợp doanh thu mỗi trạm"""
    return {key: sum(values) for key, values in shuffled.items()}

# Test
invoices = [
    "H01,C01,500000", "H01,C02,300000", "H02,C01,200000",
    "H01,C03,150000", "H02,C02,400000"
]
result = reduce_phase(shuffle_phase(map_phase(invoices)))
print(result)  # {'H01': 950000.0, 'H02': 600000.0}
```

**Complexity**: Map O(n), Shuffle O(n), Reduce O(n) → Tổng **O(n)**
**Phỏng vấn hỏi sâu**: "Nếu data không vừa RAM 1 máy?"
→ Map chạy **song song trên nhiều node** theo HDFS blocks (data locality). Shuffle **ghi đĩa + truyền mạng** giữa nodes (bottleneck chính của MapReduce truyền thống). Spark tối ưu hơn bằng cách giữ data trong **RAM giữa các stage** thay vì ghi đĩa mỗi shuffle.

---

### Bài MR-2: Map-Side Join (Broadcast Join Pattern)

**Đề**: Join bảng invoices (lớn) với bảng heads (nhỏ) để thêm tên trạm vào kết quả.

```python
def map_side_join(invoice_records, heads_lookup):
    """
    heads_lookup là dict nhỏ được broadcast đến MỌI mapper.
    Không cần Shuffle/Reduce → nhanh hơn Reduce-Side Join.
    """
    results = []
    for record in invoice_records:
        parts = record.split(",")
        head_id = parts[0]
        amount = float(parts[2])
        head_name = heads_lookup.get(head_id, "UNKNOWN")
        results.append((head_name, amount))
    return results

# Bảng nhỏ (< 10MB) → broadcast được
heads_lookup = {"H01": "Bình Tân", "H02": "Thủ Đức", "H03": "Long An"}

invoices = ["H01,C01,500000", "H02,C01,200000", "H01,C02,300000"]
result = map_side_join(invoices, heads_lookup)
print(result)  # [('Bình Tân', 500000.0), ('Thủ Đức', 200000.0), ('Bình Tân', 300000.0)]
```

**Khi nào dùng Map-Side Join vs Reduce-Side Join?**
| | Map-Side Join (Broadcast) | Reduce-Side Join |
|---|---|---|
| Điều kiện | 1 bảng **nhỏ** (< 10MB), vừa RAM mỗi mapper | Cả 2 bảng đều lớn |
| Shuffle | **Không cần** | Cần shuffle cả 2 bảng |
| Performance | Nhanh (chỉ đọc bảng lớn 1 lần) | Chậm (shuffle overhead) |
| Spark tương đương | `broadcast(df_small).join(df_big)` | `df_big.join(df_other_big)` |

---

## PHẦN B: DATA STRUCTURES & ALGORITHMS — 8 PATTERNS

### DSA-1: HashMap — Two Sum Problem

**Đề**: Cho mảng số nguyên và target. Tìm 2 số có tổng bằng target, trả về indices.

```python
def two_sum(nums, target):
    """
    HashMap lookup: O(1) per check
    Tổng: O(n) time, O(n) space
    """
    seen = {}  # value → index
    for i, num in enumerate(nums):
        complement = target - num
        if complement in seen:
            return [seen[complement], i]
        seen[num] = i
    return []

print(two_sum([2, 7, 11, 15], 9))   # [0, 1]
print(two_sum([3, 2, 4], 6))        # [1, 2]
```

**Brute Force**: O(n²) time, O(1) space — 2 vòng lặp lồng nhau
**Optimized**: O(n) time, O(n) space — HashMap trade space for time
**Nếu RAM cực kỳ giới hạn?** → Sort O(n log n) + Two Pointers O(n) = O(n log n) time, O(1) space

---

### DSA-2: Set — Deduplicate Customer IDs

```python
def dedupe_customers(customer_ids):
    """O(n) time, O(n) space — Set membership test O(1)"""
    seen = set()
    unique = []
    for cid in customer_ids:
        if cid not in seen:
            seen.add(cid)
            unique.append(cid)  # Giữ thứ tự xuất hiện
    return unique

ids = ["C01", "C02", "C01", "C03", "C02", "C04"]
print(dedupe_customers(ids))  # ['C01', 'C02', 'C03', 'C04']
```

**Liên hệ Fleet**: Kafka CDC có thể gửi duplicate events (at-least-once delivery). Dùng Set check message_id trước khi INSERT.

---

### DSA-3: Heap — Top-K Customers by Revenue

**Đề**: Tìm K khách hàng có doanh thu cao nhất từ stream dữ liệu lớn (không load hết vào RAM).

```python
import heapq

def top_k_customers(revenue_stream, k):
    """
    Min-Heap kích thước K:
    - Giữ K phần tử lớn nhất tại mọi thời điểm
    - Khi phần tử mới > min(heap) → thay thế
    Time: O(n log k)  ← nhanh hơn sort O(n log n) khi k << n
    Space: O(k)       ← chỉ giữ k phần tử trong RAM
    """
    min_heap = []
    for customer_id, revenue in revenue_stream:
        if len(min_heap) < k:
            heapq.heappush(min_heap, (revenue, customer_id))
        elif revenue > min_heap[0][0]:  # So sánh với phần tử nhỏ nhất
            heapq.heapreplace(min_heap, (revenue, customer_id))
    
    # Trả về sorted descending
    return sorted(min_heap, key=lambda x: -x[0])

stream = [("C01", 5000000), ("C02", 2000000), ("C03", 8000000),
          ("C04", 1000000), ("C05", 6000000), ("C06", 3000000)]
print(top_k_customers(stream, 3))
# [(8000000, 'C03'), (6000000, 'C05'), (5000000, 'C01')]
```

**Phỏng vấn hỏi**: "Tại sao Min-Heap mà không phải Max-Heap?"
→ Vì ta cần **loại bỏ phần tử nhỏ nhất** khi heap đầy. Min-Heap cho phép truy cập min trong O(1) để so sánh, replace trong O(log k).

---

### DSA-4: Linked List — LRU Cache

```python
from collections import OrderedDict

class LRUCache:
    """
    Liên hệ: Redis eviction policy (allkeys-lru)
    OrderedDict = HashMap + Doubly Linked List
    GET/PUT: O(1) amortized
    """
    def __init__(self, capacity):
        self.cache = OrderedDict()
        self.capacity = capacity
    
    def get(self, key):
        if key not in self.cache:
            return -1
        self.cache.move_to_end(key)  # Đánh dấu "vừa dùng"
        return self.cache[key]
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.capacity:
            self.cache.popitem(last=False)  # Xóa item ít dùng nhất (đầu list)

cache = LRUCache(3)
cache.put("head:1", "Bình Tân")
cache.put("head:2", "Thủ Đức")
cache.put("head:3", "Long An")
cache.get("head:1")           # Đưa head:1 lên cuối (vừa dùng)
cache.put("head:4", "Biên Hòa")  # Capacity vượt → xóa head:2 (ít dùng nhất)
```

---

### DSA-5: Binary Tree — DFS Traversals

```python
class TreeNode:
    def __init__(self, val, left=None, right=None):
        self.val = val
        self.left = left
        self.right = right

def inorder(node):
    """Left → Root → Right. Cho BST → kết quả sorted."""
    if not node: return []
    return inorder(node.left) + [node.val] + inorder(node.right)

def preorder(node):
    """Root → Left → Right. Dùng để serialize/copy tree."""
    if not node: return []
    return [node.val] + preorder(node.left) + preorder(node.right)

# Build tree:     5
#               /   \
#              3     8
#             / \
#            1   4
root = TreeNode(5, TreeNode(3, TreeNode(1), TreeNode(4)), TreeNode(8))
print(inorder(root))   # [1, 3, 4, 5, 8] ← sorted!
print(preorder(root))  # [5, 3, 1, 4, 8]
```

**Complexity**: Time O(n) — visit mỗi node 1 lần. Space O(h) — call stack depth = height of tree.

---

### DSA-6: Stack & Queue — BFS trên Graph

```python
from collections import deque

def bfs(graph, start):
    """
    Breadth-First Search: tìm shortest path trên unweighted graph
    Liên hệ: Neo4j BFS traversal tìm tất cả students cách 2 hops từ 1 course
    Time: O(V + E), Space: O(V)
    """
    visited = set()
    queue = deque([start])
    visited.add(start)
    order = []
    
    while queue:
        node = queue.popleft()  # FIFO
        order.append(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                visited.add(neighbor)
                queue.append(neighbor)
    return order

# Graph: Student enrollment relationships
graph = {
    "Student_A": ["Course_1", "Course_2"],
    "Student_B": ["Course_1"],
    "Course_1": ["Student_A", "Student_B", "Assignment_1"],
    "Course_2": ["Student_A", "Assignment_2"],
    "Assignment_1": ["Course_1"],
    "Assignment_2": ["Course_2"],
}
print(bfs(graph, "Student_A"))
# ['Student_A', 'Course_1', 'Course_2', 'Student_B', 'Assignment_1', 'Assignment_2']
```

**DFS bằng Stack (thay Queue):**
```python
def dfs(graph, start):
    """Depth-First Search: dùng Stack (LIFO) thay Queue (FIFO)"""
    visited = set()
    stack = [start]
    order = []
    while stack:
        node = stack.pop()  # LIFO ← khác BFS ở đây
        if node not in visited:
            visited.add(node)
            order.append(node)
            for neighbor in reversed(graph.get(node, [])):
                if neighbor not in visited:
                    stack.append(neighbor)
    return order
```

---

### DSA-7: Binary Search — 3 Biến thể

```python
def binary_search_exact(arr, target):
    """Tìm chính xác target. O(log n)"""
    lo, hi = 0, len(arr) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        if arr[mid] == target:
            return mid
        elif arr[mid] < target:
            lo = mid + 1
        else:
            hi = mid - 1
    return -1

def binary_search_lower_bound(arr, target):
    """Tìm vị trí ĐẦU TIÊN >= target (lower bound). Dùng cho range query."""
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo

def binary_search_upper_bound(arr, target):
    """Tìm vị trí ĐẦU TIÊN > target (upper bound)."""
    lo, hi = 0, len(arr)
    while lo < hi:
        mid = (lo + hi) // 2
        if arr[mid] <= target:
            lo = mid + 1
        else:
            hi = mid
    return lo

arr = [1, 3, 3, 3, 5, 7, 9]
print(binary_search_exact(arr, 3))        # 1 (hoặc 2 hoặc 3 — bất kỳ vị trí nào)
print(binary_search_lower_bound(arr, 3))  # 1 (vị trí đầu tiên >= 3)
print(binary_search_upper_bound(arr, 3))  # 4 (vị trí đầu tiên > 3)
# Số phần tử = 3: upper - lower = 4 - 1 = 3
```

**Liên hệ DE**: Parquet Footer lưu min/max mỗi Row Group → Spark dùng binary search-like logic để skip Row Groups không chứa giá trị cần tìm (predicate pushdown).

---

## PHẦN C: SORTING — 2 THUẬT TOÁN VIẾT TỪ ĐẦU

### QuickSort

```python
def quicksort(arr):
    """
    Average: O(n log n)   — pivot chia đều mảng
    Worst:   O(n²)        — pivot luôn là min/max (mảng đã sorted + chọn pivot đầu/cuối)
    Space:   O(log n)     — call stack depth
    NOT stable (không giữ thứ tự phần tử bằng nhau)
    """
    if len(arr) <= 1:
        return arr
    pivot = arr[len(arr) // 2]  # Chọn pivot giữa → tránh worst case trên sorted array
    left = [x for x in arr if x < pivot]
    middle = [x for x in arr if x == pivot]
    right = [x for x in arr if x > pivot]
    return quicksort(left) + middle + quicksort(right)

print(quicksort([5, 3, 8, 1, 4, 9, 2]))  # [1, 2, 3, 4, 5, 8, 9]
```

### MergeSort

```python
def mergesort(arr):
    """
    Average & Worst: O(n log n) — GUARANTEED (không có worst case)
    Space: O(n) — cần mảng phụ để merge
    STABLE (giữ thứ tự phần tử bằng nhau)
    → External Sort dùng MergeSort vì stable + predictable
    """
    if len(arr) <= 1:
        return arr
    mid = len(arr) // 2
    left = mergesort(arr[:mid])
    right = mergesort(arr[mid:])
    return merge(left, right)

def merge(left, right):
    result = []
    i = j = 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:  # <= giữ stable
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    result.extend(left[i:])
    result.extend(right[j:])
    return result

print(mergesort([5, 3, 8, 1, 4, 9, 2]))  # [1, 2, 3, 4, 5, 8, 9]
```

**Bảng so sánh nhanh:**

| Thuật toán | Average | Worst | Space | Stable | Khi nào dùng |
|---|---|---|---|---|---|
| QuickSort | O(n log n) | **O(n²)** | O(log n) | ❌ | In-memory sort, data random |
| MergeSort | O(n log n) | O(n log n) | **O(n)** | ✅ | External sort, cần stable |
| HeapSort | O(n log n) | O(n log n) | **O(1)** | ❌ | Cần O(1) space (rare) |
| TimSort | O(n) best | O(n log n) | O(n) | ✅ | **Python default** (`sorted()`) |

---

## PHẦN D: BIG-O ANALYSIS — 5 BÀI TẬP

### Bài 1: Xác định complexity

```python
def mystery_1(arr):
    n = len(arr)
    for i in range(n):           # O(n)
        for j in range(i, n):    # O(n - i) → trung bình O(n/2)
            print(arr[i], arr[j])
```
**Đáp án**: O(n²) — Tổng = n + (n-1) + (n-2) + ... + 1 = n(n+1)/2 ≈ n²/2 → O(n²)

### Bài 2:
```python
def mystery_2(n):
    i = 1
    while i < n:
        print(i)
        i *= 2     # i: 1, 2, 4, 8, 16, ... → log₂(n) lần lặp
```
**Đáp án**: O(log n)

### Bài 3:
```python
def mystery_3(arr):
    n = len(arr)
    for i in range(n):          # O(n)
        j = 1
        while j < n:            # O(log n)
            print(arr[i], j)
            j *= 2
```
**Đáp án**: O(n log n) — Vòng ngoài O(n) × vòng trong O(log n)

### Bài 4:
```python
def mystery_4(arr):
    seen = set()
    for x in arr:               # O(n)
        if x not in seen:       # O(1) amortized
            seen.add(x)
    return list(seen)
```
**Đáp án**: O(n) time, O(n) space

### Bài 5 (Tricky):
```python
def mystery_5(arr):
    arr.sort()                  # O(n log n)
    for i in range(len(arr)):   # O(n)
        # Binary search in sorted array
        target = 100 - arr[i]
        lo, hi = i+1, len(arr)-1
        while lo <= hi:         # O(log n)
            mid = (lo + hi) // 2
            if arr[mid] == target: break
            elif arr[mid] < target: lo = mid + 1
            else: hi = mid - 1
```
**Đáp án**: O(n log n) — Sort O(n log n) + Loop n × Binary Search O(log n) = O(n log n) + O(n log n) = O(n log n)

---

## PHẦN E: SQL LIVE-CODING — 5 ĐỀ MỚI (Không trùng tài liệu cũ)

### SQL-1: Top 5 khách hàng theo doanh thu mỗi quý + % tăng trưởng

```sql
WITH quarterly_revenue AS (
    SELECT
        c.company_name,
        dd.fiscal_year,
        dd.fiscal_quarter,
        SUM(f.total_amount) AS quarterly_revenue
    FROM fact_repair_service_revenue f
    JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = TRUE
    JOIN dim_date dd ON f.date_key = dd.date_key
    GROUP BY c.company_name, dd.fiscal_year, dd.fiscal_quarter
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY fiscal_year, fiscal_quarter
            ORDER BY quarterly_revenue DESC
        ) AS rank,
        LAG(quarterly_revenue) OVER (
            PARTITION BY company_name ORDER BY fiscal_year, fiscal_quarter
        ) AS prev_quarter_revenue
    FROM quarterly_revenue
)
SELECT
    fiscal_year, fiscal_quarter, company_name, quarterly_revenue,
    ROUND(
        (quarterly_revenue - prev_quarter_revenue) * 100.0
        / NULLIF(prev_quarter_revenue, 0), 1
    ) AS growth_pct
FROM ranked
WHERE rank <= 5
ORDER BY fiscal_year, fiscal_quarter, rank;
```

### SQL-2: Chuỗi tăng trưởng liên tục 3 tháng

```sql
WITH monthly AS (
    SELECT customer_id,
           DATE_TRUNC('month', invoice_date) AS month,
           SUM(total_amount) AS revenue
    FROM invoices WHERE status = 'paid'
    GROUP BY customer_id, DATE_TRUNC('month', invoice_date)
),
lagged AS (
    SELECT *,
        LAG(revenue, 1) OVER (PARTITION BY customer_id ORDER BY month) AS prev_1,
        LAG(revenue, 2) OVER (PARTITION BY customer_id ORDER BY month) AS prev_2
    FROM monthly
)
SELECT customer_id, month, revenue, prev_1, prev_2
FROM lagged
WHERE revenue > prev_1 AND prev_1 > prev_2
ORDER BY customer_id, month;
```

### SQL-3: MERGE / UPSERT cho Incremental Load

```sql
-- PostgreSQL: INSERT ... ON CONFLICT (UPSERT pattern)
INSERT INTO dim_head (head_id, head_name, city, capacity, updated_at)
VALUES (1, 'Bình Tân', 'HCM', 50, CURRENT_TIMESTAMP)
ON CONFLICT (head_id)
DO UPDATE SET
    head_name = EXCLUDED.head_name,
    city = EXCLUDED.city,
    capacity = EXCLUDED.capacity,
    updated_at = EXCLUDED.updated_at
WHERE dim_head.head_name <> EXCLUDED.head_name
   OR dim_head.city <> EXCLUDED.city
   OR dim_head.capacity <> EXCLUDED.capacity;
-- Chỉ UPDATE khi data thực sự thay đổi → tránh unnecessary writes
```

### SQL-4: Self-JOIN — Khách hàng sử dụng dịch vụ ở ≥2 trạm trong cùng 1 tuần

```sql
SELECT DISTINCT
    a.customer_id,
    a.head_id AS head_1,
    b.head_id AS head_2,
    a.invoice_date
FROM invoices a
JOIN invoices b
    ON a.customer_id = b.customer_id
    AND a.head_id < b.head_id   -- Tránh trùng cặp (H1,H2) = (H2,H1) và (H1,H1)
    AND ABS(a.invoice_date - b.invoice_date) <= 7
WHERE a.status = 'paid' AND b.status = 'paid';
```

### SQL-5: Recursive CTE — Tính tổng doanh thu tích lũy theo chuỗi ngày

```sql
WITH RECURSIVE date_series AS (
    -- Anchor: ngày đầu tiên
    SELECT DATE '2026-01-01' AS report_date
    UNION ALL
    -- Recursive: +1 ngày
    SELECT report_date + INTERVAL '1 day'
    FROM date_series
    WHERE report_date < DATE '2026-01-31'
),
daily_revenue AS (
    SELECT DATE(invoice_date) AS day, SUM(total_amount) AS revenue
    FROM invoices WHERE status = 'paid'
    GROUP BY DATE(invoice_date)
)
SELECT
    ds.report_date,
    COALESCE(dr.revenue, 0) AS daily_revenue,
    SUM(COALESCE(dr.revenue, 0)) OVER (ORDER BY ds.report_date) AS cumulative_revenue
FROM date_series ds
LEFT JOIN daily_revenue dr ON ds.report_date = dr.day
ORDER BY ds.report_date;
```

---

## 📋 CHECKLIST ÔN TẬP TRỤ CỘT 2

- [ ] Viết MapReduce 3 pha từ đầu KHÔNG nhìn code mẫu
- [ ] Giải Two-Sum bằng HashMap trong 3 phút
- [ ] Code Top-K bằng Min-Heap giải thích được tại sao min không phải max
- [ ] Code BFS + DFS giải thích khác biệt (Queue vs Stack)
- [ ] Code Binary Search 3 biến thể (exact, lower, upper)
- [ ] Code QuickSort + MergeSort từ đầu, nói được worst case mỗi cái
- [ ] Giải 5 bài Big-O analysis không nhìn đáp án
- [ ] Giải 1 bài SQL Window Functions mới trong 20 phút
