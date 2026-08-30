# Cài đặt Redis cho Fleet Platform

> 📌 **VỊ TRÍ TRIỂN KHAI CLUSTER:**
> - **Redis Server**: Chỉ cài đặt và chạy duy nhất 1 instance trên **`master`** (port 6379, listening `0.0.0.0`).
> - **`slave1` & `slave2`**: **KHÔNG CẦN** chạy Redis Server. Spark workers, Python workers trên slave1/slave2 sẽ kết nối từ xa tới Redis trên master (`master:6379`).
> - Nếu muốn chạy lệnh `redis-cli` từ slave1/slave2 để kiểm tra kết nối, chỉ cần cài gói **`redis-tools`** (chỉ client, không phải server).
>
> **3 vai trò của Redis trong dự án:**
> 1. **Search/Serving Store** — GEO + Hash lưu data Head cho matching engine (độ trễ < 1ms)
> 2. **Concurrency Control** — Atomic reservation bằng Lua Script chống overbooking đặt lịch
> 3. **Cache/Notify Layer** — Lưu aggregates + Pub/Sub thông báo cho WebSocket backend
>
> ---
>
> ## 1. Cài đặt Redis Server (CHỈ trên master)
>
> ```bash
> ssh aiguystory@master
>
> # Thêm Redis official repository (để có version 7+)
> curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg
> echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list
>
> sudo apt update
> sudo apt install -y redis
>
> # Verify version (phải >= 7.0)
> redis-server --version
> ```

---

## 2. Cấu hình redis.conf

```bash
sudo nano /etc/redis/redis.conf
```

Chỉnh các dòng:

```ini
# Lắng nghe từ tất cả interfaces (Spark, backend cần kết nối)
bind 0.0.0.0

# Tắt protected-mode để cho phép remote connections
protected-mode no

# Port mặc định
port 6379

# Maxmemory — giới hạn RAM Redis sử dụng (tùy máy bạn)
# Head data + aggregates không nhiều, 512MB đủ cho lab
maxmemory 512mb

# Eviction policy — khi hết memory, xóa key ít dùng nhất
# Quan trọng: aggregates cần giữ, head data cần giữ
# allkeys-lru là an toàn nhất cho lab
maxmemory-policy allkeys-lru

# Bật keyspace notifications (cho WebSocket backend subscribe)
notify-keyspace-events KEA

# RDB snapshot (backup) — giữ default cho lab
save 900 1
save 300 10
save 60 10000
```

---

## 3. Restart và verify
 
 ```bash
 sudo systemctl restart redis-server
 sudo systemctl enable redis-server
 
 # Test địa phương trên master
 redis-cli ping
 # → PONG
 ```
 
 ### 3.1 Cài postgresql-client / redis-tools trên slave1 & slave2 (để test)
 
 ```bash
 # Trên slave1
 ssh aiguystory@slave1
 sudo apt update && sudo apt install -y redis-tools
 
 # Trên slave2
 ssh aiguystory@slave2
 sudo apt update && sudo apt install -y redis-tools
 ```
 
 ### 3.2 Test kết nối mạng từ slave1/slave2 tới master
 
 ```bash
 # Từ slave1
 ssh aiguystory@slave1
 redis-cli -h master ping
 # → PONG
 
 # Test GEO command (sẽ dùng cho Head matching)
 redis-cli -h master GEOADD geo:heads 106.6297 10.8231 "head_001"
 redis-cli -h master GEOSEARCH geo:heads FROMLONLAT 106.6297 10.8231 BYRADIUS 10 km ASC
 # → head_001
 
```bash
 # Cleanup test data
 redis-cli -h master DEL geo:heads
 ```
