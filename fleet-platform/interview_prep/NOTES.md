Về Idempotency: Toàn bộ pipeline của em tuân thủ chặt chẽ tính bất biến. Ở tầng DWH Batch, em dùng Surrogate Key MD5 đơn định kết hợp chế độ partitionOverwriteMode = dynamic trong Spark để Airflow có retry hay backfill lại cũng không bao giờ bị nhân đôi số liệu. Ở tầng Streaming, em dùng Redis SET NX với TTL 24h để tự động loại bỏ các gói tin GPS trùng lặp do mạng 4G gửi lại.

Về quy mô Event trên Kafka: Hệ thống của em xử lý trung bình khoảng 
7.2
7.2 triệu events/ngày (thông lượng bình quân tầm 
80
−
100
 events/s
80−100 events/s, vào khung giờ cao điểm sáng và chiều khi xe xuất bến nhiều thì đạt khoảng 
300
−
500
 events/s
300−500 events/s). Trong đó:

Dữ liệu ERP CDC qua Debezium chiếm khoảng 
100.000
 events/ng
a
ˋ
y
100.000 events/ng 
a
ˋ
 y ghi nhận các giao dịch sửa chữa, xuất linh kiện từ 50 trạm.
Phần lớn thông lượng là khoảng 
7
7 triệu events Telemetry từ 10.000 xe tải. Do đặc thù thực tế chỉ có khoảng 
35
%
35% xe di chuyển đồng thời (ping 30s/lần) và phần còn lại ở chế độ nghỉ/dừng đỗ (ping 5 phút/lần), sinh ra khoảng 
1.8
GB
1.8GB dữ liệu thô/ngày.
Em cấu hình topic thành 6 partitions chia đều trên 3 brokers, đảm bảo thông lượng xử lý realtime với độ trễ cực thấp dưới 
5
ms
5ms."*