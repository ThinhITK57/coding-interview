"""
Fleet Platform — Atomic Slot Reservation (Concurrency Control)
==============================================================
Module xử lý đặt lịch hẹn giữ chỗ (Atomic Reservation) tại Trạm sửa chữa:
  - Giải quyết bài toán tranh chấp giữ chỗ khi nhiều chủ xe đặt lịch cùng 1 Head trong 1 khung giờ.
  - Sử dụng **Redis Lua Script** đảm bảo Atomic Check-and-Set:
      1. Kiểm tra số slot đã giữ chỗ trong khung giờ (`head:{id}:slots:{date}:{hour}`)
      2. So sánh với `capacity_slots` của Head
      3. Nếu chưa vượt quá capacity → Tăng counter & cấp Reservation Token với TTL 15 phút.
      4. Nếu vượt quá capacity → Từ chối đặt lịch (Trả về false, chống Overbooking 100%).
"""

import time
import uuid
import redis
from typing import Tuple, Dict, Optional


# Redis Lua script thực thi Atomic Slot Reservation
LUA_RESERVE_SLOT = """
local slot_key = KEYS[1]
local capacity = tonumber(ARGV[1])
local reservation_id = ARGV[2]
local ttl_seconds = tonumber(ARGV[3])

-- Lấy số slot hiện tại đã booked
local current_booked = tonumber(redis.call('GET', slot_key) or '0')

if current_booked < capacity then
    -- Đủ chỗ: Tăng số chỗ booked và đặt TTL
    redis.call('INCR', slot_key)
    redis.call('EXPIRE', slot_key, ttl_seconds * 4) -- Giữ slot counter tồn tại lâu hơn TTL giữ chỗ
    return 1
else
    -- Đã hết chỗ
    return 0
end
"""


class SlotReservationService:
    def __init__(self, redis_host: str = "master", redis_port: int = 6379, redis_db: int = 0):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)
        self.lua_reserve = self.r.register_script(LUA_RESERVE_SLOT)

    def reserve_slot(
        self,
        head_id: int,
        truck_plate: str,
        booking_date: str,  # Format: "YYYY-MM-DD"
        booking_hour: int,   # Format: 8, 9, 10, ... (gio trong ngay)
        hold_ttl_seconds: int = 900,  # 15 phút giữ chỗ
    ) -> Tuple[bool, str, Optional[Dict]]:
        """
        Thực hiện giữ chỗ Atomic cho xe tải tại Head.

        :return: (success: bool, message: str, reservation_data: dict)
        """
        # 1. Lấy capacity_slots của Head từ Redis Hash
        head_info = self.r.hgetall(f"head:info:{head_id}")
        if not head_info or head_info.get("is_active") == "0":
            return False, f"Head #{head_id} không tồn tại hoặc ngưng hoạt động.", None

        capacity = int(head_info.get("capacity_slots", 4))

        # Key đếm slot: head:1:slots:2024-07-29:09
        slot_key = f"head:{head_id}:slots:{booking_date}:{booking_hour:02d}"
        reservation_id = f"RES-{uuid.uuid4().hex[:8].upper()}"

        # 2. Gọi Lua script thực thi Atomic Check-and-Set
        res = self.lua_reserve(keys=[slot_key], args=[capacity, reservation_id, hold_ttl_seconds])

        if res == 1:
            # Giữ chỗ thành công → Lưu thông tin reservation với TTL
            res_key = f"reservation:{reservation_id}"
            res_data = {
                "reservation_id": reservation_id,
                "head_id": str(head_id),
                "truck_plate": truck_plate,
                "booking_date": booking_date,
                "booking_hour": str(booking_hour),
                "created_at": str(int(time.time())),
                "status": "HELD",
            }
            self.r.hset(res_key, mapping=res_data)
            self.r.expire(res_key, hold_ttl_seconds)

            return True, "Giữ chỗ thành công", res_data
        else:
            return False, f"Trạm #{head_id} đã hết chỗ trong khung giờ {booking_hour}:00 ngày {booking_date}.", None

    def release_reservation(self, reservation_id: str) -> bool:
        """Hủy giữ chỗ (ví dụ khi khách hủy hoặc hết thời gian chờ)."""
        res_key = f"reservation:{reservation_id}"
        res_data = self.r.hgetall(res_key)
        if not res_data:
            return False

        head_id = res_data["head_id"]
        date = res_data["booking_date"]
        hour = int(res_data["booking_hour"])
        slot_key = f"head:{head_id}:slots:{date}:{hour:02d}"

        # Giảm slot counter
        self.r.decr(slot_key)
        self.r.delete(res_key)
        return True


# Quick Test CLI
if __name__ == "__main__":
    service = SlotReservationService()
    success, msg, data = service.reserve_slot(
        head_id=1,
        truck_plate="51C-123.45",
        booking_date="2026-07-30",
        booking_hour=9,
    )
    print(f"Reservation Result: {success} | Message: {msg}")
    if data:
        print(f"Reservation Token: {data['reservation_id']}")
