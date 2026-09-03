"""
Fleet Platform — Head Matching Service
======================================
Module dịch vụ tìm kiếm Trạm sửa chữa (Head) phù hợp cho xe tải bị sự cố:
  1. Sử dụng Redis `GEOSEARCH` tìm các Head nằm trong bán kính R (km) từ vị trí xe tải.
  2. Truy vấn Redis Hash `head:parts:{head_id}` kiểm tra tồn kho linh kiện cần thiết (`quantity > 0`).
  3. Đánh giá & xếp hạng các Head theo khoảng cách (km) và tình trạng đáp ứng linh kiện.
  4. Trả về kết quả trong thời gian dưới millisecond (< 1-2ms) mà KHÔNG cần query PostgreSQL.
"""

import json
import redis
from typing import List, Dict, Optional


class HeadMatchingService:
    def __init__(self, redis_host: str = "master", redis_port: int = 6379, redis_db: int = 0):
        self.r = redis.Redis(host=redis_host, port=redis_port, db=redis_db, decode_responses=True)

    def find_matching_heads(
        self,
        truck_lat: float,
        truck_lng: float,
        radius_km: float = 25.0,
        required_component_id: Optional[int] = None,
        max_results: int = 5,
    ) -> List[Dict]:
        """
        Tìm kiếm các Trạm sửa chữa (Head) phù hợp nhất.

        :param truck_lat: Vĩ độ vị trí xe tải
        :param truck_lng: Kinh độ vị trí xe tải
        :param radius_km: Bán kính tìm kiếm (km)
        :param required_component_id: ID linh kiện cần thay thế (nếu có)
        :param max_results: Số lượng kết quả tối đa
        :return: Danh sách các Head phù hợp xếp theo khoảng cách từ gần đến xa
        """
        # 1. Redis GEOSEARCH: Tìm candidate heads trong bán kính radius_km
        # GEOSEARCH geo:heads FROMLONLAT lng lat BYRADIUS radius_km km WITHDIST WITHCOORD ASC
        try:
            geo_results = self.r.geosearch(
                "geo:heads",
                longitude=truck_lng,
                latitude=truck_lat,
                radius=radius_km,
                unit="km",
                withdist=True,
                withcoord=True,
                sort="ASC",
            )
        except Exception:
            # Fallback cho phiên bản Redis cũ hơn (dùng GEORADIUS)
            geo_results = self.r.georadius(
                "geo:heads",
                longitude=truck_lng,
                latitude=truck_lat,
                radius=radius_km,
                unit="km",
                withdist=True,
                withcoord=True,
                sort="ASC",
            )

        matched_heads = []

        for item in geo_results:
            # geo_results return item format: [member_name, distance_km, (lng, lat)]
            member_name, dist_km, coords = item[0], float(item[1]), item[2]
            head_id = member_name.replace("head_", "")

            # 2. Lấy thông tin Head từ Redis Hash
            head_info = self.r.hgetall(f"head:info:{head_id}")
            if not head_info or head_info.get("is_active") == "0":
                continue

            # 3. Kiểm tra tồn kho linh kiện nếu required_component_id được chỉ định
            has_stock = True
            part_price = 0.0
            available_qty = 0

            if required_component_id:
                part_raw = self.r.hget(f"head:parts:{head_id}", f"comp_{required_component_id}")
                if part_raw:
                    part_info = json.loads(part_raw)
                    available_qty = part_info.get("quantity", 0)
                    part_price = part_info.get("unit_price", 0.0)
                    if available_qty <= 0:
                        has_stock = False
                else:
                    has_stock = False

            # Thêm vào kết quả nếu thỏa mãn stock
            if has_stock:
                matched_heads.append({
                    "head_id": int(head_id),
                    "name": head_info.get("name", f"Trạm #{head_id}"),
                    "address": head_info.get("address", ""),
                    "city": head_info.get("city", ""),
                    "distance_km": round(dist_km, 2),
                    "latitude": float(coords[1]),
                    "longitude": float(coords[0]),
                    "capacity_slots": int(head_info.get("capacity_slots", 4)),
                    "required_part": {
                        "component_id": required_component_id,
                        "available_quantity": available_qty,
                        "unit_price": part_price,
                    } if required_component_id else None,
                })

            if len(matched_heads) >= max_results:
                break

        return matched_heads


# Quick Test CLI
if __name__ == "__main__":
    service = HeadMatchingService()
    # Test vị trí ở Bình Tân (HCM)
    results = service.find_matching_heads(
        truck_lat=10.7513900,
        truck_lng=106.6063900,
        radius_km=15.0,
        required_component_id=1,  # Má phanh trước
    )

    print("=" * 60)
    print(" Head Matching Results (Truck at Binh Tan)")
    print("=" * 60)
    print(json.dumps(results, indent=2, ensure_ascii=False))
