# Data Engineer Interview Prep — Fleet Maintenance & Repair Data Platform

> Odoo trong tài liệu này được xem như **1 OLTP source system** (giống SAP/Salesforce) — không đi sâu vào cấu trúc module. Trọng tâm là dòng chảy dữ liệu và các quyết định kỹ thuật DE.

---

## 0. Mục tiêu kinh doanh (Business Goal — nói đầu tiên khi mở bài phỏng vấn)

Toàn bộ hệ thống được xây dựng để phục vụ **2 nguồn doanh thu chính**: **(1) bán dịch vụ sửa chữa/bảo dưỡng chăm sóc khách hàng sau bán xe**, và **(2) bán linh kiện thay thế**. Mọi quyết định kỹ thuật trong pipeline — từ việc thu thập telemetry, matching Head, đến cách mô hình hóa dữ liệu báo cáo — đều phục vụ mục tiêu này: giúp đúng khách hàng tìm đúng nơi sửa chữa nhanh nhất (tăng tỉ lệ chuyển đổi dịch vụ), và đo lường chính xác doanh thu/lợi nhuận từ 2 nguồn trên để ra 