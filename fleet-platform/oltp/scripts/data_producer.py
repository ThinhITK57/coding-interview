"""
Fleet Platform — Data Producer (Mô phỏng hoạt động nghiệp vụ Odoo)
===================================================================
Script này INSERT/UPDATE dữ liệu vào Postgres liên tục, mô phỏng:
  1. Khách hàng mới đăng ký
  2. Tạo work order mới (đặt lịch sửa chữa)
  3. Cập nhật trạng thái work order (scheduled → in_progress → completed)
  4. Tạo hóa đơn khi work order completed
  5. Cập nhật tồn kho (trừ linh kiện khi sửa chữa)
  6. Thay đổi status khách hàng (mới → cũ → thường niên) — trigger SCD Type 2

Debezium sẽ bắt tất cả thay đổi này qua WAL → Kafka CDC topics.

Usage: python3 data_producer.py [--interval 5] [--rounds 50]
Chạy trên: master (hoặc bất kỳ node nào có psycopg2 và kết nối Postgres)
"""

import argparse
import random
import time
import sys
from datetime import datetime, timedelta

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("Cần cài psycopg2: pip install psycopg2-binary")
    sys.exit(1)


# =============================================================
# Config
# =============================================================
DB_CONFIG = {
    "host": "master",
    "port": 5432,
    "dbname": "fleet_oltp",
    "user": "fleet_app",
    "password": "fleet_app_2024",
}

TRUCK_PLATES = [
    "51C-999.01", "51C-999.02", "51D-888.03", "60C-777.04",
    "51H-666.05", "62C-555.06", "51F-444.07", "51C-333.08",
]

ISSUES = [
    "Phanh kêu — kiểm tra và thay má phanh",
    "Lốp mòn — thay lốp trước",
    "Bảo dưỡng định kỳ 50.000km",
    "Bảo dưỡng định kỳ 100.000km",
    "Đèn báo dầu nhớt — thay dầu + lọc",
    "Máy phát điện yếu — kiểm tra + thay nếu cần",
    "Rò rỉ nước làm mát — kiểm tra két nước",
    "Tiếng ồn hộp số — kiểm tra ly hợp",
    "Giảm xóc hỏng — thay giảm xóc 2 bên",
    "Ắc quy yếu — thay ắc quy mới",
]


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_random_ids(conn):
    """Lấy danh sách IDs hiện có từ các bảng."""
    cur = conn.cursor()
    cur.execute("SELECT id FROM heads WHERE is_active = TRUE")
    head_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM components")
    component_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    return head_ids, customer_ids, component_ids


# =============================================================
# Action 1: Tạo work order mới
# =============================================================
def create_work_order(conn, head_ids, customer_ids):
    cur = conn.cursor()
    customer_id = random.choice(customer_ids)
    head_id = random.choice(head_ids)
    plate = random.choice(TRUCK_PLATES)
    issue = random.choice(ISSUES)
    scheduled = datetime.now() + timedelta(days=random.randint(1, 7))

    cur.execute("""
        INSERT INTO work_orders (customer_id, head_id, truck_plate, status, issue_summary, scheduled_at)
        VALUES (%s, %s, %s, 'scheduled', %s, %s)
        RETURNING id
    """, (customer_id, head_id, plate, issue, scheduled))

    wo_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    print(f"  [CREATE] Work Order #{wo_id}: {plate} → Head #{head_id} | {issue[:40]}...")
    return wo_id


# =============================================================
# Action 2: Cập nhật trạng thái work order
# =============================================================
def progress_work_order(conn):
    cur = conn.cursor()

    # scheduled → in_progress
    cur.execute("""
        UPDATE work_orders SET status = 'in_progress', started_at = NOW()
        WHERE id = (
            SELECT id FROM work_orders WHERE status = 'scheduled'
            ORDER BY RANDOM() LIMIT 1
        )
        RETURNING id
    """)
    row = cur.fetchone()
    if row:
        print(f"  [UPDATE] Work Order #{row[0]}: scheduled → in_progress")

    # in_progress → completed (với chi phí)
    labor = random.choice([400000, 500000, 600000, 800000, 1000000, 1500000, 2000000])
    parts = random.choice([1500000, 2500000, 3500000, 5000000, 7000000])
    cur.execute("""
        UPDATE work_orders
        SET status = 'completed', completed_at = NOW(),
            labor_cost = %s, total_parts_cost = %s, total_amount = %s
        WHERE id = (
            SELECT id FROM work_orders WHERE status = 'in_progress'
            ORDER BY RANDOM() LIMIT 1
        )
        RETURNING id, customer_id, labor_cost, total_parts_cost
    """, (labor, parts, labor + parts))
    row = cur.fetchone()
    if row:
        wo_id, cust_id, lc, pc = row
        print(f"  [UPDATE] Work Order #{wo_id}: in_progress → completed (labor={lc:,.0f} parts={pc:,.0f})")
        # Tạo hóa đơn cho completed order
        _create_invoice(cur, wo_id, cust_id, lc, pc)

    conn.commit()
    cur.close()


def _create_invoice(cur, wo_id, customer_id, service_amt, parts_amt):
    tax = int((service_amt + parts_amt) * 0.1)
    total = service_amt + parts_amt + tax
    inv_num = f"HD-{datetime.now().strftime('%Y')}-{random.randint(100000, 999999):06d}"

    cur.execute("""
        INSERT INTO invoices (work_order_id, customer_id, invoice_number, invoice_date,
                              due_date, service_amount, parts_amount, tax_amount, total_amount, status)
        VALUES (%s, %s, %s, CURRENT_DATE, CURRENT_DATE + INTERVAL '30 days',
                %s, %s, %s, %s, 'issued')
        RETURNING id
    """, (wo_id, customer_id, inv_num, service_amt, parts_amt, tax, total))
    inv_id = cur.fetchone()[0]
    print(f"  [CREATE] Invoice #{inv_id}: {inv_num} total={total:,.0f}đ")


# =============================================================
# Action 3: Cập nhật tồn kho (xuất kho linh kiện)
# =============================================================
def update_inventory(conn, head_ids, component_ids):
    cur = conn.cursor()
    head_id = random.choice(head_ids)
    comp_id = random.choice(component_ids)
    qty_change = random.randint(1, 3)

    cur.execute("""
        UPDATE parts_inventory
        SET quantity = GREATEST(0, quantity - %s)
        WHERE head_id = %s AND component_id = %s
        RETURNING id, quantity
    """, (qty_change, head_id, comp_id))

    row = cur.fetchone()
    if row:
        print(f"  [UPDATE] Inventory: Head #{head_id}, Component #{comp_id} → qty={row[1]} (-{qty_change})")
    conn.commit()
    cur.close()


# =============================================================
# Action 4: Nhập kho (bổ sung linh kiện)
# =============================================================
def restock_inventory(conn, head_ids, component_ids):
    cur = conn.cursor()
    head_id = random.choice(head_ids)
    comp_id = random.choice(component_ids)
    qty_add = random.randint(5, 20)

    cur.execute("""
        UPDATE parts_inventory
        SET quantity = quantity + %s
        WHERE head_id = %s AND component_id = %s
        RETURNING id, quantity
    """, (qty_add, head_id, comp_id))

    row = cur.fetchone()
    if row:
        print(f"  [RESTOCK] Inventory: Head #{head_id}, Component #{comp_id} → qty={row[1]} (+{qty_add})")
    conn.commit()
    cur.close()


# =============================================================
# Action 5: Thay đổi status khách hàng (SCD Type 2 trigger)
# =============================================================
def upgrade_customer_status(conn):
    cur = conn.cursor()

    # mới → cũ
    cur.execute("""
        UPDATE customers SET status = 'cũ'
        WHERE id = (
            SELECT id FROM customers WHERE status = 'mới' ORDER BY RANDOM() LIMIT 1
        )
        RETURNING id, company_name
    """)
    row = cur.fetchone()
    if row:
        print(f"  [SCD] Customer #{row[0]} ({row[1]}): mới → cũ")

    # cũ → thường niên (ít hơn)
    if random.random() < 0.3:
        cur.execute("""
            UPDATE customers SET status = 'thường niên', loyalty_program = 'VIP-2024'
            WHERE id = (
                SELECT id FROM customers WHERE status = 'cũ' ORDER BY RANDOM() LIMIT 1
            )
            RETURNING id, company_name
        """)
        row = cur.fetchone()
        if row:
            print(f"  [SCD] Customer #{row[0]} ({row[1]}): cũ → thường niên (VIP-2024)")

    conn.commit()
    cur.close()


# =============================================================
# Action 6: Thanh toán hóa đơn
# =============================================================
def pay_invoice(conn):
    cur = conn.cursor()
    cur.execute("""
        UPDATE invoices SET status = 'paid', payment_date = NOW()
        WHERE id = (
            SELECT id FROM invoices WHERE status = 'issued' ORDER BY RANDOM() LIMIT 1
        )
        RETURNING id, invoice_number, total_amount
    """)
    row = cur.fetchone()
    if row:
        print(f"  [PAYMENT] Invoice #{row[0]} ({row[1]}): paid — {row[2]:,.0f}đ")
    conn.commit()
    cur.close()


# =============================================================
# Main loop
# =============================================================
def main():
    parser = argparse.ArgumentParser(description="Fleet Platform — OLTP Data Producer")
    parser.add_argument("--interval", type=int, default=5, help="Giây giữa mỗi round (default: 5)")
    parser.add_argument("--rounds", type=int, default=50, help="Số round chạy (default: 50)")
    args = parser.parse_args()

    print("=" * 60)
    print(" Fleet Platform — Data Producer")
    print(f" {args.rounds} rounds, {args.interval}s interval")
    print("=" * 60)

    conn = get_connection()
    head_ids, customer_ids, component_ids = get_random_ids(conn)
    conn.close()

    for i in range(1, args.rounds + 1):
        print(f"\n--- Round {i}/{args.rounds} [{datetime.now().strftime('%H:%M:%S')}] ---")

        conn = get_connection()
        try:
            # Mỗi round thực hiện 2-4 actions ngẫu nhiên
            actions = random.sample([
                lambda: create_work_order(conn, head_ids, customer_ids),
                lambda: progress_work_order(conn),
                lambda: update_inventory(conn, head_ids, component_ids),
                lambda: restock_inventory(conn, head_ids, component_ids),
                lambda: upgrade_customer_status(conn),
                lambda: pay_invoice(conn),
            ], k=random.randint(2, 4))

            for action in actions:
                try:
                    action()
                except Exception as e:
                    print(f"  [ERROR] {e}")
                    conn.rollback()

        finally:
            conn.close()

        if i < args.rounds:
            time.sleep(args.interval)

    print("\n" + "=" * 60)
    print(" Done. All rounds completed.")
    print("=" * 60)


if __name__ == "__main__":
    main()
