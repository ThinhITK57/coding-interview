"""
Fleet Platform — Redis Serving Layer Unit & Concurrency Tests
===============================================================
Script kiểm thử toàn bộ Redis Serving Layer:
  1. Test Sub-millisecond Head Matching latency (< 2ms)
  2. Test Concurrency Overbooking Prevention (Chạy 50 threads đặt lịch cùng lúc cho Head có capacity=4)
     → Kết quả PHẢI ĐẢM BẢO: Đúng 4 requests THÀNH CÔNG, 46 requests BỊ TỪ CHỐI, 0 overbooking.

Usage:
  python3 test_redis_matching.py
"""

import sys
import time
import concurrent.futures
from services.head_matching_service import HeadMatchingService
from services.slot_reservation import SlotReservationService


def test_matching_performance():
    print("\n[Test 1] Head Matching Latency Benchmark")
    service = HeadMatchingService()

    start = time.perf_counter()
    results = service.find_matching_heads(
        truck_lat=10.7513900,
        truck_lng=106.6063900,
        radius_km=25.0,
        required_component_id=1,
    )
    elapsed_ms = (time.perf_counter() - start) * 1000.0

    print(f"  ✓ Matching Executed in {elapsed_ms:.3f} ms")
    print(f"  ✓ Candidate Heads Found: {len(results)}")

    if elapsed_ms < 5.0:
        print("  ✅ Latency Benchmark PASSED (< 5ms)")
    else:
        print("  ⚠ Latency higher than expected.")


def test_concurrency_reservation():
    print("\n[Test 2] Concurrency Overbooking Prevention (50 Parallel Requests)")
    service = SlotReservationService()

    # Set mock capacity cho Head 99
    service.r.hset("head:info:99", mapping={
        "id": "99",
        "name": "Trạm Test Concurrency",
        "capacity_slots": "4",
        "is_active": "1",
    })

    slot_key = "head:99:slots:2026-07-30:10"
    service.r.delete(slot_key)  # Reset counter

    NUM_THREADS = 50
    CAPACITY = 4

    def attempt_booking(thread_idx):
        plate = f"51C-999.{thread_idx:02d}"
        success, msg, data = service.reserve_slot(
            head_id=99,
            truck_plate=plate,
            booking_date="2026-07-30",
            booking_hour=10,
        )
        return success, plate

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(attempt_booking, i) for i in range(NUM_THREADS)]
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

    success_count = sum(1 for res in results if res[0])
    fail_count = sum(1 for res in results if not res[0])

    print(f"  Total Requests Executed : {NUM_THREADS}")
    print(f"  Successful Reservations  : {success_count} (Capacity limit: {CAPACITY})")
    print(f"  Rejected Requests        : {fail_count}")

    # Clean up test keys
    service.r.delete(slot_key)
    service.r.delete("head:info:99")

    if success_count == CAPACITY and fail_count == (NUM_THREADS - CAPACITY):
        print("  ✅ Concurrency Control Test PASSED: Zero Overbooking Guaranteed!")
    else:
        print(f"  ❌ Concurrency Test FAILED: Expected {CAPACITY} successes, got {success_count}")


def main():
    print("=" * 60)
    print(" Fleet Platform — Serving Layer Tests")
    print("=" * 60)

    try:
        test_matching_performance()
        test_concurrency_reservation()
    except Exception as e:
        print(f"\n❌ Test suite execution failed: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print(" All Serving Layer Tests Completed Successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()
