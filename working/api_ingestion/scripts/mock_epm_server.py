"""
Mock HTTP Server for Planview / Clarizen AdaptiveWork API.
Mô phỏng chính xác hành vi endpoint POST /Task/query của Planview:
- Đọc payload JSON body: {"typeName": "Task", "fields": [...], "paging": {"from": ..., "limit": ...}}
- Phân trang từ tập dữ liệu giả lập sinh bởi MockDataGenerator
- Hỗ trợ mô phỏng lỗi gián đoạn mạng (Transient fault) để test Retry & Circuit Breaker
"""

import sys
import os
import json
import logging
import argparse
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import List, Dict, Any

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.mock_data_generator import MockDataGenerator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("MockEPMServer")


class PlanviewAPIHandler(BaseHTTPRequestHandler):
    """Xử lý các request gửi tới Mock Planview Server."""

    dataset: List[Dict[str, Any]] = []
    fault_counter: int = 0
    simulate_faults: bool = False

    def do_POST(self):
        """Xử lý truy vấn POST /Task/query."""
        content_length = int(self.headers.get("Content-Length", 0))
        body_bytes = self.rfile.read(content_length)

        try:
            payload = json.loads(body_bytes.decode("utf-8"))
        except Exception as e:
            self.send_response(400)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": f"Invalid JSON payload: {e}"}).encode("utf-8"))
            return

        # Kiểm tra mô phỏng lỗi gián đoạn (Rate Limit 429 hoặc 503)
        if self.simulate_faults:
            PlanviewAPIHandler.fault_counter += 1
            if PlanviewAPIHandler.fault_counter % 7 == 0:
                logger.warning("⚠️ [Simulated Fault] Returning HTTP 429 Too Many Requests")
                self.send_response(429)
                self.send_header("Content-Type", "application/json")
                self.send_header("Retry-After", "1")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Rate limit exceeded. Please retry."}).encode("utf-8"))
                return

        paging = payload.get("paging", {})
        from_offset = int(paging.get("from", 0))
        limit = int(paging.get("limit", 50))

        total_records = len(self.dataset)
        end_offset = min(from_offset + limit, total_records)
        chunk = self.dataset[from_offset:end_offset]
        has_more = (end_offset < total_records)

        response_data = {
            "entities": chunk,
            "paging": {
                "from": from_offset,
                "limit": limit,
                "hasMore": has_more,
                "totalRecords": total_records,
            },
        }

        response_bytes = json.dumps(response_data, ensure_ascii=False).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response_bytes)))
        self.end_headers()
        self.wfile.write(response_bytes)

        logger.info(
            f"Handled POST {self.path} - offset={from_offset}, limit={limit}, returned={len(chunk)}/{total_records}"
        )

    def do_GET(self):
        """Health check endpoint."""
        if self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"status": "UP", "total_records": len(self.dataset)}).encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        """Ghi log qua logger thay vì in mặc định ra stderr."""
        logger.debug("%s - - [%s] %s" % (self.client_address[0], self.log_date_time_string(), format % args))


def run_server(
    port: int = 8088,
    total_records: int = 1500,
    simulate_faults: bool = False,
):
    """Khởi động Mock Server."""
    generator = MockDataGenerator(seed=42)
    logger.info(f"Generating {total_records} mock task records...")
    PlanviewAPIHandler.dataset = generator.generate_dataset(total_records=total_records)
    PlanviewAPIHandler.simulate_faults = simulate_faults

    server_address = ("127.0.0.1", port)
    httpd = HTTPServer(server_address, PlanviewAPIHandler)
    logger.info(f"🚀 Mock Planview EPM Server listening on http://127.0.0.1:{port}")
    logger.info("Endpoints available:")
    logger.info("  POST http://127.0.0.1:8088/Task/query")
    logger.info("  GET  http://127.0.0.1:8088/health")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down mock server...")
        httpd.server_close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Mock Planview EPM API Server")
    parser.add_argument("--port", type=int, default=8088, help="Server port (default: 8088)")
    parser.add_argument("--records", type=int, default=1500, help="Total mock tasks (default: 1500)")
    parser.add_argument("--faults", action="store_true", help="Simulate transient faults (429/503)")
    args = parser.parse_args()

    run_server(port=args.port, total_records=args.records, simulate_faults=args.faults)
