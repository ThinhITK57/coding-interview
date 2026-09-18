#!/usr/bin/env bash
# ==============================================================================
# Entrypoint chung cho EPM Lightweight Crawler Container
# Hỗ trợ 2 chế độ:
#   serve            (mặc định) chạy epm_crawler_flow.py -> serve() deployments
#   adhoc <args...>  chạy thẳng epm/prefect_flow.py một lần
# ==============================================================================
set -euo pipefail

API_URL="${PREFECT_API_URL:-http://prefect-server:4200/api}"

wait_for_api() {
    echo "[entrypoint] Chờ Prefect API: ${API_URL}"
    for attempt in $(seq 1 60); do
        if curl -fsS --noproxy '*' --max-time 5 "${API_URL}/health" >/dev/null 2>&1; then
            echo "[entrypoint] Prefect API sẵn sàng sau ${attempt} lần thử."
            return 0
        fi
        sleep 3
    done
    echo "[entrypoint] CẢNH BÁO: Không kết nối được ${API_URL} sau 180s. Tiếp tục chạy..." >&2
    return 0
}

MODE="${1:-serve}"

case "${MODE}" in
    serve)
        wait_for_api
        cd /app
        echo "[entrypoint] serve() các deployment EPM..."
        exec python epm_crawler_flow.py
        ;;
    adhoc)
        shift
        cd /app
        echo "[entrypoint] Thực thi lệnh ad-hoc..."
        exec python epm/prefect_flow.py "$@"
        ;;
    *)
        exec "$@"
        ;;
esac
