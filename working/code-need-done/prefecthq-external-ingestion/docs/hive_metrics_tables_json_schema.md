## Mô tả chung
Tài liệu này mô tả **JSON schema** cho các bảng HIVE sinh ra từ danh sách truy vấn InfluxDB (min / max / mean).

### Quy ước chung
- Mỗi metric + phép đo (min/max/mean/last/capacity) → **1 bảng HIVE**
- Định dạng lưu trữ: **PARQUET**
- Time:
  - `time` : STRING (ISO-8601 hoặc YYYY-mm-ddTHH:00:00Z)
  - `time_ts` : BIGINT (Unix timestamp ms)

---

## Schema cột chuẩn (base schema)
```json
{
  "name": "string – tên measurement gốc từ InfluxDB",
  "customer": "string – mã khách hàng",
  "host": "string – hostname / ip",
  "project": "string – project / service",
  "tags": "struct – các tag động (path, name, speed, ds_name, …)",
  "time": "string – thời gian tổng hợp theo giờ",
  "<agg_value>": "double – giá trị metric (min/max/mean/last)",
  "time_ts": "bigint – unix timestamp (ms)"
}
```

---

## 1. mem_project__util
### Bảng
- `mem_project__util__mean`
- `mem_project__util__max`
- `mem_project__util__min`

### JSON mô tả
```json
{
  "table": "mem_project__util__mean",
  "description": "Mức sử dụng bộ nhớ (%) theo project – giá trị trung bình theo giờ",
  "columns": {
    "name": {"type": "string", "desc": "measurement InfluxDB"},
    "customer": {"type": "string", "desc": "khách hàng"},
    "host": {"type": "string", "desc": "máy chủ"},
    "project": {"type": "string", "desc": "project"},
    "tags": {"type": "struct", "desc": "tag mở rộng (nếu có)"},
    "time": {"type": "string", "desc": "thời gian theo giờ"},
    "mean": {"type": "double", "desc": "% bộ nhớ sử dụng trung bình"},
    "time_ts": {"type": "bigint", "desc": "unix timestamp (ms)"}
  }
}
```

---

## 2. swap_used_percent
```json
{
  "table": "swap_used_percent__max",
  "description": "Tỷ lệ swap sử dụng (%) – giá trị lớn nhất theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": "struct",
    "time": "string",
    "max": "double – % swap sử dụng cao nhất",
    "time_ts": "bigint"
  }
}
```

---

## 3. la_per_cpu
```json
{
  "table": "la_per_cpu__mean",
  "description": "Load average trên mỗi CPU – trung bình theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": "struct",
    "time": "string",
    "mean": "double – load average / CPU",
    "time_ts": "bigint"
  }
}
```

---

## 4. cpu usage (project normalized)
```json
{
  "table": "usage__max",
  "description": "CPU usage (%) theo project – giá trị cao nhất theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": "struct",
    "time": "string",
    "max": "double – % CPU usage",
    "time_ts": "bigint"
  }
}
```

---

## 5. disk_used_percent
```json
{
  "table": "disk_used_percent__mean",
  "description": "Dung lượng disk đã dùng (%) theo path – trung bình theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": {
      "type": "struct",
      "fields": {"path": "string"}
    },
    "time": "string",
    "mean": "double – % disk sử dụng",
    "time_ts": "bigint"
  }
}
```

---

## 6. disk_project__io_util
```json
{
  "table": "disk_project__io_util__max",
  "description": "IO utilization disk (%) theo project – max theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": {"name": "string"},
    "time": "string",
    "max": "double – % IO utilization",
    "time_ts": "bigint"
  }
}
```

---

## 7. net_customer__util
```json
{
  "table": "net_customer__util__mean",
  "description": "Mức sử dụng network (%) theo customer – trung bình theo giờ",
  "columns": {
    "name": "string",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": {"speed": "string"},
    "time": "string",
    "mean": "double – % network utilization",
    "time_ts": "bigint"
  }
}
```

---

## 8. vSphere HostSystem metrics
### mem_usage_average / cpu_usage_average
```json
{
  "table": "cpu_usage_average__max",
  "description": "CPU usage của ESXi host – max theo giờ (đã chuẩn hóa %)",
  "columns": {
    "name": "string – vsphere_hostsystem",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": {"name": "string – ESXi host"},
    "time": "string",
    "max": "double – % CPU usage",
    "time_ts": "bigint"
  }
}
```

---

## 9. datastore capacity (free_space__capacity)
```json
{
  "table": "free_space__capacity",
  "description": "Tỷ lệ sử dụng datastore (%) từ free_space / capacity",
  "columns": {
    "name": "string – vsphere_datastore",
    "customer": "string",
    "host": "string",
    "project": "string",
    "tags": {"ds_name": "string"},
    "time": "string",
    "value": "double – % datastore đã sử dụng",
    "time_ts": "bigint"
  }
}
```

---

## Gợi ý mở rộng
- Chuẩn hóa tên cột metric: **value** thay vì min/max/mean (kèm field `agg_type`)
- Partition HIVE theo `time` hoặc `YYYYMMDD`
- Sinh JSON schema tự động từ danh sách query

