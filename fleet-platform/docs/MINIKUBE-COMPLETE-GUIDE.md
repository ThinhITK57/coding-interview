# HƯỚNG DẪN TOÀN DIỆN MINIKUBE CHO DATA ENGINEER
## Từ Khái Niệm Nền Tảng → Thực Hành Terminal → Kết Nối Production

> **Mục đích**: Tài liệu này phục vụ cho việc luyện tập thực chiến hàng ngày, giúp chuyển từ "người viết DAGs" thành "Senior Data Platform Engineer" có khả năng kiểm soát toàn bộ hạ tầng, tham số, và vòng đời triển khai của mỗi ETL job.

---

## MỤC LỤC

```
PHẦN 1: Minikube Là Gì — Hiểu Đúng Bản Chất
PHẦN 2: Cài Đặt & Khởi Tạo — Từng Bước Trên Terminal
PHẦN 3: 12 Khái Niệm Cốt Lõi Của K8s Phải Nắm Vững
PHẦN 4: 20 Lệnh CLI Phải Thuộc Lòng
PHẦN 5: 8 Bẫy Lỗi Kinh Điển Khi Dùng Minikube
PHẦN 6: Bẫy Quan Trọng Nhất — Docker Image Local Trong Minikube
PHẦN 7: Ví Dụ Thực Tế — Deploy PostgreSQL Lên Minikube Bằng YAML
PHẦN 8: Flow Làm Việc Hàng Ngày Của Senior DE Trên Minikube
PHẦN 9: Tóm Tắt 7 Điều Phải Nhớ
PHẦN 10: So Sánh Minikube Local vs Production Thật
PHẦN 11: Kustomize — 1 Bộ YAML Chạy Trên Cả Local Và Prod
PHẦN 12: 3 Con Đường Thực Tế Cho Bare-Metal + Minikube
```

---

# PHẦN 1: MINIKUBE LÀ GÌ — HIỂU ĐÚNG BẢN CHẤT

### 1.1 Một Câu Định Nghĩa Chuẩn Xác:
> **Minikube là một cụm Kubernetes 1-node chạy bên trong một máy ảo (VM) hoặc container trên chính laptop/desktop của bạn**, cho phép bạn học, phát triển và kiểm thử ứng dụng K8s mà không cần thuê cloud hay dựng cụm bare-metal thật.

### 1.2 Minikube KHÔNG Phải Là Gì:
```
┌──────────────────────────────────┬─────────────────────────────────────────────────────────────────────┐
│ Minikube LÀ                      │ Minikube KHÔNG PHẢI LÀ                                              │
├──────────────────────────────────┼─────────────────────────────────────────────────────────────────────┤
│ • K8s cluster thu nhỏ để DEV     │ • Hệ thống Production (Không bao giờ chạy prod trên Minikube)       │
│ • Sandbox để test Helm charts    │ • Thay thế cho EKS/GKE/AKS (Managed K8s trên cloud)                │
│ • Môi trường CI/CD local         │ • Công cụ quản lý Docker containers (Đó là Docker Compose)          │
│ • Nơi học K8s API cho Senior DE  │ • Giải pháp cho multi-node cluster (Đó là K3s/Kind/kubeadm)         │
└──────────────────────────────────┴─────────────────────────────────────────────────────────────────────┘
```

### 1.3 Kiến Trúc Bên Trong Minikube:
```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ MÁY LAPTOP CỦA BẠN (HOST OS: Ubuntu / Windows / macOS):                                                │
│                                                                                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ MINIKUBE NODE (VM hoặc Docker Container):                                                        │  │
│  │                                                                                                  │  │
│  │  ┌─── CONTROL PLANE (Master Components) ──────────────────────────────────────────────────────┐  │  │
│  │  │  • kube-apiserver    — Cổng giao tiếp duy nhất (REST API, Port 6443)                       │  │  │
│  │  │  • etcd              — Database lưu trữ toàn bộ trạng thái cluster (Key-Value Store)        │  │  │
│  │  │  • kube-scheduler    — Quyết định Pod chạy trên node nào                                    │  │  │
│  │  │  • kube-controller   — Vòng lặp điều khiển: đảm bảo desired state = actual state           │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                                  │  │
│  │  ┌─── WORKER (Chạy chung trên cùng node) ────────────────────────────────────────────────────┐  │  │
│  │  │  • kubelet           — Agent quản lý lifecycle của Pods trên node này                       │  │  │
│  │  │  • kube-proxy        — Quản lý Network Rules (iptables/IPVS) để Pods giao tiếp nhau         │  │  │
│  │  │  • Container Runtime — containerd / Docker Engine / CRI-O (chạy containers thực sự)         │  │  │
│  │  └────────────────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                                  │  │
│  │  ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐                       │  │
│  │  │ Pod: Postgres  │ │ Pod: Redis    │ │ Pod: Redpanda │ │ Pod: Spark Job│                       │  │
│  │  │ (port 5432)    │ │ (port 6379)   │ │ (port 9092)   │ │ (ephemeral)   │                       │  │
│  │  └───────────────┘ └───────────────┘ └───────────────┘ └───────────────┘                       │  │
│  └──────────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                        │
│  [kubectl] ──(Port 6443)──► [kube-apiserver] ──► [etcd, scheduler, controllers, kubelet]               │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 1.4 Chi Tiết Bên Trong 1 Node Minikube (Xem Qua SSH):

Khi chạy `minikube ssh` bạn sẽ thấy toàn bộ tiến trình sau:

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ BÊN TRONG NODE MINIKUBE (minikube ssh → ps aux):                                                        │
│                                                                                                        │
│  SYSTEM PROCESSES (PID 1+):                                                                            │
│  • systemd                        — Init system quản lý mọi tiến trình                                │
│  • containerd                     — Container Runtime                                                  │
│  • kubelet                        — Agent nhận lệnh từ API Server                                     │
│  • kube-proxy                     — Quản lý iptables rules                                            │
│                                                                                                        │
│  STATIC PODS (Control Plane — chạy dưới dạng containers):                                              │
│  • Pod: kube-apiserver-minikube   — REST API server (Port 6443)                                        │
│  • Pod: etcd-minikube             — Database Key-Value lưu cluster state                               │
│  • Pod: kube-scheduler-minikube   — Quyết định Pod nào chạy trên node nào                              │
│  • Pod: kube-controller-manager   — Vòng lặp reconciliation (desired vs actual)                        │
│                                                                                                        │
│  SYSTEM PODS (kube-system namespace):                                                                  │
│  • Pod: coredns-xxxxxxxxx         — DNS nội bộ cluster (phân giải tên Service → IP)                    │
│  • Pod: storage-provisioner       — Tự động tạo PersistentVolume khi có PVC mới                        │
│  • Pod: metrics-server (nếu bật)  — Thu thập CPU/RAM metrics cho kubectl top                           │
│  • Pod: dashboard (nếu bật)       — Web UI quản trị                                                   │
│  • Pod: ingress-nginx (nếu bật)   — Reverse proxy HTTP từ ngoài vào cluster                            │
│                                                                                                        │
│  YOUR PODS (default namespace):                                                                        │
│  • Pod: postgres-xxxx, redis-xxxx, spark-job-xxxx...                                                   │
│                                                                                                        │
│  STORAGE:                                                                                              │
│  • /var/lib/containerd/              — Layers của Docker images                                        │
│  • /var/lib/etcd/                    — Data của etcd (cluster state)                                   │
│  • /tmp/hostpath-provisioner/        — PersistentVolumes (dữ liệu Postgres, MinIO...)                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Lệnh tự kiểm chứng:
```bash
minikube ssh                              # SSH vào bên trong node
ps aux                                    # Xem tất cả tiến trình đang chạy
ls /var/lib/containerd/                   # Xem Docker image layers
ls /tmp/hostpath-provisioner/             # Xem PersistentVolume data
exit
```

---

# PHẦN 2: CÀI ĐẶT & KHỞI TẠO — TỪNG BƯỚC TRÊN TERMINAL

### 2.1 Cài Đặt Minikube Trên Ubuntu Server:
```bash
# 1. Cài đặt kubectl (CLI giao tiếp với K8s API)
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# 2. Cài đặt Minikube binary
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# 3. Xác nhận cài đặt thành công
minikube version
kubectl version --client --short
```

### 2.2 Chọn Driver:

```
┌──────────────────────────┬──────────────────────────────────────────┬──────────────────────────────────────────┐
│ Driver                   │ Cách Hoạt Động                            │ Khi Nào Dùng                              │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ docker (Khuyên dùng)     │ Chạy K8s bên trong 1 Docker container.  │ Máy đã cài Docker. Nhanh nhất.           │
│                          │ Không cần VM, nhẹ, khởi động < 30 giây. │ Dùng driver này cho Fleet Platform.      │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ virtualbox               │ Tạo 1 VM VirtualBox chạy Linux bên trong│ Máy không có Docker. Windows cũ.          │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ kvm2                     │ Tạo VM KVM trên Linux (hardware-accel). │ Linux server có hỗ trợ KVM.              │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ none                     │ Cài K8s trực tiếp lên OS (không VM).    │ CI/CD pipeline. Cần quyền root.          │
│                          │ ⚠️ NGUY HIỂM: Ảnh hưởng trực tiếp OS.   │ Không khuyên dùng cho máy dev.            │
└──────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────────┘
```

### 2.3 Lệnh Khởi Tạo Chuẩn Cho Data Engineer:
```bash
minikube start \
  --driver=docker \
  --cpus=4 \
  --memory=8192 \
  --disk-size=40g \
  --kubernetes-version=v1.30.0 \
  --addons=metrics-server,dashboard,ingress

# Giải thích từng cờ:
#   --driver=docker         → Chạy bên trong Docker container (nhẹ nhất)
#   --cpus=4                → Cấp 4 CPU cores cho cụm K8s
#   --memory=8192           → Cấp 8GB RAM (tối thiểu 4GB cho Spark jobs)
#   --disk-size=40g         → 40GB đĩa ảo (chứa Docker images + PV data)
#   --kubernetes-version    → Ghim phiên bản K8s cố định
#   --addons                → Bật sẵn Metrics Server + Dashboard + Ingress
```

---

# PHẦN 3: 12 KHÁI NIỆM CỐT LÕI CỦA K8S PHẢI NẮM VỮNG

```
┌──────────────────────────┬──────────────────────────────────────────────────────────────────────────────┐
│ Khái Niệm K8s            │ Giải Thích Bằng Ngôn Ngữ Data Engineer                                      │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 1. Pod                   │ Đơn vị nhỏ nhất chạy 1+ containers. Ví dụ: 1 Pod = 1 Spark Job.            │
│                          │ Pod là ephemeral — chết là mất dữ liệu nếu không mount volume.              │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 2. Deployment            │ Quản lý N bản sao (replicas) của Pod. Auto-restart nếu Pod crash.           │
│                          │ Ví dụ: Deployment PostgreSQL = 1 replica, Deployment Redis = 1 replica.     │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 3. Service               │ Cấp 1 DNS name + IP ổn định cho nhóm Pods phía sau.                         │
│                          │ Code gọi "postgres-svc:5432" thay vì IP Pod (vì IP Pod thay đổi).           │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 4. ConfigMap             │ Lưu trữ cấu hình dạng key-value (non-sensitive). Mount vào Pod.             │
│                          │ Ví dụ: base.yaml, local.yaml của chúng ta.                                  │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 5. Secret                │ Giống ConfigMap nhưng mã hóa Base64 (sensitive data).                       │
│                          │ Ví dụ: DB password, Redis password, API keys.                               │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 6. PersistentVolume (PV) │ Ổ đĩa vật lý trên node. Ví dụ: 10GB SSD dành cho PostgreSQL data.          │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 7. PersistentVolumeClaim │ "Đơn xin cấp đĩa" từ Pod. K8s tìm PV phù hợp và gắn vào Pod.             │
│   (PVC)                  │                                                                              │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 8. Job                   │ Chạy 1 Pod đến khi hoàn thành rồi tự dừng (exit code 0).                   │
│                          │ ĐÚNG cho Spark Batch ETL: chạy xong → kết thúc → giải phóng tài nguyên.    │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 9. CronJob               │ Job chạy theo lịch cron (giống Airflow schedule).                           │
│                          │ Ví dụ: CronJob "0 3 * * *" = chạy Batch DWH lúc 3h sáng mỗi ngày.         │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 10. Namespace            │ Phân vùng logic trong cùng 1 cluster.                                       │
│                          │ Ví dụ: namespace "dev" vs "staging" vs "prod" trên cùng Minikube.           │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 11. Ingress              │ Bộ định tuyến HTTP từ bên ngoài vào Services bên trong cluster.             │
│                          │ Ví dụ: http://fleet.local/api → Service Airflow Webserver.                  │
├──────────────────────────┼──────────────────────────────────────────────────────────────────────────────┤
│ 12. Helm Chart           │ "Package manager" của K8s. 1 Helm chart = 1 bộ YAML template tái sử dụng.  │
│                          │ Ví dụ: helm install postgres bitnami/postgresql → done.                     │
└──────────────────────────┴──────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 4: 20 LỆNH CLI PHẢI THUỘC LÒNG

```bash
# ─── QUẢN LÝ LIFECYCLE MINIKUBE ───
minikube start                        # Khởi động cụm
minikube stop                         # Dừng cụm (giữ dữ liệu)
minikube delete                       # XÓA hoàn toàn cụm
minikube status                       # Kiểm tra trạng thái
minikube dashboard                    # Mở Web UI K8s trên trình duyệt

# ─── QUẢN LÝ PODS & DEPLOYMENTS ───
kubectl get pods                      # Liệt kê Pods (namespace default)
kubectl get pods -A                   # Liệt kê ALL Pods trên ALL namespaces
kubectl get pods -o wide              # Hiển thị thêm IP, Node, Age
kubectl describe pod <tên-pod>        # Chi tiết Events → DEBUG lỗi
kubectl logs <tên-pod>                # Xem stdout/stderr container
kubectl logs <tên-pod> -f             # Logs realtime (giống tail -f)
kubectl logs <tên-pod> --previous     # Log của container đã crash trước đó

# ─── DEPLOY & XÓA RESOURCES ───
kubectl apply -f manifest.yaml        # Tạo/Cập nhật resource từ file YAML
kubectl delete -f manifest.yaml       # Xóa resource đã deploy
kubectl get svc                       # Liệt kê Services
kubectl get pvc                       # Liệt kê PersistentVolumeClaims

# ─── TRUY CẬP SERVICE TỪ MÁY HOST ───
minikube service <tên-service>        # Mở browser trỏ tới Service
kubectl port-forward svc/postgres-svc 5432:5432   # Forward port ra localhost

# ─── DEBUG & EXEC VÀO CONTAINER ───
kubectl exec -it <tên-pod> -- bash    # SSH vào bên trong container
kubectl top pods                      # Xem CPU/RAM đang dùng
```

---

# PHẦN 5: 8 BẪY LỖI KINH ĐIỂN KHI DÙNG MINIKUBE

```
┌──────────────────────────┬──────────────────────────────────────────┬──────────────────────────────────────────┐
│ Bẫy Lỗi                  │ Hiện Tượng / Triệu Chứng                 │ Cách Xử Lý Chuẩn Xác                     │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 1. Pod Pending vô hạn   │ STATUS = Pending                         │ kubectl describe pod → Xem Events        │
│                          │                                          │ Thường do thiếu CPU/RAM/PVC.             │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 2. ImagePullBackOff      │ Pod không tải được Docker image.         │ Dùng image local: eval $(minikube        │
│                          │                                          │ docker-env) rồi docker build bên trong.  │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 3. CrashLoopBackOff      │ Container khởi động rồi chết liên tục.  │ kubectl logs <pod> --previous            │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 4. Không kết nối Service │ localhost:5432 nhưng PG trong Minikube.  │ kubectl port-forward hoặc                │
│    từ Host               │                                          │ minikube service <name> --url            │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 5. Hết đĩa Minikube     │ Pods bị Evicted.                         │ minikube ssh -- df -h                    │
│                          │                                          │ Tăng --disk-size khi minikube start.     │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 6. Docker image local    │ Build trên Host nhưng K8s không thấy.   │ eval $(minikube docker-env) TRƯỚC build  │
│    không thấy trong K8s  │                                          │ Hoặc: minikube image load <image>        │
│                          │                                          │ Hoặc: imagePullPolicy: Never             │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 7. DNS Resolution lỗi   │ "could not resolve host" giữa Pods.     │ Dùng FQDN: <svc>.<ns>.svc.cluster.local │
│                          │                                          │ Kiểm tra CoreDNS pod hoạt động.          │
├──────────────────────────┼──────────────────────────────────────────┼──────────────────────────────────────────┤
│ 8. Minikube chạy chậm   │ Khởi động > 5 phút.                     │ Tăng --cpus và --memory.                 │
│                          │                                          │ Dùng driver docker thay virtualbox.      │
└──────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────────┘
```

---

# PHẦN 6: BẪY QUAN TRỌNG NHẤT — DOCKER IMAGE LOCAL TRONG MINIKUBE

```bash
# ❌ SAI: Build image trên máy Host → Minikube KHÔNG NHÌN THẤY
docker build -t fleet-etl:v1 .
kubectl apply -f job.yaml        # → ImagePullBackOff

# ✅ ĐÚNG CÁCH 1: Chuyển Docker CLI trỏ vào Minikube daemon
eval $(minikube docker-env)      # Terminal này giao tiếp với Docker BÊN TRONG Minikube
docker build -t fleet-etl:v1 .   # Image nằm BÊN TRONG Minikube → K8s nhìn thấy

# ✅ ĐÚNG CÁCH 2: Load image từ Host vào Minikube
docker build -t fleet-etl:v1 .
minikube image load fleet-etl:v1

# ✅ ĐÚNG CÁCH 3: Set imagePullPolicy trong YAML manifest
#   spec:
#     containers:
#     - name: etl-job
#       image: fleet-etl:v1
#       imagePullPolicy: Never
```

---

# PHẦN 7: VÍ DỤ THỰC TẾ — DEPLOY POSTGRESQL LÊN MINIKUBE

```yaml
# 1. PersistentVolumeClaim — Xin cấp 5GB đĩa
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: postgres-pvc
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 5Gi
---
# 2. Deployment — Pod PostgreSQL (1 replica, auto-restart)
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:14-alpine
        ports:
        - containerPort: 5432
        env:
        - name: POSTGRES_DB
          value: "fleet_oltp"
        - name: POSTGRES_USER
          value: "fleet_app"
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: fleet-secrets
              key: db-password
        volumeMounts:
        - name: pg-data
          mountPath: /var/lib/postgresql/data
      volumes:
      - name: pg-data
        persistentVolumeClaim:
          claimName: postgres-pvc
---
# 3. Service — DNS "postgres-svc" ổn định
apiVersion: v1
kind: Service
metadata:
  name: postgres-svc
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
    targetPort: 5432
  type: ClusterIP
```

Deploy và kiểm tra:
```bash
kubectl apply -f k8s/base/postgres.yaml
kubectl get pods                                       # Chờ STATUS = Running
kubectl port-forward svc/postgres-svc 5432:5432        # Forward ra localhost
psql -h localhost -U fleet_app -d fleet_oltp           # Kết nối từ máy Host
```

---

# PHẦN 8: FLOW LÀM VIỆC HÀNG NGÀY CỦA SENIOR DE

```
┌─────────────────────────────────────┬─────────────────────────────────────┐
│ PANE 1: Code & Git                  │ PANE 2: K8s Management              │
│  vim src/jobs/scd2_job.py            │  k9s                                │
│  git add . && git commit             │  (TUI quản lý Pods, Logs, Exec)     │
├─────────────────────────────────────┼─────────────────────────────────────┤
│ PANE 3: Build & Test                │ PANE 4: Monitoring & Logs           │
│  make test                           │  kubectl logs -f <spark-job-pod>    │
│  eval $(minikube docker-env)         │  kubectl top pods                   │
│  docker build -t fleet-etl:v2 .      │  minikube dashboard                 │
│  kubectl apply -f k8s/job.yaml       │                                     │
└─────────────────────────────────────┴─────────────────────────────────────┘

FLOW: Code → Build Image → Deploy K8s Job → Watch Logs → Fix → Repeat
```

```bash
# Bước 1: Bật Minikube
minikube start --driver=docker --cpus=4 --memory=8192

# Bước 2: Chuyển Docker CLI vào Minikube
eval $(minikube docker-env)

# Bước 3: Build Docker image
docker build -t fleet-etl:v1 .

# Bước 4: Deploy job lên K8s
kubectl apply -f k8s/jobs/scd2-job.yaml

# Bước 5: Theo dõi logs realtime
kubectl logs -f job/scd2-customer-job

# Bước 6: Kiểm tra tài nguyên
kubectl top pods

# Bước 7: Kết thúc ngày — dừng Minikube
minikube stop
```

---

# PHẦN 9: TÓM TẮT 7 ĐIỀU PHẢI NHỚ

1. **Luôn chỉ định `--driver`, `--cpus`, `--memory`, `--disk-size`** khi `minikube start`.
2. **Dùng `eval $(minikube docker-env)`** trước khi `docker build`.
3. **Set `imagePullPolicy: Never`** trong YAML manifest khi dùng image local.
4. **Dùng `kubectl port-forward`** để truy cập Service từ Host.
5. **Dùng `kubectl describe pod` để debug** — phần Events chứa thông tin chi tiết nhất.
6. **`minikube stop` khi không dùng** — tiết kiệm RAM và CPU.
7. **`minikube delete` khi muốn làm sạch hoàn toàn** — reset về trạng thái ban đầu.

---

# PHẦN 10: SO SÁNH MINIKUBE LOCAL VS PRODUCTION THẬT

```
┌──────────────────────────┬──────────────────────────────────┬──────────────────────────────────────────┐
│ Tiêu Chí                  │ Minikube Local (Dev Sandbox)      │ Production Thật (Bare-Metal / K8s)       │
├──────────────────────────┼──────────────────────────────────┼──────────────────────────────────────────┤
│ Số lượng Nodes            │ 1 node duy nhất                  │ 3-100+ nodes                             │
│ Tài nguyên                │ 4 CPU, 8GB RAM                   │ Mỗi node 16-64 CPU, 64-256GB RAM        │
│ Storage                   │ PV trên đĩa ảo 40GB             │ Ceph / NFS / EBS / HDFS (hàng TB)        │
│ Networking                │ ClusterIP + port-forward         │ LoadBalancer / Ingress Controller        │
│ High Availability         │ ❌ Không                          │ ✅ Multi-master, Pod anti-affinity        │
│ Secrets Management        │ kubectl create secret             │ HashiCorp Vault / AWS Secrets Manager    │
│ Monitoring                │ kubectl top + dashboard          │ Prometheus + Grafana + AlertManager      │
│ Docker Image              │ Build local + imagePullPolicy    │ Push lên Registry (ECR/GCR/Harbor)       │
├──────────────────────────┼──────────────────────────────────┼──────────────────────────────────────────┤
│ YAML Manifests            │ ✅ DÙNG CHUNG (qua Kustomize)     │ ✅ DÙNG CHUNG                             │
│ Code Logic                │ ✅ DÙNG CHUNG                     │ ✅ DÙNG CHUNG                             │
│ kubectl commands           │ ✅ DÙNG CHUNG                     │ ✅ DÙNG CHUNG                             │
│ Helm Charts               │ ✅ DÙNG CHUNG                     │ ✅ DÙNG CHUNG                             │
└──────────────────────────┴──────────────────────────────────┴──────────────────────────────────────────┘
```

Khác 5%: Storage Class, Image Registry, Secrets management, Replicas, Networking.
Giống 95%: Code, YAML, kubectl, Helm — tất cả dùng chung.

---

# PHẦN 11: KUSTOMIZE — 1 BỘ YAML CHẠY TRÊN CẢ LOCAL VÀ PROD

```
k8s/
├── base/                              # YAML gốc — DÙNG CHUNG
│   ├── kustomization.yaml
│   ├── postgres.yaml
│   ├── redis.yaml
│   ├── configmap.yaml
│   └── spark-job.yaml
│
├── overlays/
│   ├── local/                         # Ghi đè cho Minikube
│   │   ├── kustomization.yaml
│   │   └── patches/
│   │       └── spark-patch.yaml       # resources: 1 CPU, 2GB RAM
│   │
│   └── prod/                          # Ghi đè cho Production
│       ├── kustomization.yaml
│       └── patches/
│           └── spark-patch.yaml       # resources: 8 CPU, 16GB RAM
```

Lệnh deploy:
```bash
kubectl apply -k k8s/overlays/local/     # Deploy lên Minikube
kubectl apply -k k8s/overlays/prod/      # Deploy lên Production
kubectl kustomize k8s/overlays/prod/     # Preview YAML đã merge (không deploy)
```

---

# PHẦN 12: 3 CON ĐƯỜNG THỰC TẾ CHO BARE-METAL + MINIKUBE

```
┌──────────────────────────┬──────────────────────────────────┬─────────────────────┬──────────────────┐
│ Con Đường                 │ Mô Tả                            │ Ưu Điểm              │ Nhược Điểm        │
├──────────────────────────┼──────────────────────────────────┼─────────────────────┼──────────────────┤
│ A. Giữ Bare-Metal        │ Không dùng K8s. Dùng systemd +  │ • Đã quen thuộc     │ • Không học K8s  │
│    như hiện tại           │ spark-submit + Airflow Bash.     │ • Ổn định            │ • Deploy thủ công│
├──────────────────────────┼──────────────────────────────────┼─────────────────────┼──────────────────┤
│ B. Minikube trên VM      │ Minikube CHỈ trên VM để học K8s  │ • An toàn            │ • 2 thế giới     │
│    + Bare-Metal giữ Prod │ và test code. Cụm 3 máy vẫn     │ • Học K8s nhanh     │   tách biệt      │
│    ✅ KHUYÊN DÙNG          │ chạy native.                     │ • Code param-driven │                  │
├──────────────────────────┼──────────────────────────────────┼─────────────────────┼──────────────────┤
│ C. K3s thật lên cụm      │ Cài K8s multi-node lên cụm 3    │ • K8s thật 100%     │ • Phức tạp       │
│    3 máy (tương lai)      │ máy bằng K3s.                    │ • Production-ready  │ • Rủi ro cao     │
└──────────────────────────┴──────────────────────────────────┴─────────────────────┴──────────────────┘
```

Khuyến nghị: Bắt đầu với **Con đường B**, khi đã thành thạo K8s trên Minikube (2-4 tuần) thì đánh giá lại có nên chuyển sang C hay không.

Dấu hiệu nên chuyển sang C (K3s trên bare-metal):
- Master chạy 10+ processes tranh chấp RAM/CPU
- Deploy thủ công mệt mỏi (SSH → git pull → restart systemd)
- Cần rollback nhanh khi deploy lỗi
- Cần auto-healing (service chết tự restart)
- Team lớn hơn 3 người cùng deploy
