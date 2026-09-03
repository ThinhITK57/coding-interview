# HƯỚNG DẪN THIẾT LẬP MÔI TRƯỜNG PHÁT TRIỂN TRÊN VMWARE WORKSTATION
## Ubuntu Server VM + i3 + Terminator + Docker + Minikube

> **Mục đích**: Tạo môi trường phát triển chuyên nghiệp, terminal-first cho Data Engineer.
> Máy ảo này đóng vai trò sandbox để luyện tập K8s, viết/test ETL jobs, và chuẩn bị
> kỹ năng Senior DE trước khi triển khai trên hạ tầng thật.

---

## MỤC LỤC

```
PHẦN 1: Mô Hình Tổng Thể — VM Dev vs Bare-Metal Prod
PHẦN 2: Cấu Hình VM Trên VMware Workstation
PHẦN 3: Cài Đặt Từng Bước (6 Bước)
PHẦN 4: Layout Terminator 4 Panes Cho Công Việc Hàng Ngày
PHẦN 5: Checklist Kiểm Tra Sau Cài Đặt
PHẦN 6: Workflow Hàng Ngày — Từ Code Đến Deploy
```

---

# PHẦN 1: MÔ HÌNH TỔNG THỂ

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ MÁY WINDOWS (VMware Workstation):                      CỤM BARE-METAL (3 máy vật lý):                │
│                                                                                                        │
│  ┌─────────────────────────────────────┐     ┌─────────────────────────────────────┐                   │
│  │ VM Ubuntu Server                    │     │ master / slave1 / slave2           │                   │
│  │  • i3 + Terminator + Gruvbox       │     │ Hadoop, Kafka, Spark, PG,          │                   │
│  │  • Docker + Minikube               │     │ Redis, Hive, Airflow, Debezium     │                   │
│  │  • kubectl, k9s, Helm              │     │ (Native systemd — giữ nguyên)      │                   │
│  │  • Git, Vim/Neovim                 │     │                                     │                   │
│  │  • Python, PySpark                 │     │                                     │                   │
│  │                                     │     │                                     │                   │
│  │  Vai trò: HỌC + DEV + TEST        │     │ Vai trò: STAGING + PRODUCTION      │                   │
│  └─────────────────────────────────────┘     └─────────────────────────────────────┘                   │
│                                                                                                        │
│  Code viết trên VM → test Minikube (APP_ENV=local) → push Git → deploy bare-metal (APP_ENV=prod)      │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 2: CẤU HÌNH VM TRÊN VMWARE WORKSTATION

### 2.1 Thông Số Khuyên Dùng:

```
┌──────────────────────────┬──────────────────────────────────────────┐
│ Thông số VM               │ Giá trị khuyên dùng                      │
├──────────────────────────┼──────────────────────────────────────────┤
│ CPU                      │ 4 cores (tối thiểu 2)                    │
│ RAM                      │ 8 GB (tối thiểu 6 GB)                    │
│ Disk                     │ 60 GB (thin provisioning)                │
│ OS                       │ Ubuntu Server 22.04 LTS (Không cần GUI) │
│ Network                  │ NAT hoặc Bridged (nếu cần kết nối LAN)  │
│ Nested Virtualization    │ BẬT (nếu Minikube dùng driver KVM)      │
└──────────────────────────┴──────────────────────────────────────────┘
```

### 2.2 Cài Đặt Nested Virtualization:

Trong VMware Workstation:
1. VM Settings → Processors
2. Chọn **"Virtualize Intel VT-x/EPT or AMD-V/RVI"**
3. OK → Khởi động VM

> **Lưu ý**: Nếu dùng Minikube driver `docker` thì bước này **không bắt buộc**
> nhưng vẫn nên bật để có thêm lựa chọn driver `kvm2` nếu cần sau này.

### 2.3 Phân Bổ RAM Khuyến Nghị:

Giả sử máy Host Windows có 16GB RAM:
```
Windows Host:      4 GB (OS + IDE/Browser)
VM Ubuntu Server:  8 GB (i3 + Docker + Minikube)
└── Minikube:      4 GB (--memory=4096)
└── Docker:        ~2 GB (Docker Compose services)
└── OS + Tools:    ~2 GB (i3, Terminator, Python, Git)
```

Nếu máy Host có 32GB RAM:
```
Windows Host:      8 GB
VM Ubuntu Server:  16 GB
└── Minikube:      8 GB (--memory=8192) ← Thoải mái chạy Spark
```

---

# PHẦN 3: CÀI ĐẶT TỪNG BƯỚC

## Bước 1: Hệ Thống Cơ Bản + Môi Trường Terminal

```bash
# Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y

# Cài đặt i3 Window Manager + Terminator + Tools
sudo apt install -y xorg i3 i3status i3lock dmenu xinit
sudo apt install -y terminator vim git wget unzip curl htop
sudo apt install -y net-tools tree jq bat ripgrep fd-find

# Cài đặt JetBrains Mono Nerd Font
mkdir -p ~/.local/share/fonts && cd ~/.local/share/fonts
wget https://github.com/ryanoasis/nerd-fonts/releases/download/v3.2.1/JetBrainsMono.zip
unzip JetBrainsMono.zip && rm JetBrainsMono.zip && fc-cache -fv

# Cấu hình Terminator Gruvbox theme
mkdir -p ~/.config/terminator
cat > ~/.config/terminator/config << 'EOF'
[global_config]
  title_hide_sizetext = True
  title_transmit_bg_color = "#d65d0e"
  title_inactive_bg_color = "#3c3836"

[keybindings]

[profiles]
  [[default]]
    background_color = "#282828"
    cursor_color = "#ebdbb2"
    font = JetBrainsMono Nerd Font 13
    foreground_color = "#ebdbb2"
    palette = "#282828:#cc241d:#98971a:#d79921:#458588:#b16286:#689d6a:#a89984:#928374:#fb4934:#b8bb26:#fabd2f:#83a598:#d3869b:#8ec07c:#ebdbb2"
    scrollback_infinite = True
    use_system_font = False

[layouts]
  [[default]]
    [[[window0]]]
      type = Window
    [[[child1]]]
      type = Terminal
      parent = window0

[plugins]
EOF

# Cấu hình i3 cơ bản
echo 'exec i3' > ~/.xinitrc
```

## Bước 2: Docker Engine

```bash
# Cài Docker Engine (bắt buộc cho Minikube driver=docker)
sudo apt install -y ca-certificates gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Cho phép user chạy Docker không cần sudo
sudo usermod -aG docker $USER
newgrp docker

# Kiểm tra
docker run hello-world
```

## Bước 3: kubectl + Minikube

```bash
# kubectl — CLI giao tiếp với K8s API
curl -LO "https://dl.k8s.io/release/$(curl -L -s \
  https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl

# Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# Khởi tạo cluster lần đầu
minikube start --driver=docker --cpus=2 --memory=4096 --disk-size=30g

# Kiểm tra
kubectl get nodes          # NAME=minikube, STATUS=Ready
kubectl get pods -A        # Thấy coredns, etcd, apiserver, scheduler...
```

## Bước 4: Công Cụ Bổ Trợ (k9s, Helm, make)

```bash
# k9s — TUI quản lý K8s (thay thế kubectl cho thao tác nhanh)
curl -sS https://webinstall.dev/k9s | bash

# Helm — Package Manager cho K8s
curl https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash

# make (thường đã có sẵn, nếu chưa thì:)
sudo apt install -y build-essential
```

## Bước 5: Python + PySpark

```bash
sudo apt install -y python3 python3-pip python3-venv
pip3 install pyyaml pytest pyspark redis flake8
```

## Bước 6: Clone Repo và Bắt Đầu Làm Việc

```bash
git clone https://github.com/<your-repo>/fleet-platform.git
cd fleet-platform

# Kiểm tra config hoạt động
APP_ENV=local python3 -m src.core.config

# Chạy tests
make test

# Bật Docker Compose local sandbox
make up
make smoke-test
```

---

# PHẦN 4: LAYOUT TERMINATOR 4 PANES

Sau khi `startx` vào i3, mở Terminator rồi chia panes:
- `Ctrl+Shift+E` — Chia dọc (Vertical Split)
- `Ctrl+Shift+O` — Chia ngang (Horizontal Split)
- `Alt+Arrow` — Di chuyển giữa các panes

```
┌─────────────────────────────────────┬─────────────────────────────────────┐
│ PANE 1: CODE + GIT                  │ PANE 2: K8S MANAGEMENT              │
│                                     │                                     │
│  vim src/jobs/scd2_customer_job.py   │  k9s                                │
│  git status                          │  (Navigate: Pods, Logs, Describe)   │
│  git add . && git commit -m "..."    │                                     │
│  make test                           │                                     │
├─────────────────────────────────────┼─────────────────────────────────────┤
│ PANE 3: BUILD + DEPLOY              │ PANE 4: MONITORING + LOGS           │
│                                     │                                     │
│  eval $(minikube docker-env)         │  kubectl logs -f job/scd2-job       │
│  docker build -t fleet-etl:v1 .      │  kubectl top pods                   │
│  kubectl apply -f k8s/job.yaml       │  htop                               │
│  make config env=prod                │  docker stats                       │
└─────────────────────────────────────┴─────────────────────────────────────┘
```

---

# PHẦN 5: CHECKLIST KIỂM TRA SAU CÀI ĐẶT

Chạy lần lượt từng lệnh, tất cả phải pass:

```bash
# 1. Terminal đẹp
startx                                    # i3 khởi động
# Super+Enter → mở Terminator với Gruvbox theme
echo "✓ i3 + Terminator OK"

# 2. Docker hoạt động
docker run hello-world                    # ✓ Hello from Docker!
echo "✓ Docker OK"

# 3. Minikube hoạt động
minikube status                           # host: Running, kubelet: Running
echo "✓ Minikube OK"

# 4. kubectl giao tiếp được cluster
kubectl get nodes                         # minikube   Ready
echo "✓ kubectl OK"

# 5. k9s hoạt động
k9s                                       # TUI hiển thị Pods (Ctrl+C thoát)
echo "✓ k9s OK"

# 6. Helm hoạt động
helm version                              # version.BuildInfo{Version:"v3.x.x"...}
echo "✓ Helm OK"

# 7. Fleet Platform config hoạt động
cd ~/fleet-platform
APP_ENV=local python3 -m src.core.config  # In ra JSON config local
echo "✓ Config Loader OK"

# 8. Tests passed
make test                                 # 13 passed
echo "✓ Tests OK"
```

---

# PHẦN 6: WORKFLOW HÀNG NGÀY — TỪ CODE ĐẾN DEPLOY

### 6.1 Buổi Sáng — Bật Môi Trường:
```bash
# Bật Minikube (nếu đã stop từ hôm trước)
minikube start

# Bật Docker Compose sandbox (Postgres, Redis, Redpanda, MinIO)
cd ~/fleet-platform
make up
make status
```

### 6.2 Trong Ngày — Code → Test → Deploy:
```bash
# Viết / sửa code
vim src/jobs/scd2_customer_job.py

# Chạy unit tests
make test

# Kiểm tra config
make config env=local
make config env=prod

# Build Docker image (cho K8s deployment)
eval $(minikube docker-env)
docker build -t fleet-etl:v1 .

# Deploy lên Minikube
kubectl apply -f k8s/jobs/scd2-job.yaml

# Xem logs
kubectl logs -f job/scd2-customer-job

# Commit code
git add .
git commit -m "feat: refactor SCD2 job with config loader"
git push origin main
```

### 6.3 Cuối Ngày — Tắt Môi Trường:
```bash
# Tắt Docker Compose sandbox
make down

# Dừng Minikube (giữ dữ liệu, khởi động nhanh ngày hôm sau)
minikube stop
```

### 6.4 Deploy Lên Cụm Bare-Metal (Khi Code Đã Test Xong):
```bash
# SSH vào máy master bare-metal
ssh aiguystory@192.168.1.10

# Pull code mới nhất
cd /opt/fleet-platform
git pull origin main

# Deploy bằng spark-submit
APP_ENV=prod spark-submit \
  --master spark://master:7077 \
  --deploy-mode client \
  src/jobs/scd2_customer_job.py

# Hoặc thông qua Airflow (tự động theo schedule)
```

---

# PHỤ LỤC: PHÍM TẮT I3 + TERMINATOR CẦN THUỘC

### i3 Window Manager:
```
Super + Enter          → Mở Terminal mới
Super + d              → Mở dmenu (app launcher)
Super + Shift + q      → Đóng cửa sổ hiện tại
Super + 1-9            → Chuyển workspace
Super + Shift + 1-9    → Di chuyển cửa sổ sang workspace khác
Super + h/v            → Chia cửa sổ horizontal / vertical
Super + f              → Fullscreen
Super + Shift + r      → Reload i3 config
Super + Shift + e      → Thoát i3
```

### Terminator:
```
Ctrl + Shift + E       → Chia dọc (Vertical Split)
Ctrl + Shift + O       → Chia ngang (Horizontal Split)
Alt + Arrow Keys       → Di chuyển giữa panes
Ctrl + Shift + W       → Đóng pane hiện tại
Ctrl + Shift + X       → Phóng to/thu nhỏ pane (Toggle Zoom)
Ctrl + Shift + T       → Mở tab mới
Ctrl + PageUp/Down     → Chuyển tab
```
