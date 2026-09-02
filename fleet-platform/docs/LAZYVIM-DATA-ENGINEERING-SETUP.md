# HƯỚNG DẪN THIẾT LẬP TERMINAL IDE: LAZYVIM CHO DATA ENGINEER
## Code PySpark & SQL Chuyên Nghiệp 100% Trong Terminator

> **Mục đích**: Hướng dẫn chi tiết từng bước biến Terminal (Terminator) thành một **IDE hoàn chỉnh tương đương VS Code / PyCharm / DBeaver**, nhưng siêu nhẹ (khởi động 0.03s, tốn < 100MB RAM), phục vụ code PySpark, SQL, dọn dẹp data và debug pipeline mà không cần bất kỳ giao diện đồ họa (GUI) nào.

---

## 📋 MỤC LỤC

```
PHẦN 1: Tổng Quan Kiến Trúc Terminal IDE
PHẦN 2: Cài Đặt Từng Bước (Neovim >= 0.10 + LazyVim)
PHẦN 3: Cấu Hình PySpark & Python LSP (Auto-complete & Type Checking)
PHẦN 4: Cấu Hình SQL Intelligence & Auto-Format (SQLFluff + SQLS)
PHẦN 5: Bộ Công Cụ TUI Bổ Trợ (Harlequin, pgcli, VisiData, LazyGit)
PHẦN 6: Bảng Phím Tắt Vàng (Cheat Sheet) Thao Tác Trong LazyVim
PHẦN 7: Bố Trí Layout 4 Panes Hoàn Chỉnh Trên Terminator
```

---

# PHẦN 1: TỔNG QUAN KIẾN TRÚC TERMINAL IDE

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ KIẾN TRÚC TERMINAL IDE CHO DATA ENGINEER:                                                              │
│                                                                                                        │
│  [Terminator Terminal Emulator (i3 Window Manager + Gruvbox Theme)]                                    │
│  │                                                                                                     │
│  ├── 1. Code Editor: NEOVIM + LAZYVIM                                                                  │
│  │   • Package Manager   : lazy.nvim (tải và quản lý plugins siêu tốc)                                │
│  │   • Syntax Highlighting: nvim-treesitter (tô màu code chính xác theo ngữ pháp AST)                  │
│  │   • Language Servers  : Mason + Pyright (PySpark/Python) + sqlls/sqlfluff (SQL) + yamlls (YAML)     │
│  │   • Auto-completion   : nvim-cmp / blink.cmp (gợi ý hàm, tham số, biến khi gõ)                      │
│  │   • Fuzzy Finder      : Telescope / FZF (tìm file, tìm chuỗi text trên toàn repo)                  │
│  │   • File Explorer     : Neo-tree (cây thư mục dự án bên trái)                                      │
│  │   • Git In-Editor     : Gitsigns (thấy ngay dòng vừa sửa) + LazyGit (Space+g+g)                     │
│  │                                                                                                     │
│  ├── 2. SQL & Data TUI Tools (Chạy độc lập trên các panes khác):                                       │
│  │   • Harlequin         : SQL IDE TUI (preview bảng, chạy query Postgres, DuckDB, Parquet)           │
│  │   • pgcli             : CLI Postgres thông minh (auto-complete tên bảng, tên cột)                  │
│  │   • VisiData          : Soi dữ liệu Parquet, CSV, JSON hàng triệu dòng ngay trong terminal         │
│  │   • LazyGit           : Quản lý Git commits, branches trực quan dạng TUI                            │
│  │   • k9s               : Quản trị Kubernetes cluster / Minikube pods                                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 2: CÀI ĐẶT TỪNG BƯỚC

### Bước 2.1: Cài Đặt Dependencies Cơ Bản
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl wget unzip ripgrep fd-find build-essential python3-pip python3-venv

# Cài Node.js LTS (bắt buộc để Mason cài các Language Servers)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs
```

### Bước 2.2: Cài Đặt Neovim Mới Nhất (>= 0.10.0)
```bash
# Tải bản release chính thức từ GitHub
curl -LO https://github.com/neovim/neovim/releases/latest/download/nvim-linux64.tar.gz
sudo rm -rf /opt/nvim
sudo tar -C /opt -xzf nvim-linux64.tar.gz
sudo ln -sf /opt/nvim-linux64/bin/nvim /usr/local/bin/nvim
rm nvim-linux64.tar.gz

# Kiểm tra phiên bản (phải >= v0.10.x)
nvim --version
```

### Bước 2.3: Cài Đặt LazyVim Starter
```bash
# Sao lưu cấu hình nvim cũ (nếu có)
mv ~/.config/nvim ~/.config/nvim.bak 2>/dev/null || true
mv ~/.local/share/nvim ~/.local/share/nvim.bak 2>/dev/null || true

# Clone LazyVim starter
git clone https://github.com/LazyVim/starter ~/.config/nvim
rm -rf ~/.config/nvim/.git
```

---

# PHẦN 3: CẤU HÌNH PYSPARK & PYTHON LSP

Để LazyVim gợi ý code PySpark (`df.select()`, `col()`, `withColumn()`), chúng ta kích hoạt Python extra của LazyVim và cấu hình Pyright.

### Bước 3.1: Kích Hoạt Python & SQL Extras
Tạo file `~/.config/nvim/lua/config/lazy.lua` hoặc thêm vào `lua/plugins/` các extras:

Mở Neovim:
```bash
nvim
```
Gõ lệnh trong Neovim:
```vim
:LazyExtras
```
Dùng phím `j`/`k` di chuyển, bấm `x` để bật các extras sau:
- `lang.python` (Tự động kích hoạt Pyright/BasedPyright, Black, Ruff)
- `lang.sql` (Tự động kích hoạt SQL tooling)
- `lang.yaml` (Cấu hình cho file `configs/*.yaml`)
- `lang.docker` (Cấu hình cho Dockerfile & Docker Compose)
- `lang.json` (Cấu hình cho JSON schema)

### Bước 3.2: Cài Đặt Python Tools trên OS
```bash
pip3 install --user pyright black isort flake8 ruff pyspark
```

### Bước 3.3: Tinh Chỉnh Pyright Nhận Diện PySpark Trong Dự Án
Tạo file `pyrightconfig.json` ở thư mục gốc `fleet-platform/`:
```json
{
  "include": ["src", "analytics", "streaming", "serving", "tests"],
  "venvPath": ".",
  "venv": ".venv",
  "pythonVersion": "3.10",
  "typeCheckingMode": "basic"
}
```

---

# PHẦN 4: CẤU HÌNH SQL INTELLIGENCE & AUTO-FORMAT

### Bước 4.1: Cài Đặt SQLFluff & SQL Language Server
```bash
pip3 install --user sqlfluff
```

### Bước 4.2: Cấu Hình SQLFluff Cho Dự Án
Tạo file `.sqlfluff` tại thư mục gốc `fleet-platform/`:
```ini
[sqlfluff]
dialect = postgres
max_line_length = 120
indent_unit = space
indent_length = 4

[sqlfluff:rules:capitalisation.keywords]
capitalisation_policy = upper

[sqlfluff:rules:capitalisation.identifiers]
capitalisation_policy = lower
```

---

# PHẦN 5: BỘ CÔNG CỤ TUI BỔ TRỢ CHO DATA ENGINEER

Cài đặt 4 công cụ TUI chạy trực tiếp trong Terminator:

```bash
# 1. Harlequin — SQL IDE TUI hoàn chỉnh (Postgres + DuckDB + SQLite + Parquet)
pip3 install --user "harlequin[postgres,duckdb]"

# 2. pgcli — PostgreSQL CLI thông minh (auto-complete tên bảng, tên cột)
sudo apt install -y pgcli

# 3. VisiData — Soi dữ liệu Parquet/CSV hàng triệu dòng cực nhanh
pip3 install --user visidata

# 4. LazyGit — Quản lý Git trực quan
LAZYGIT_VERSION=$(curl -s "https://api.github.com/repos/jesseduffield/lazygit/releases/latest" | grep -Po '"tag_name": "v\K[^"]*')
curl -Lo lazygit.tar.gz "https://github.com/jesseduffield/lazygit/releases/latest/download/lazygit_${LAZYGIT_VERSION}_Linux_x86_64.tar.gz"
tar xf lazygit.tar.gz lazygit
sudo install lazygit /usr/local/bin
rm lazygit lazygit.tar.gz
```

### Cách Sử Dụng Từng Công Cụ:

#### 1. Harlequin (Thay thế DBeaver/DataGrip trong Terminal):
```bash
# Kết nối Postgres local sandbox
harlequin -a postgres --host localhost --port 5432 --user fleet_app --password local_dev_pass --dbname fleet_oltp

# Hoặc soi trực tiếp file Parquet trên đĩa:
harlequin -a duckdb data/warehouse/fact_repair_service_revenue.parquet
```
*Giao diện Harlequin*: Cột trái hiển thị Schemas & Tables, khung trên gõ SQL (bấm `Ctrl+Enter` để chạy), khung dưới hiển thị bảng kết quả đẹp mắt.

#### 2. pgcli (Gõ SQL nhanh):
```bash
pgcli postgresql://fleet_app:local_dev_pass@localhost:5432/fleet_oltp
# Gõ SELECT * FROM cu -> tự động gợi ý "customers", gõ status -> tự gợi ý
```

#### 3. VisiData (Soi file Parquet lớn):
```bash
vd data/warehouse/dim_customer.parquet
# Phím tắt:
#   Shift + F : Xem phân bố tần suất giá trị của cột (Frequency Table)
#   [ hoặc ]  : Sắp xếp tăng/giảm dần
#   /         : Tìm kiếm dữ liệu
#   q         : Thoát
```

---

# PHẦN 6: BẢNG PHÍM TẮT VÀNG (CHEAT SHEET) TRONG LAZYVIM

Phím `Leader` mặc định trong LazyVim là phím **`Space` (Phím Cách)**.

```
┌──────────────────────────────┬────────────────────────────────────────────────────────────────────────┐
│ Phím Bấm                     │ Hành Động                                                              │
├──────────────────────────────┼────────────────────────────────────────────────────────────────────────┤
│ ─── FILE & TÌM KIẾM ───      │                                                                        │
│ Space + e                    │ Bật / Tắt cây thư mục File Tree (Neo-tree)                             │
│ Space + Space (hoặc Space+f+f)│ Mở bảng tìm kiếm file siêu tốc (Fuzzy Finder)                          │
│ Space + s + g                │ Live Grep: Tìm chuỗi text trên toàn bộ dự án                           │
│ Space + f + b                │ Danh sách các Buffer (tab file) đang mở                                │
│                                                                                                        │
│ ─── ĐIỀU HƯỚNG CODE (LSP) ───│                                                                        │
│ g + d                        │ Go to Definition: Nhảy thẳng đến nơi khai báo hàm/class                │
│ g + r                        │ Go to References: Xem tất cả những nơi đang gọi hàm này                │
│ K                            │ Hover Doc: Xem tài liệu, kiểu dữ liệu và tham số của hàm/biến          │
│ Space + c + r                │ Rename: Đổi tên hàm/biến trên toàn bộ project an toàn                  │
│ Space + c + a                │ Code Action: Tự động sửa lỗi / import thư viện còn thiếu               │
│                                                                                                        │
│ ─── GIT TÍCH HỢP ───         │                                                                        │
│ Space + g + g                │ Mở LazyGit full màn hình (quản lý commit/push/diff)                    │
│ ] + c  /  [ + c              │ Nhảy đến đoạn code thay đổi tiếp theo / trước đó                       │
│ Space + g + h + p            │ Preview đoạn thay đổi Git ngay tại dòng đó                             │
│                                                                                                        │
│ ─── CỬA SỔ & TAB ───         │                                                                        │
│ Space + |                    │ Chia dọc màn hình code (Vertical Split)                                │
│ Space + -                    │ Chia ngang màn hình code (Horizontal Split)                            │
│ Ctrl + h / j / k / l         │ Di chuyển giữa các cửa sổ con                                          │
│ Space + b + d                │ Đóng buffer (file) hiện tại                                            │
│                                                                                                        │
│ ─── QUẢN LÝ LSP & PLUGINS ───│                                                                        │
│ :Mason                       │ Mở giao diện quản lý / cài đặt thêm Language Servers                   │
│ :Lazy                        │ Mở giao diện cập nhật plugins                                          │
└──────────────────────────────┴────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 7: BỐ TRÍ LAYOUT 4 PANES HOÀN CHỈNH TRÊN TERMINATOR

Khi mở Terminator trong i3, chia 4 panes như sau để có trải nghiệm làm việc đỉnh cao:

```
┌──────────────────────────────────────────┬──────────────────────────────────────────┐
│ PANE 1 (Top-Left): LAZYVIM IDE           │ PANE 2 (Top-Right): HARLEQUIN SQL TUI    │
│                                          │                                          │
│  nvim src/jobs/scd2_customer_job.py      │  harlequin -a postgres \                 │
│  (Code PySpark, auto-complete hàm,       │    --host localhost -U fleet_app         │
│   nhảy định nghĩa, format on save)       │  (Truy vấn SQL, xem schema, soi data)    │
├──────────────────────────────────────────┼──────────────────────────────────────────┤
│ PANE 3 (Bottom-Left): MAKE & TEST        │ PANE 4 (Bottom-Right): K8S / DOCKER OPS  │
│                                          │                                          │
│  make test                               │  k9s                                     │
│  make run-scd2 env=local                 │  hoặc:                                   │
│  make config env=prod                    │  docker compose ps                       │
│  git status                              │  kubectl logs -f job/scd2-job            │
└──────────────────────────────────────────┴──────────────────────────────────────────┘
```

### Quy trình thao tác:
1. Viết code PySpark tại **Pane 1** (Neovim tự gợi ý code, lưu file tự format).
2. Chạy thử query SQL kiểm tra dữ liệu nguồn/đích tại **Pane 2** (Harlequin).
3. Chạy `make test` hoặc `make run-scd2` tại **Pane 3**.
4. Theo dõi tài nguyên, container và logs tại **Pane 4** (k9s / docker).
