# P15 — CẨM NANG 20 TÌNH HUỐNG THỰC CHIẾN: QUẢN TRỊ SOURCE CODE & PHỐI HỢP LIÊN PHÒNG BAN
## Đóng Gói Toàn Diện: 10 Tình Huống Kỷ Luật Git/CI/CD & 10 Tình Huống Đàm Phán, Giải Quyết Xung Đột Nhóm (Chuẩn Senior/Lead Data Engineer)

> **Tôn Chỉ Khi Xử Lý Con Người & Mã Nguồn Cấp Senior:**
> *"Mã nguồn phản ánh kỷ luật kỹ thuật (Engineering Discipline). Cách làm việc nhóm phản ánh tư duy kiến trúc và bản lĩnh lãnh đạo (Leadership & Collaboration). 
> Một kỹ sư giỏi không chỉ biết viết code chạy được, mà phải biết bảo vệ hệ thống trước sự cố rò rỉ bảo mật, quản trị release không gián đoạn, và dẫn dắt các bên liên quan (DBA, BI, Security, PO) cùng đạt được mục tiêu chung."*

---

# MỤC LỤC 20 KỊCH BẢN TÌNH HUỐNG THỰC TẾ

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PHẦN 1: 10 TÌNH HUỐNG QUẢN TRỊ SOURCE CODE, GIT FLOW & CI/CD NATIVE                                    │
│   • Tình huống 1: Lỡ commit mật khẩu Database / API Token vào Git history                              │
│   • Tình huống 2: Production Hotfix khi nhánh `develop` đang dở dang Sprint                            │
│   • Tình huống 3: Giải quyết Git Merge Conflict phức tạp trên DAG Airflow & DDL SQL                    │
│   • Tình huống 4: Quản lý phiên bản Schema Database Migration không gây Downtime                       │
│   • Tình huống 5: Junior commit nhầm file Parquet/CSV 1GB làm phình to Git repository                  │
│   • Tình huống 6: Bài test PySpark Unit Test bị chập chờn (Flaky Test) làm nghẽn CI/CD                 │
│   • Tình huống 7: Thành viên cố tình dùng `git commit --no-verify` để bypass Pre-commit                │
│   • Tình huống 8: Quản trị thư viện dùng chung (Common Utilities) cho nhiều Spark Jobs & DAGs          │
│   • Tình huống 9: Triển khai Canary / Zero-downtime cho Spark Streaming & Kafka Connect               │
│   • Tình huống 10: Quy chuẩn Git Rebase vs Git Merge để giữ lịch sử Commit sạch sẽ                     │
├────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ PHẦN 2: 10 TÌNH HUỐNG LÀM VIỆC NHÓM, ĐÀM PHÁN & GIẢI QUYẾT XUNG ĐỘT LIÊN PHÒNG BAN                     │
│   • Tình huống 11: Xung đột với DBA/ERP về Replication Slot & nguy cơ nổ đĩa WAL                       │
│   • Tình huống 12: Xung đột với BI Lead: "Số liệu Dashboard lệch so với hóa đơn thực tế!"             │
│   • Tình huống 13: Xung đột với Product Owner (PO) đòi nhét tính năng mới khẩn cấp giữa Sprint         │
│   • Tình huống 14: Xung đột với Phòng An Ninh Mạng (ATTT/Infra) bị chặn quyền Sudo / Mở Port           │
│   • Tình huống 15: Team Odoo âm thầm đổi Schema bảng (Schema Drift) làm gãy ETL lúc 2h sáng            │
│   • Tình huống 16: Thành viên trong team từ chối viết Unit Test & Documentation vì "chậm tiến độ"      │
│   • Tình huống 17: Điều phối buổi họp Hậu Sự Cố Không Chỉ Trích (Blameless Post-Mortem)               │
│   • Tình huống 18: Đàm phán khi Ban Giám Đốc ép tiến độ (Yêu cầu 2 tháng phải xong trong 3 tuần)       │
│   • Tình huống 19: Bàn giao & Chống thất thoát tri thức khi thành viên chủ chốt nghỉ việc              │
│   • Tình huống 20: Cân đối giữa Trả Nợ Kỹ Thuật (Tech Debt Refactoring) và Ra Tính Năng Mới            │
└────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

# PHẦN 1: 10 TÌNH HUỐNG QUẢN TRỊ SOURCE CODE, GIT FLOW & CI/CD NATIVE

---

### 🚨 TÌNH HUỐNG 1: LỠ COMMIT MẬT KHẨU DATABASE / API TOKEN VÀO GIT
* **Bối cảnh**: Một dev vừa commit và push lên nhánh `feature/cdc-ingest` có chứa file config chứa mật khẩu Postgres thật `SecurePass_VCS_2026`.
* **Câu hỏi phỏng vấn**: *"Nếu phát hiện thành viên lỡ push thông tin bảo mật lên Git, em xử lý các bước như thế nào?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Bước 1 (Xử lý Bảo mật Tức thì - Containment)**: Coi như mật khẩu đó **ĐÃ BỊ LỘ $100\%$**. Lập tức truy cập Postgres đổi mật khẩu user đó ngay lập tức (`ALTER USER fleet_cdc WITH PASSWORD 'NewRandomPass_...'`), không được phép chỉ xóa code mà không đổi pass.
  2. **Bước 2 (Dọn dẹp lịch sử Git)**:
     * Tuyệt đối không làm: Tạo commit mới để xóa password (vì password vẫn còn nguyên trong git commit history cũ!).
     * Dùng công cụ **`git-filter-repo`** hoặc **`BFG Repo-Cleaner`** để xóa vĩnh viễn commit chứa chuỗi bí mật khỏi toàn bộ nhánh và tags.
     ```bash
     # Xóa chuỗi mật khẩu khỏi toàn bộ commit history
     git-filter-repo --replace-text <(echo 'SecurePass_VCS_2026==>REDACTED_SECRET')
     git push origin --force --all
     ```
  3. **Bước 3 (Chốt chặn phòng ngừa vĩnh viễn)**: Kích hoạt Pre-commit hook với **`gitleaks`** hoặc **`trufflehog`** trên máy local của dev và trên GitLab CI pipeline. Nếu commit có chứa chuỗi regex dạng API key / Password thì Git từ chối commit ngay từ máy cá nhân.
* **Kịch bản nói mẫu**:
  > *"Thưa anh, nguyên tắc an ninh đầu tiên tại VCS là: Secret một khi đã lên Git thì coi như đã bị lộ. Em xử lý 3 bước: Đầu tiên đổi ngay mật khẩu trên hệ thống thật; sau đó dùng `git-filter-repo` viết lại lịch sử commit để thanh tẩy Git history; và cuối cùng bắt buộc toàn bộ team cài `gitleaks` pre-commit hook để chặn đứng việc này từ máy local."*

---

### 🚨 TÌNH HUỐNG 2: PRODUCTION HOTFIX KHI NHÁNH `DEVELOP` ĐANG DỞ DANG SPRINT
* **Bối cảnh**: Hệ thống DWH Production bị lỗi tính sai doanh thu do lỗi chia cho 0. Trong khi đó nhánh `develop` đang chứa 10 tính năng mới của Sprint đang test dở, chưa thể release toàn bộ.
* **Câu hỏi phỏng vấn**: *"Làm thế nào để release bản vá lỗi lên Production ngay lập tức mà không kéo theo các tính năng dở dang của Sprint?"*
* **Quy trình xử lý chuẩn Senior**:
  1. Từ nhánh `main` (Production đang chạy), tạo một nhánh hotfix riêng:
     ```bash
     git checkout main
     git pull origin main
     git checkout -b hotfix/fix-zero-division-revenue
     ```
  2. Sửa lỗi, viết unit test bổ sung kiểm tra case chia cho 0 (`F.when(col("total") == 0, 0).otherwise(...)`).
  3. Tạo Pull Request vào `main` $\to$ Chạy CI/CD $\to$ Review và Merge vào `main` $\to$ Đóng Tag phiên bản `v1.2.1` $\to$ Deploy lên Production.
  4. **Bước quan trọng nhất (Tránh mất bản vá)**: Merge ngược nhánh `hotfix/fix-zero-division-revenue` vào nhánh `develop` để Sprint hiện tại và các release tương lai không bị tái phát lỗi này.
     ```bash
     git checkout develop
     git pull origin develop
     git merge hotfix/fix-zero-division-revenue
     git push origin develop
     ```

---

### 🚨 TÌNH HUỐNG 3: GIẢI QUYẾT GIT MERGE CONFLICT PHỨC TẠP TRÊN AIRFLOW DAG & SQL DWH
* **Bối cảnh**: Hai kỹ sư cùng sửa file `dags/daily_dwh_fact_pipeline.py` và file DDL `dim_customer.sql`. Khi tạo PR vào `develop`, Git báo conflict 50 dòng code.
* **Câu hỏi phỏng vấn**: *"Em quy định quy trình giải quyết Merge Conflict trong team như thế nào để không làm mất logic của nhau?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Nguyên tắc**: Không bao giờ đơn phương giải quyết conflict trên Web UI của GitHub/GitLab.
  2. Người tạo PR sau bắt buộc phải kéo nhánh `develop` mới nhất về máy local của mình và thực hiện `rebase` hoặc `merge`:
     ```bash
     git checkout feature/scd2-customer
     git fetch origin develop
     git merge origin/develop
     ```
  3. **Họp trực tiếp 1-1 (Pairing 5 phút)** với kỹ sư đã viết đoạn code xung đột để phân tích:
     * Dòng nào của feature A cần giữ?
     * Dòng nào của feature B cần giữ?
  4. Chạy bộ kiểm thử tự động tại local: `pytest tests/test_dags.py` và `sqlfluff lint sql/`.
  5. Khi toàn bộ test pass thì mới push lại lên nhánh PR.

---

### 🚨 TÌNH HUỐNG 4: QUẢN LÝ PHIÊN BẢN SCHEMA DATABASE MIGRATION KHÔNG GÂY DOWNTIME
* **Bối cảnh**: Cần thêm cột `driver_tier` vào bảng Production `fact_parts_sales` (50 triệu dòng) và sửa kiểu dữ liệu cột `note`.
* **Câu hỏi phỏng vấn**: *"Làm sao quản lý migration script trong Git và chạy trên Production mà không làm khóa bảng hay gián đoạn hệ thống?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Quản lý Migration bằng Code**: Sử dụng công cụ Database Version Control như **Flyway** hoặc **Alembic**. Mỗi migration là 1 file SQL có thứ tự bất biến: `V1.2__add_driver_tier_to_fact.sql`. Cấm tuyệt đối việc gõ SQL bằng tay trên Production.
  2. **Quy tắc Expand-Contract (Zero-Downtime Migration)**:
     * *Pha 1 (Expand)*: Thêm cột mới nullable (`ALTER TABLE ... ADD COLUMN driver_tier VARCHAR(50) DEFAULT NULL`). Tuyệt đối không đặt `NOT NULL DEFAULT '...'` trên Postgres cũ vì sẽ gây full table rewrite và khóa bảng `ACCESS EXCLUSIVE`.
     * *Pha 2 (Deploy Code)*: Deploy code mới bắt đầu ghi dữ liệu vào cả cột mới.
     * *Pha 3 (Backfill)*: Chạy batch script cập nhật dữ liệu lịch sử cho cột mới theo từng chunk 10.000 dòng để không giữ lock lâu.
     * *Pha 4 (Contract)*: Thêm ràng buộc `NOT NULL` (nếu cần) qua `ALTER TABLE ... VALIDATE CONSTRAINT`.

---

### 🚨 TÌNH HUỐNG 5: JUNIOR COMMIT NHẦM FILE PARQUET/CSV 1GB LÀM PHÌNH TO GIT REPO
* **Bối cảnh**: Một bạn dev chạy test local sinh ra thư mục output `data/output.parquet` dung lượng 1.2GB và lỡ `git add .` rồi push lên repo công ty. Repo bị phình to khủng khiếp, mỗi lần `git clone` mất 30 phút.
* **Câu hỏi phỏng vấn**: *"Em xử lý việc này thế nào để thu nhỏ repo và phòng ngừa tái diễn?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Dọn dẹp Git Tree**: Dùng `git-filter-repo --path data/output.parquet --invert-paths` để xóa hoàn toàn metadata của file 1.2GB khỏi mọi commit. Chạy `git gc --prune=now --aggressive` để thu hồi dung lượng đĩa.
  2. **Cập nhật `.gitignore` chuẩn Data Engineering**:
     ```gitignore
     # Data files
     *.parquet
     *.csv
     *.orc
     *.avro
     *.tar.gz
     /data/
     /tmp/
     .checkpoint/
     ```
  3. **Thiết lập Giới Hạn Dung Lượng Trên Server Git**: Cấu hình trên GitLab/GitHub Server: **Push Rule / Pre-receive hook** từ chối bất kỳ commit nào chứa file đơn lẻ $> 50\text{MB}$. Nếu thực sự cần lưu file mẫu, bắt buộc dùng **Git LFS (Large File Storage)** hoặc lưu trên MinIO/HDFS.

---

### 🚨 TÌNH HUỐNG 6: BÀI TEST PYSPARK UNIT TEST BỊ CHẬP CHỜN (FLAKY TEST) TRÊN CI/CD
* **Bối cảnh**: Pipeline CI/CD trên GitLab thỉnh thoảng bị Fail ngẫu nhiên ở bước test PySpark do lỗi timeout khởi tạo local SparkContext hoặc do thứ tự dòng trong DataFrame không cố định (`assert df1.collect() == df2.collect()`).
* **Câu hỏi phỏng vấn**: *"Làm thế nào để trị dứt điểm Flaky Test trong các dự án Big Data?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Khởi tạo SparkSession dùng chung (Pytest Fixture với Scope Session)**:
     ```python
     # conftest.py
     import pytest
     from pyspark.sql import SparkSession

     @pytest.fixture(scope="session")
     def spark():
         return SparkSession.builder \
             .master("local[2]") \
             .appName("Pytest-SparkSession") \
             .config("spark.ui.enabled", "false") \
             .config("spark.sql.shuffle.partitions", "2") \
             .getOrCreate()
     ```
     *(Tránh việc mỗi file test tự khởi động và tắt SparkContext làm tràn bộ nhớ JVM).*
  2. **Sửa lỗi Assert thứ tự ngẫu nhiên**: Dữ liệu phân tán trong Spark không đảm bảo thứ tự dòng. Tuyệt đối không dùng `collect() == collect()`. Thay bằng hàm so sánh DataFrame độc lập với thứ tự:
     ```python
     from chispa import assert_df_equality
     # So sánh cấu trúc và dữ liệu, bỏ qua thứ tự dòng
     assert_df_equality(df_actual, df_expected, ignore_row_order=True)
     ```

---

### 🚨 TÌNH HUỐNG 7: DEV CỐ TÌNH DÙNG `git commit --no-verify` ĐỂ BYPASS LINT
* **Bối cảnh**: Một số lập trình viên vì muốn commit nhanh nên gõ `git commit -m "fix" --no-verify`, bỏ qua bước quét linter Flake8 và Sqlfluff. Kết quả là code đẩy lên `develop` bị lỗi format, biến đặt tên lộn xộn.
* **Câu hỏi phỏng vấn**: *"Em làm sao để ngăn chặn việc bypass pre-commit hook của các thành viên?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Hiểu bản chất**: Pre-commit hook ở máy local của lập trình viên là **phòng tuyến Client-side** (có thể bị bypass).
  2. **Thiết lập Phòng Tuyến Bắt Buộc Server-Side (CI/CD Quality Gate)**:
     * Trên GitLab CI, bước đầu tiên của mọi Merge Request là chạy:
       ```yaml
       stages:
         - lint_and_security
       
       lint_check:
         stage: lint_and_security
         script:
           - flake8 src/
           - sqlfluff lint sql/
           - gitleaks detect --verbose
       ```
     * Nếu bước này Fail, GitLab **KHÓA NÚT MERGE** (Block PR). Lập trình viên bắt buộc phải sửa code cho chuẩn format thì mới được review.
  3. **Văn hóa**: Đưa việc tuân thủ code style vào tiêu chí Definition of Done (DoD) của Sprint.

---

### 🚨 TÌNH HUỐNG 8: QUẢN TRỊ THƯ VIỆN DÙNG CHUNG (COMMON UTILITIES) CHO SPARK VÀ AIRFLOW
* **Bối cảnh**: Dự án có nhiều hàm dùng chung: Hàm tính Năm tài khóa, hàm parse JSON CDC, hàm sinh Surrogate Key MD5. Nếu copy-paste vào từng file DAG/Spark job thì khi sửa logic sẽ bị sai lệch.
* **Câu hỏi phỏng vấn**: *"Em đóng gói và quản lý mã nguồn dùng chung như thế nào trong hệ thống Data Platform?"*
* **Quy trình xử lý chuẩn Senior**:
  1. Đóng gói các hàm tiện ích thành một **Python Package nội bộ** (Ví dụ: `fleet_common`):
     ```text
     fleet-common/
     ├── pyproject.toml
     ├── setup.py
     └── fleet_common/
         ├── __init__.py
         ├── date_utils.py       # fiscal_year_calculator
         ├── hashing.py          # md5_surrogate_key
         └── data_quality.py     # schema_validators
     ```
  2. Trong CI/CD, tự động build thành file `.whl` (Wheel) hoặc `.zip`.
  3. Khi nộp Spark job: Truyền file thư viện qua cờ `--py-files /opt/spark/libs/fleet_common.whl`.
  4. Đối với Airflow: Cài đặt package vào Virtualenv của Airflow (`pip install /opt/airflow/libs/fleet_common.whl`).

---

### 🚨 TÌNH HUỐNG 9: ZERO-DOWNTIME DEPLOYMENT CHO KAFKA CONNECT & SPARK STREAMING
* **Bối cảnh**: Cần cập nhật logic transform cho Spark Streaming và thêm bảng mới vào Debezium Connector đang chạy 24/7 trên Production.
* **Câu hỏi phỏng vấn**: *"Làm thế nào để deploy phiên bản mới cho các ứng dụng Streaming mà không bị mất mát hay trùng lặp dữ liệu?"*
* **Quy trình xử lý chuẩn Senior**:
  1. **Đối với Debezium Kafka Connect**:
     * Sử dụng REST API nạp config cập nhật (`PUT /connectors/fleet-postgres-cdc/config`).
     * Debezium tự động pause task, reload config thêm bảng mới, và tiếp tục đọc WAL tại đúng vị trí LSN hiện tại mà không làm rớt kết nối Kafka.
  2. **Đối với Spark Structured Streaming**:
     * Tận dụng cơ chế **Checkpointing trên HDFS** (`checkpointLocation`):
     * *Bước 1*: Gửi tín hiệu dừng an toàn (Graceful Shutdown): `spark.streams.active[0].stop()`.
     * *Bước 2*: Deploy file Python code mới lên máy chủ.
     * *Bước 3*: Khởi động lại Spark job trỏ vào **cùng đường dẫn checkpoint cũ**.
     * Spark sẽ đọc metadata checkpoint, phục hồi đúng Kafka Offsets và Watermark State để chạy tiếp $\implies$ Đảm bảo ngữ nghĩa **Exactly-Once / At-Least-Once** mà không mất tin nhắn.

---

### 🚨 TÌNH HUỐNG 10: QUY CHUẨN GIT REBASE VS GIT MERGE
* **Bối cảnh**: Một số kỹ sư thích dùng `git merge` tạo ra hàng chục commit rác `Merge branch 'develop' into feature` làm đồ thị Git rối như mạng nhện. Một số khác lại lạm dụng `git rebase` trên các nhánh dùng chung làm mất lịch sử commit của người khác.
* **Câu hỏi phỏng vấn**: *"Em quy định tiêu chuẩn sử dụng Git Merge và Git Rebase trong dự án như thế nào?"*
* **Quy trình xử lý chuẩn Senior**:
  ```
  ┌──────────────────────────────────────────────────────────────────────────────────────────────────────┐
  │ QUY TẮC VÀNG VỀ GIT TRONG TEAM DATA PLATFORM:                                                        │
  ├────────────────────────────┬─────────────────────────────────────────────────────────────────────────┤
  │ 1. Trên nhánh Local        │ Dùng `git pull --rebase origin develop` để cập nhật code mới nhất từ    │
  │    (Private Feature Branch)│ develop vào nhánh của mình mà không tạo commit merge rác.               │
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 2. Khi tạo Pull Request    │ Áp dụng **Squash and Merge**: Gộp 15 commits vụn vặt trong lúc dev thành│
  │    vào `develop` / `main`  │ đúng 1 Commit duy nhất có gắn mã Jira: `[FLEET-102] Feat: Add SCD2 Merge`│
  ├────────────────────────────┼─────────────────────────────────────────────────────────────────────────┤
  │ 3. Tuyệt đối CẤM (Rule)    │ **CẤM `git push --force` trên các nhánh công cộng (`develop`, `main`)**. │
  └────────────────────────────┴─────────────────────────────────────────────────────────────────────────┘
  ```

---

# PHẦN 2: 10 TÌNH HUỐNG LÀM VIỆC NHÓM, ĐÀM PHÁN & GIẢI QUYẾT XUNG ĐỘT LIÊN PHÒNG BAN

---

### 🤝 TÌNH HUỐNG 11: XUNG ĐỘT VỚI DBA/ERP VỀ REPLICATION SLOT & NGUY CƠ NỔ ĐĨA WAL
* **Bối cảnh**: Senior DBA của Odoo từ chối cấp quyền CDC và dọa tắt Replication Slot vì sợ: *"Debezium mà bị treo thì WAL giữ lại sẽ ghi đầy 100% ổ cứng máy chủ ERP làm sập toàn bộ công ty!"*
* **Câu hỏi phỏng vấn**: *"Em làm việc và đàm phán với DBA như thế nào để họ yên tâm cấp quyền triển khai CDC?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Đồng cảm và thừa nhận rủi ro**: Chấp nhận nỗi lo của DBA là hoàn toàn có cơ sở kỹ thuật chính xác.
  2. **Đưa ra giải pháp kỹ thuật bảo vệ DBA 100% (Chốt an toàn Failsafe)**:
     * Cùng DBA cấu hình tham số: **`max_slot_wal_keep_size = 10240MB` (10GB)** trong `postgresql.conf`.
     * Giải thích: Nếu Debezium bị sập quá lâu, Postgres sẽ **chủ động hủy slot và xóa WAL cũ để bảo vệ đĩa máy chủ ERP không bao giờ bị đầy**.
  3. **Cam kết trách nhiệm phía Data Team**: Viết script Prometheus/Grafana giám sát `pg_wal_lsn_diff()` cảnh báo qua Telegram/Slack ngay khi slot lag vượt quá 2GB. Nếu slot bị drop, Data team tự kích hoạt Signal Incremental Snapshot để bù dữ liệu mà không làm phiền DBA.
* **Kịch bản nói mẫu**:
  > *"Em không tranh cãi suông với DBA mà cùng họ cấu hình `max_slot_wal_keep_size = 10GB` để chốt an toàn bảo vệ đĩa ERP tuyệt đối. Em cam kết với DBA rằng nếu sự cố xảy ra, Postgres được quyền ưu tiên sống còn, phía Data Team em đã có cơ chế Incremental Snapshot tự động bù dữ liệu."*

---

### 🤝 TÌNH HUỐNG 12: XUNG ĐỘT VỚI BI LEAD: "SỐ LIỆU DASHBOARD LỆCH SO VỚI HÓA ĐƠN TRÊN ODOO!"
* **Bối cảnh**: Trưởng nhóm BI báo cáo lên Ban Giám Đốc rằng số liệu doanh thu Quý 1 trên Dashboard DWH lệch 50 triệu so với báo cáo Odoo. BI cho rằng Data Pipeline bị mất dữ liệu.
* **Câu hỏi phỏng vấn**: *"Khi có phản ánh số liệu lệch giữa DWH và hệ thống nguồn, em điều tra và xử lý thế nào?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Bước 1 (Giữ bình tĩnh & Lập bảng đối soát Reconciliation)**: Không vội phủ nhận. Viết script SQL đối soát song song giữa bảng nguồn Odoo và bảng Gold DWH theo từng trạm và từng ngày:
     ```sql
     -- Query đối soát tìm chính xác ngày và trạm bị lệch
     SELECT 
         o.invoice_date::DATE,
         o.head_id,
         o.odoo_revenue,
         d.dwh_revenue,
         (o.odoo_revenue - d.dwh_revenue) AS diff
     FROM odoo_daily_summary o
     FULL OUTER JOIN dwh_daily_summary d 
       ON o.invoice_date = d.report_date AND o.head_id = d.head_id
     WHERE o.odoo_revenue != d.dwh_revenue;
     ```
  2. **Bước 2 (Tìm nguyên nhân gốc rễ)**: Thường do 3 nguyên nhân:
     * *Nguyên nhân 1 (Timezone Mismatch)*: Odoo lưu UTC nhưng BI xem theo GMT+7 khiến các hóa đơn xuất lúc 23:30 bị nhảy sang ngày hôm sau.
     * *Nguyên nhân 2 (Late-arriving updates)*: Kế toán sửa hóa đơn cũ trong Odoo nhưng BI query dữ liệu chưa refresh.
     * *Nguyên nhân 3 (Logic Năm Tài Khóa)*: Odoo tính theo năm dương lịch, DWH tính theo Fiscal Year (01/10).
  3. **Bước 3 (Thống nhất & Minh bạch hóa)**: Xuất báo cáo giải trình chi tiết từng hóa đơn chênh lệch, gắn nhãn `Data Freshness: Last synced at HH:mm` trên Dashboard PowerBI.

---

### 🤝 TÌNH HUỐNG 13: XUNG ĐỘT VỚI PRODUCT OWNER (PO) ĐÒI NHÉT TÍNH NĂNG MỚI GIỮA SPRINT
* **Bối cảnh**: Sprint 2 tuần đang chạy được 5 ngày. PO yêu cầu team Data thêm ngay một pipeline phân tích lưu lượng xe tải theo giờ để kịp trình bày với Tổng Giám Đốc vào thứ Sáu.
* **Câu hỏi phỏng vấn**: *"Làm thế nào để từ chối hoặc đàm phán với PO mà không gây mất lòng nhưng vẫn bảo vệ được cam kết Sprint?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Lắng nghe mục tiêu kinh doanh**: Hiểu rõ tại sao PO cần gấp (để trình bày sếp lớn).
  2. **Áp dụng nguyên tắc Quản trị Agile (Trade-off Scope)**:
     * *Phương án 1 (Swap Ticket)*: Nếu bắt buộc làm tính năng này ngay trong Sprint, PO phải đồng ý **bỏ bớt 1 task có Story Points tương đương** ra khỏi Sprint Backlog và dời sang Sprint sau.
     * *Phương án 2 (Cung cấp MVP / Quick Spike)*: Thay vì xây dựng full pipeline tự động từ CDC đến DWH (tốn 1 tuần), Data Engineer hỗ trợ xuất một file báo cáo Ad-hoc nhanh hoặc tạo một Materialized View tạm thời để PO có số liệu trình bày trước, sau đó đưa bài toán xây dựng pipeline chuẩn vào Sprint tiếp theo.
* **Kịch bản nói mẫu**:
  > *"Em luôn đứng trên góc độ hỗ trợ kinh doanh nhưng giữ vững kỷ luật Agile. Em giải thích cho PO về năng lực hữu hạn của Sprint: Nếu đưa việc mới vào thì phải rút việc cũ ra để tránh nợ kỹ thuật (Tech Debt). Nếu việc quá gấp, em sẽ tạo một bản báo cáo Ad-hoc nhanh để giải quyết nhu cầu trước mắt của PO, sau đó lên kế hoạch triển khai kiến trúc chuẩn vào Sprint kế tiếp."*

---

### 🤝 TÌNH HUỐNG 14: XUNG ĐỘT VỚI PHÒNG AN NINH MẠNG (ATTT/INFRA) BỊ CHẶN QUYỀN VÀ CỔNG MẠNG
* **Bối cảnh**: Đội ngũ ATTT/Security chặn toàn bộ port 9092 của Kafka, port 8083 của Debezium và từ chối cấp quyền `root/sudo` trên các máy chủ Bare-metal Ubuntu.
* **Câu hỏi phỏng vấn**: *"Làm sao để phối hợp với team Bảo Mật để hệ thống vận hành trơn tru mà vẫn tuân thủ 100% chính sách An toàn Thông tin của Viettel?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Hiểu tư duy của Security**: Nhiệm vụ của ATTT là giảm thiểu bề mặt tấn công (Attack Surface). Không bao giờ đòi hỏi mở port bừa bãi hay cấp quyền root vô tội vạ.
  2. **Soạn thảo Tài liệu Kiến trúc Mạng & Phân Vùng (Network Architecture Document)**:
     * Vẽ rõ ma trận luồng dữ liệu (Data Flow Matrix): IP nguồn, IP đích, Cổng dịch vụ, Mục đích sử dụng.
     * Đề xuất gom các máy chủ dữ liệu vào **VLAN Nội Bộ Cô Lập (Internal Data Subnet)**: Chỉ cho phép các máy trong dải `192.168.1.0/24` giao tiếp nội bộ với nhau qua port 9092/8083, chặn $100\%$ truy cập từ dải mạng ngoài.
  3. **Tuân thủ Cơ Chế Phân Quyền Không Cần Sudo**:
     * Đóng gói dịch vụ chạy dưới user dịch vụ riêng biệt (`aiguystory` hoặc `hadoop`), cấp quyền thư mục chuẩn `chown -R`, quản lý tự động qua Systemd service.

---

### 🤝 TÌNH HUỐNG 15: TEAM ODOO ÂM THẦM ĐỔI SCHEMA BẢNG LÀM GÃY ETL LÚC 2H SÁNG
* **Bối cảnh**: Team ERP đổi tên cột `amount_total` thành `total_amount_vnd` trên Odoo mà không báo trước $\to$ Spark Batch Job đêm bị lỗi không tìm thấy cột, gãy pipeline.
* **Câu hỏi phỏng vấn**: *"Em thiết lập cơ chế gì để giải quyết vấn đề Schema Drift giữa các team phát triển?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Khắc phục sự cố trong đêm (Immediate Fix)**: Sửa ánh xạ schema tạm thời trong Spark job, rerun backfill dữ liệu đêm để phục vụ báo cáo 8h sáng.
  2. **Thiết lập Hợp Đồng Dữ Liệu (Data Contract)**:
     * Họp với Tech Lead phía ERP Odoo để ký kết **Data Contract Agreement**: Bất kỳ thay đổi nào liên quan đến Schema của các bảng Master Data/Transactional Data phải được thông báo trước ít nhất **1 Sprint (2 tuần)**.
  3. **Áp dụng Tự Động Hóa Kỹ Thuật (Schema Registry & CI/CD Validation)**:
     * Sử dụng **Schema Registry** với chế độ `BACKWARD_TRANSITIVE`.
     * Tích hợp bước kiểm tra Schema Compatibility vào chính pipeline CI/CD của team Odoo. Nếu code Odoo sửa cột mà làm gãy tương thích của Data Platform $\implies$ CI/CD của Odoo tự động báo đỏ và chặn merge!

---

### 🤝 TÌNH HUỐNG 16: THÀNH VIÊN TRONG TEAM TỪ CHỐI VIẾT UNIT TEST VÌ "CHẬM TIẾN ĐỘ"
* **Bối cảnh**: Một đồng nghiệp Senior trong team Data có thói quen viết code thẳng lên server test, không viết unit test và nói rằng: *"Viết test mất thời gian, việc nhiều thế này test bằng tay cho nhanh"*.
* **Câu hỏi phỏng vấn**: *"Với vai trò là Lead/Senior trong team, em làm thế nào để nâng cao kỷ luật kỹ thuật của đồng nghiệp?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Không áp đặt quyền lực, hãy chỉ ra lợi ích thực tế**: Chia sẻ trải nghiệm về những lần pipeline bị gãy lúc nửa đêm và mất cả ngày để debug thủ công $\implies$ Viết test tốn 30 phút ban đầu nhưng tiết kiệm hàng chục giờ bảo trì sau này.
  2. **Xây dựng khung mẫu Test sẵn có (Test Templates & Helpers)**:
     * Viết sẵn `conftest.py`, các fixture giả lập SparkSession và dữ liệu mẫu (Mock DataFrame). Đồng nghiệp chỉ cần viết 5 dòng code là xong 1 bài test hoàn chỉnh.
  3. **Đưa Test Coverage vào Quy chuẩn Bắt Buộc (Policy Enforcement)**:
     * Quy định trong team: Mọi Pull Request phải có Code Coverage $\ge 80\%$ thì mới được duyệt. Cài đặt SonarQube tự động chấm điểm trên GitLab CI.

---

### 🤝 TÌNH HUỐNG 17: ĐIỀU PHỐI BUỔI HỌP HẬU SỰ CỐ KHÔNG CHỈ TRÍCH (BLAMELESS POST-MORTEM)
* **Bối cảnh**: Một kỹ sư mới vô tình chạy nhầm script làm ghi đè mất phân vùng dữ liệu ngày hôm trước trên HDFS Gold DWH, khiến toàn bộ Dashboard sáng thứ Hai bị trống số liệu.
* **Câu hỏi phỏng vấn**: *"Em tổ chức buổi họp Post-Mortem xử lý sự cố này như thế nào để vừa tìm ra nguyên nhân gốc rễ vừa giữ được tinh thần đoàn kết?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Tôn chỉ Blameless (Không đổ lỗi cá nhân)**:
     * *"Nếu một con người có thể bấm nhầm làm hỏng hệ thống, đó là **LỖI CỦA HỆ THỐNG THIẾU CƠ CHẾ AN TOÀN**, không phải lỗi của cá nhân đó."*
  2. **Cấu trúc biên bản Post-Mortem 5 phần**:
     * *Timeline*: Chi tiết diễn biến sự cố theo từng phút.
     * *Impact*: Ảnh hưởng bao nhiêu người dùng, thời gian gián đoạn bao lâu.
     * *Root Cause Analysis (5 Whys)*: Tại sao dev chạy nhầm? $\to$ Vì file config môi trường Staging và Production đặt tên giống nhau.
     * *Recovery*: Đã phục hồi dữ liệu từ bản sao lưu Bronze Parquet như thế nào.
     * *Action Items (Hành động sửa lỗi)*: Phân quyền lại quyền ghi trên HDFS Production (chỉ cấp cho service user, tài khoản cá nhân chỉ có quyền Read-only); tách biệt hoàn toàn credential giữa Dev và Prod.

---

### 🤝 TÌNH HUỐNG 18: ĐÀM PHÁN KHI BAN GIÁM ĐỐC ÉP TIẾN ĐỘ (YÊU CẦU 2 THÁNG XONG TRONG 3 TUẦN)
* **Bối cảnh**: Ban Giám Đốc muốn đẩy nhanh tiến độ bàn giao toàn bộ Data Platform 50 trạm từ 2 tháng xuống còn 3 tuần để phục vụ đợt thanh tra chiến lược.
* **Câu hỏi phỏng vấn**: *"Khi nhận được yêu cầu bất khả thi về mặt thời gian từ Sếp lớn, em ứng xử và đàm phán ra sao?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Không nói "Không" một cách cụt lủn, mà nói "Có, nhưng đi kèm phạm vi tương ứng (Yes, and...)"**:
  2. **Sử dụng Tam Giác Quản Trị Dự Án (Scope - Time - Quality)**:
     * Giải thích: Thời gian cố định (3 tuần), Chất lượng không được thỏa hiệp (độ chính xác tài chính $100\%$) $\implies$ **Phạm vi (Scope) bắt buộc phải thu gọn về bản MVP**.
  3. **Đề xuất Kế hoạch Phát hành 2 Giai đoạn (Phased Rollout Plan)**:
     * *Pha 1 (Đúng hạn 3 tuần - MVP)*: Triển khai trước cho **10 trạm trọng điểm** tại Hà Nội & TP.HCM, tập trung duy nhất vào 2 báo cáo cốt lõi: Doanh thu sửa chữa và Doanh thu phụ tùng.
     * *Pha 2 (5 tuần tiếp theo)*: Mở rộng ra 40 trạm còn lại và bổ sung các tính năng nâng cao (Bảo trì dự đoán IoT, Phân tích giỏ hàng Apriori).

---

### 🤝 TÌNH HUỐNG 19: BÀN GIAO & CHỐNG THẤT THOÁT TRI THỨC KHI KEY MEMBER NGHỈ VIỆC
* **Bối cảnh**: Kỹ sư phụ trách toàn bộ pipeline Debezium CDC và Spark Streaming nộp đơn xin nghỉ việc. Dự án đối mặt nguy cơ không ai hiểu sâu về hệ thống để vận hành.
* **Câu hỏi phỏng vấn**: *"Làm sao để đảm bảo dự án không bị phụ thuộc vào một cá nhân duy nhất (Bus Factor > 1)?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Quy tắc "Code là tài liệu sống"**:
     * Mọi pipeline phải có file `README.md` chuẩn hóa: Kiến trúc, Biến môi trường, Cách chạy test local, Cách debug khi có sự cố.
     * Lưu trữ toàn bộ tài liệu vận hành (Runbook) trên Wiki/Confluence chung của công ty.
  2. **Chiến lược Shadowing & Pair Programming trong 2 tuần bàn giao**:
     * Cử một kỹ sư khác làm "cái bóng" (Shadow). Trong 2 tuần cuối, mọi sự cố phát sinh trên Production phải do kỹ sư ở lại trực tiếp thao tác dưới sự giám sát của kỹ sư sắp nghỉ việc.
  3. **Kiểm kê Toàn bộ Access & Credentials**: Rà soát và chuyển giao toàn bộ SSH keys, Token quản trị, tài khoản dịch vụ sang Vault của team trước ngày làm việc cuối cùng.

---

### 🤝 TÌNH HUỐNG 20: CÂN ĐỐI GIỮA TRẢ NỢ KỸ THUẬT (TECH DEBT) VÀ RA TÍNH NĂNG MỚI
* **Bối cảnh**: Sau 6 tháng phát triển nóng, hệ thống có nhiều đoạn code Spark viết vội chạy chậm, chưa có test đầy đủ. Nhưng Product Owner liên tục ép ra thêm các báo cáo mới cho phòng Kinh doanh.
* **Câu hỏi phỏng vấn**: *"Làm thế nào để thuyết phục PO và lãnh đạo dành thời gian dọn dẹp Tech Debt?"*
* **Cách giải quyết chuẩn Senior**:
  1. **Định lượng tác động của Nợ Kỹ Thuật ra tiền bạc và thời gian (Monetize Tech Debt)**:
     * Thay vì nói: *"Code này xấu quá em muốn refactor"*, hãy nói: *"Job Spark này đang tốn 45 phút và chiếm $80\%$ RAM cụm máy chủ. Nếu refactor bằng Salting, thời gian chạy giảm về 1.4 phút, tiết kiệm cho công ty $50\%$ chi phí nâng cấp phần cứng và đảm bảo báo cáo sáng không bao giờ bị trễ SLA"*.
  2. **Quy tắc Phân Bổ Ngân Sách 80/20 trong từng Sprint**:
     * Đàm phán với PO đưa vào quy chế hoạt động của team: **$80\%$ Story Points của Sprint dành cho Tính năng Nghiệp vụ mới (Business Features)**, và **$20\%$ Story Points cố định dành riêng cho Refactoring, Tối ưu hóa hiệu năng và Nâng cấp hạ tầng**.

---

### 🏆 TỔNG KẾT BẢN LĨNH 20 TÌNH HUỐNG:
Toàn bộ 20 tình huống trên đã chuyển hóa từ các lý thuyết trừu tượng thành **những hành động thực chiến, lệnh Git chính xác, quy trình xử lý khủng hoảng và nghệ thuật đàm phán liên phòng ban**. Nắm vững bộ cẩm nang này, bạn hoàn toàn tự tin thể hiện tầm vóc của một Senior / Lead Data Engineer xuất sắc tại Viettel Cyber Security!
