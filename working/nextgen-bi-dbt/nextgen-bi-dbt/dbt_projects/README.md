# Bộ dbt projects tách theo domain cho Lightdash

Thư mục này chứa các dbt project độc lập, tách theo từng domain để dễ quản trị và phân quyền theo project trong Lightdash:
- crm
- cx
- finance
- hr
- jira
- noc
- investment
- other

## Mục tiêu
Triển khai nhiều dbt project độc lập lên cùng một Lightdash instance, giúp:
- Phân quyền theo domain/project rõ ràng hơn.
- Quản lý vòng đời deploy độc lập giữa các domain.
- Tách phạm vi ownership theo team nghiệp vụ.

## Cấu trúc chính
- Mỗi domain có 1 dbt project riêng: `dbt_project.yml`, `profiles.yml`, `models/`, `macros/`...
- Docker deploy image nằm tại: `dbt_projects/Dockerfile`.
- Script deploy trong container: `dbt_projects/scripts/setup-lightdash-projects.sh`.
- Docker Compose riêng cho deploy dbt_projects: `dbt_projects/docker-compose.dbt-projects.yml`.
- Script deploy từ root repo:
  - `scripts/docker-dbt-projects-build.sh`
  - `scripts/docker-dbt-projects-deploy.sh`
  - `scripts/docker-dbt-projects-run.sh`
  - `scripts/docker-dbt-projects-quickstart.sh`

## Điều kiện cần
1. Lightdash đã chạy sẵn (ví dụ bằng `docker compose` ở root repo).
2. Có Personal Access Token của Lightdash.
3. Có file env dùng để deploy.
  - Với script ở root repo: khuyến nghị `.env.lightdash`.
  - Với Docker Compose trong `dbt_projects`: dùng file `dbt_projects/.env` (copy từ `dbt_projects/.env.example`).
4. Các biến tối thiểu cần có:
   - `LIGHTDASH_URL=http://localhost:8080`
   - `LIGHTDASH_API_TOKEN=<token_cua_ban>`
   - Các biến kết nối Trino `DBT_TRINO_*`.

Bạn có thể tham khảo mẫu biến tại `dbt_projects/.env.example`.

## Cách triển khai

### 1. Build Docker image
Chạy từ root repo:

```bash
./scripts/docker-dbt-projects-build.sh
```

Nếu muốn đặt tên image riêng:

```bash
./scripts/docker-dbt-projects-build.sh vcs-lightdash-dbt-projects:latest
```

### 2. Deploy toàn bộ domain projects lên một Lightdash
Chạy:

```bash
./scripts/docker-dbt-projects-run.sh ./.env.lightdash
```

Script sẽ:
1. Chạy container deploy.
2. Login vào Lightdash bằng `LIGHTDASH_API_TOKEN`.
3. Deploy lần lượt các project: `crm`, `cx`, `finance`, `hr`, `jira`, `noc`, `investment`, `other`.

### 3. Create hoặc Redeploy theo mode
Script mới hỗ trợ 2 mode:
- `create`: tạo project nếu chưa có rồi deploy.
- `redeploy`: deploy lại project đã tồn tại.

Lệnh tổng quát:

```bash
./scripts/docker-dbt-projects-deploy.sh [create|redeploy] [ENV_FILE] [IMAGE_NAME] [DOMAINS_CSV]
```

Ví dụ create toàn bộ domain:

```bash
./scripts/docker-dbt-projects-deploy.sh create ./.env.lightdash
```

Ví dụ redeploy một số domain:

```bash
./scripts/docker-dbt-projects-deploy.sh redeploy ./.env.lightdash vcs-lightdash-dbt-projects:latest crm,cx,finance
```

Ý nghĩa tham số:
1. `create|redeploy`: mode deploy.
2. `ENV_FILE`: file môi trường, mặc định `./.env.lightdash`.
3. `IMAGE_NAME`: tên image deploy, mặc định `vcs-lightdash-dbt-projects:latest`.
4. `DOMAINS_CSV`: danh sách domain cách nhau bởi dấu phẩy, để trống sẽ chạy toàn bộ domain mặc định.

### 4. Quickstart (build + deploy)
Chạy một lệnh duy nhất:

```bash
./scripts/docker-dbt-projects-quickstart.sh ./.env.lightdash
```

### 5. Chạy bằng Docker Compose
Build service deploy:

```bash
docker compose -f dbt_projects/docker-compose.dbt-projects.yml build
```

Chạy deploy mặc định:

```bash
docker compose -f dbt_projects/docker-compose.dbt-projects.yml run --rm dbt-projects
```

Chạy mode create qua Compose:

```bash
docker compose -f dbt_projects/docker-compose.dbt-projects.yml run --rm \
  -e DEPLOY_MODE=create \
  dbt-projects
```

Chạy redeploy cho domain cụ thể qua Compose:

```bash
docker compose -f dbt_projects/docker-compose.dbt-projects.yml run --rm \
  -e DEPLOY_MODE=redeploy \
  -e DEPLOY_DOMAINS=crm,cx \
  dbt-projects
```

## Tùy chỉnh
- Đổi thứ tự hoặc thêm/bớt domain: sửa mảng `DEFAULT_DOMAINS` trong `dbt_projects/scripts/setup-lightdash-projects.sh`.
- Đổi tên project hiển thị trên Lightdash: sửa tham số `--project-name` trong cùng script.
- Đổi schema/target theo môi trường: cập nhật `profiles.yml` của từng domain project.
- Chọn mode deploy từ môi trường:
  - `DEPLOY_MODE=create|redeploy`
  - `DEPLOY_DOMAINS=crm,cx,finance`

## Lưu ý quan trọng
- Dự án `nextgen_bi` được giữ nguyên, không bị thay đổi.
- Domain `investment` hiện đang là project khởi tạo với placeholder model để sẵn sàng mở rộng.
- Mode `create` dùng Lightdash CLI với cờ `--create` để tự tạo project nếu chưa tồn tại.
- Mode `redeploy` sẽ deploy trực tiếp vào project đã có theo `--project-name`.
