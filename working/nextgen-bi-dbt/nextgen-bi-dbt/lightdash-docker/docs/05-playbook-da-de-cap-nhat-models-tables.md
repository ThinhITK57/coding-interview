# Playbook DA/DE cập nhật models và tables

Tài liệu này dành cho DA/DE khi cần thêm bảng mới, cập nhật model dbt, và đồng bộ sang Lightdash.

## 1) Phạm vi áp dụng

1. Cập nhật DDL nguồn tại [../sqls/datalake-ddl.sql](../sqls/datalake-ddl.sql)
2. Cập nhật khai báo source/staging trong dbt project [../nextgen_bi](../nextgen_bi)
3. Kiểm tra chất lượng (parse/test cơ bản)
4. Đồng bộ metadata sang Lightdash

## 2) Quy ước hiện tại trong dự án

1. Source schema dùng file [../nextgen_bi/models/sources/bi_silver__sources.yml](../nextgen_bi/models/sources/bi_silver__sources.yml)
2. Staging tách theo domain trong [../nextgen_bi/models/staging](../nextgen_bi/models/staging)
3. Tên model staging theo mẫu `stg_bi_silver__<table_name>`

## 3) Quy trình chuẩn khi thêm table mới

1. Cập nhật DDL
 - Thêm `CREATE TABLE hive.bi_silver.<table_name>` vào [../sqls/datalake-ddl.sql](../sqls/datalake-ddl.sql)

2. Cập nhật dbt source
 - Thêm table vào [../nextgen_bi/models/sources/bi_silver__sources.yml](../nextgen_bi/models/sources/bi_silver__sources.yml)
 - Khai báo đầy đủ columns
 - Chọn cột khóa ứng viên để gắn test `not_null` + `unique` (nếu phù hợp)

3. Tạo staging model
 - Chọn domain phù hợp: `crm`, `finance`, `hr`, `cx`, `jira`, `noc`, `dim`, `other`
 - Tạo file SQL tại `models/staging/<domain>/stg_bi_silver__<table_name>.sql`
 - Nội dung tối thiểu:

```sql
select *
from {{ source('bi_silver', '<table_name>') }}
```

4. Cập nhật schema cho staging
 - Thêm model vào file schema domain tương ứng, ví dụ:
   - `models/staging/crm/stg_bi_silver__crm__schema.yml`
 - Khai báo `columns` và test cần thiết

5. Validate cục bộ
 - Chạy trong [../nextgen_bi](../nextgen_bi):

```bash
dbt parse --no-partial-parse --profiles-dir .
```

 - Nếu có dữ liệu test phù hợp, chạy thêm:

```bash
dbt test --select stg_bi_silver__<table_name> --profiles-dir .
```

6. Đồng bộ sang Lightdash
 - Vào UI Lightdash
 - Refresh project metadata
 - Kiểm tra Explore mới xuất hiện đúng dimensions/metrics

## 4) Quy trình khi sửa table/model hiện hữu

1. Nếu thêm cột mới ở nguồn
 - Cập nhật DDL
 - Cập nhật `columns` trong source schema
 - Cập nhật schema của staging model tương ứng

2. Nếu đổi tên cột
 - Cập nhật đồng thời source schema, staging SQL, staging schema
 - Rà soát ảnh hưởng dashboard/chart trong Lightdash

3. Nếu xóa cột
 - Đánh giá backward compatibility trước khi xóa
 - Thông báo DA/BI owner dashboard trước khi deploy

## 5) Checklist trước khi merge PR

1. DDL đã cập nhật đúng cú pháp
2. Source schema có đủ columns cho table thay đổi
3. Staging model nằm đúng domain
4. `dbt parse` thành công
5. Test trọng yếu đã chạy (nếu có)
6. Đã ghi rõ impact trong PR description

## 6) Mẫu PR description (gợi ý)

1. Mục tiêu thay đổi
2. Danh sách bảng/model bị ảnh hưởng
3. Test đã chạy và kết quả
4. Ảnh hưởng tới dashboard/lightdash explores
5. Kế hoạch rollback (nếu có)

## 7) Lưu ý vận hành

1. Không commit file chứa secret
2. Tránh gắn `unique` cho cột không đảm bảo tính duy nhất nghiệp vụ
3. Với bảng lớn, ưu tiên validate theo phạm vi model thay vì chạy toàn bộ nếu môi trường hạn chế tài nguyên