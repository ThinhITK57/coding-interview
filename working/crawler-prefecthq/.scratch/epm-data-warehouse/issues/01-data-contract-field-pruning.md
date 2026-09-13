# 01: Data Contract & Field Pruning — Thu gọn Schema theo DA Scope

**What to build:** Khi pipeline chạy, thay vì ingest toàn bộ 364 cột (Project) hay 256 cột (Task), hệ thống chỉ giữ lại đúng tập cột mà DA yêu cầu + các cột kỹ thuật (PK, FK, watermark, audit). Dữ liệu sau pruning vẫn đi qua đúng luồng `read_conformed → conform → deduplicate → TriStorageSink` mà không phá vỡ bất kỳ module nào đang chạy production.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

---

## Bối cảnh & Lý do

DA cung cấp danh sách thu gọn cho 5 bảng (tổng ~60 cột hữu ích), nhưng pipeline hiện tại đang ingest TOÀN BỘ schema từ `data_type/*.sql`:

| Bảng | Cột trong contract hiện tại | Cột DA yêu cầu | Cột kỹ thuật cần giữ | Tổng sau pruning |
|------|---------------------------|----------------|---------------------|-----------------|
| Project | 364 | 9 | ~12 (SYSID, State, TrackStatus, Parent, ParentProject, Manager, CreatedBy, LastUpdatedOn, LastUpdatedBySystemOn, C_AssociatedObjective, CreatedOn, ExternalID) | ~21 |
| Task | 256 | 14 | ~11 (SYSID, State, Parent, ParentProject, Project, CreatedBy, LastUpdatedOn, LastUpdatedBySystemOn, CreatedOn, ExternalID, PercentCompleted) | ~25 |
| Objective/BSC | 51 | 8 | ~10 (SYSID, State, Status, ParentObjective, CreatedBy, LastUpdatedOn, LastUpdatedBySystemOn, CreatedOn, ExternalID, EntityOwner) | ~18 |
| Assignment | 39 | 8 | ~10 (SYSID, CreatedBy, LastUpdatedOn, LastUpdatedBySystemOn, CreatedOn, ExternalID, C_ParentAssignment, EntityOwner) | ~18 |
| Target | 116 | 21 | ~10 (SYSID, State, Status, ParentTarget, AssociatedObjective, AssociatedItem, CreatedBy, LastUpdatedOn, LastUpdatedBySystemOn, CreatedOn) | ~31 |

**Lợi ích:**
1. Giảm ~85% dung lượng Parquet (từ 364 → 21 cột cho Project)
2. Spark shuffle/sort nhanh hơn đáng kể (ít cột = ít bytes trên wire)
3. Schema ổn định, dễ kiểm soát, dễ debug
4. DDL Trino/Zeppelin gọn, GenBI context pack chính xác hơn

---

## Chiến lược triển khai

### Phương án chọn: Pruned Contract Files (Tách riêng, không sửa file gốc)

Tạo thư mục mới `data_type_pruned/` chứa 5 file `*_dataType.sql` chỉ gồm các cột cần thiết. `SchemaContract` sẽ nhận thêm tham số `contract_dir` để chuyển đổi giữa full contract (dev/debug) và pruned contract (production).

**Tại sao không sửa trực tiếp `data_type/*.sql`:**
- File gốc là "nguồn sự thật" phản ánh toàn bộ API schema → giữ nguyên để reference
- Khi DA bổ sung cột mới, chỉ cần thêm vào `data_type_pruned/` mà không cần đối chiếu lại toàn bộ API schema
- Rollback đơn giản: chuyển `contract_dir` về `./data_type` là quay lại full schema

### Mapping DA Field → Clarizen API Field

#### Project (9 DA fields → 21 pruned columns)
| DA Field | API Column | Type | Role |
|----------|-----------|------|------|
| Name | `Name` | STRING | Dimension |
| Project Type | `ProjectType` | STRING | Dimension |
| Status | `TrackStatus` | STRING | Dimension |
| % Complete | `PercentCompleted` | DOUBLE | Measure |
| Department | `C_Department` | STRING | FK → dim_department |
| Assignee | `C_Assignee` | STRING | FK → dim_resource |
| Assignor | `CreatedBy` | STRING | FK → dim_resource |
| Project Manager | `ProjectManager` | STRING | FK → dim_resource |
| Resources | `C_ActionResources` | STRING | FK → dim_resource |
| *(kỹ thuật)* | `SYSID` | STRING | PK |
| *(kỹ thuật)* | `State` | STRING | Internal status |
| *(kỹ thuật)* | `Parent` | STRING | FK self-ref |
| *(kỹ thuật)* | `ParentProject` | STRING | FK self-ref |
| *(kỹ thuật)* | `Manager` | STRING | FK alt |
| *(kỹ thuật)* | `C_AssociatedObjective` | STRING | FK → BSC |
| *(kỹ thuật)* | `LastUpdatedOn` | DATE | Watermark |
| *(kỹ thuật)* | `LastUpdatedBySystemOn` | DATE | Audit |
| *(kỹ thuật)* | `CreatedOn` | DATE | Audit |
| *(kỹ thuật)* | `EntityOwner` | STRING | Ownership |
| *(kỹ thuật)* | `ExternalID` | STRING | Integration |
| *(kỹ thuật)* | `EntityType` | STRING | Type marker |

#### Task (14 DA fields → ~25 pruned columns)
| DA Field | API Column | Type | Role |
|----------|-----------|------|------|
| Task Type | `TaskType` | STRING | Dimension |
| Assignee | `EntityOwner` | STRING | FK → dim_resource |
| Department | *(infer từ Project.C_Department)* | — | Join-time |
| Resources | *(infer từ parent project)* | — | Join-time |
| Parent Project | `ParentProject` | STRING | FK → dim_project |
| Parent | `Parent` | STRING | FK self-ref |
| Jira Status | `ExternalID` | STRING | Integration |
| Name | `Name` | STRING | Dimension |
| Description | `Description` | STRING | Dimension |
| Work | `Work` | DOUBLE | Measure |
| Duration | `Duration` | DOUBLE | Measure |
| Priority (custom) | `Priority` | DOUBLE | Measure/Dim |
| Start Date | `StartDate` | DATE | FK → dim_date |
| Due Date | `DueDate` | DATE | FK → dim_date |
| *(kỹ thuật)* | `SYSID` | STRING | PK |
| *(kỹ thuật)* | `State` | STRING | Internal status |
| *(kỹ thuật)* | `Project` | STRING | FK → dim_project |
| *(kỹ thuật)* | `PercentCompleted` | DOUBLE | Measure |
| *(kỹ thuật)* | `ActualDuration` | DOUBLE | Measure |
| *(kỹ thuật)* | `ActualEffort` | DOUBLE | Measure |
| *(kỹ thuật)* | `ActualStartDate` | DATE | Measure |
| *(kỹ thuật)* | `ActualEndDate` | DATE | Measure |
| *(kỹ thuật)* | `TrackStatus` | STRING | Status flag |
| *(kỹ thuật)* | `LastUpdatedOn` | DATE | Watermark |
| *(kỹ thuật)* | `CreatedBy` | STRING | Audit |
| *(kỹ thuật)* | `CreatedOn` | DATE | Audit |

#### Objective/BSC (8 DA fields → ~18 pruned columns)
| DA Field | API Column | Type | Role |
|----------|-----------|------|------|
| Name | `Name` | STRING | Dimension |
| Description | `Description` | STRING | Dimension |
| Assignor | `CreatedBy` | STRING | FK → dim_resource |
| Assignee | `C_Assignee` | STRING | FK → dim_resource |
| Start Date | `StartDate` | DATE | FK → dim_date |
| End Date | `EndDate` | DATE | FK → dim_date |
| Targets | *(derived từ Target table join)* | — | Join-time |
| Parent Objective | `ParentObjective` | STRING | FK self-ref |
| *(kỹ thuật)* | `SYSID` | STRING | PK |
| *(kỹ thuật)* | `State` | STRING | Status |
| *(kỹ thuật)* | `Status` | STRING | Status |
| *(kỹ thuật)* | `C_Department` | STRING | FK → dim_department |
| *(kỹ thuật)* | `C_ObjectiveType` | STRING | Type dim |
| *(kỹ thuật)* | `EntityOwner` | STRING | Ownership |
| *(kỹ thuật)* | `LastUpdatedOn` | DATE | Watermark |
| *(kỹ thuật)* | `LastUpdatedBySystemOn` | DATE | Audit |
| *(kỹ thuật)* | `CreatedOn` | DATE | Audit |
| *(kỹ thuật)* | `Weight` | DOUBLE | Measure |

#### Assignment (8 DA fields → ~18 pruned columns)
| DA Field | API Column | Type | Role |
|----------|-----------|------|------|
| Name | `Name` | STRING | Dimension |
| Description | `Description` | STRING | Dimension |
| Assignor | `CreatedBy` / `C_Assignor` | STRING | FK → dim_resource |
| Assignee | `C_Assignee` | STRING | FK → dim_resource |
| Start Date | `C_StartDate` | DATE | FK → dim_date |
| End Date | `C_EndDate` | DATE | FK → dim_date |
| Targets | *(derived từ Target table join)* | — | Join-time |
| Parent Assignment | `C_ParentAssignment` | STRING | FK self-ref |
| *(kỹ thuật)* | `SYSID` | STRING | PK |
| *(kỹ thuật)* | `C_Department` | STRING | FK → dim_department |
| *(kỹ thuật)* | `EntityOwner` | STRING | Ownership |
| *(kỹ thuật)* | `C_AchievementRate` | DOUBLE | Measure |
| *(kỹ thuật)* | `C_TotalWeight` | DOUBLE | Measure |
| *(kỹ thuật)* | `C_SumTargetWeightPercent` | DOUBLE | Measure |
| *(kỹ thuật)* | `LastUpdatedOn` | DATE | Watermark |
| *(kỹ thuật)* | `LastUpdatedBySystemOn` | DATE | Audit |
| *(kỹ thuật)* | `CreatedOn` | DATE | Audit |
| *(kỹ thuật)* | `ExternalID` | STRING | Integration |

#### Target (21 DA fields → ~31 pruned columns)
| DA Field | API Column | Type | Role |
|----------|-----------|------|------|
| Name | `Name` | STRING | Dimension |
| Description | `Description` | STRING | Dimension |
| Update Description | `C_UpdateDescription` | STRING | Dimension |
| Unit | `Unit` | STRING | Dimension |
| Associated Objective | `AssociatedObjective` | STRING | FK → dim_objective |
| Associated Assignment | `C_AssociatedAssignment` | STRING | FK → dim_assignment |
| Parent Target | `ParentTarget` | STRING | FK self-ref |
| Target Type | `TargetType` | STRING | Dimension |
| Assignee | `C_Assignee` | STRING | FK → dim_resource |
| Assignor | `CreatedBy` | STRING | FK → dim_resource |
| Department | `C_Department` | STRING | FK → dim_department |
| Resources | `C_Teams` | STRING | FK → dim_resource |
| Target Value M | `C_TargetValueM` | DOUBLE | Measure |
| Target Date M | `C_TargetDateM` | DATE | Measure |
| Target Result M | `C_TargetResultValueM` | DOUBLE | Measure |
| Target Value N | `C_TargetValueN` | DOUBLE | Measure |
| Target Date N | `C_TargetDateN` | DATE | Measure |
| Target Result N | `C_TargetResultValueN` | DOUBLE | Measure |
| Target Value C/S | `C_TargetValueS` | DOUBLE | Measure |
| Target Date C/S | `C_TargetDateS` | DATE | Measure |
| Target Result C/S | `C_TargetResultValueS` | DOUBLE | Measure |
| *(kỹ thuật)* | `SYSID` | STRING | PK |
| *(kỹ thuật)* | `State` | STRING | Status |
| *(kỹ thuật)* | `Status` | STRING | Status |
| *(kỹ thuật)* | `AssociatedItem` | STRING | FK → Project |
| *(kỹ thuật)* | `PercentCompleted` | DOUBLE | Measure |
| *(kỹ thuật)* | `Weight` | DOUBLE | Measure |
| *(kỹ thuật)* | `LastUpdatedOn` | DATE | Watermark |
| *(kỹ thuật)* | `LastUpdatedBySystemOn` | DATE | Audit |
| *(kỹ thuật)* | `CreatedOn` | DATE | Audit |
| *(kỹ thuật)* | `CreatedBy` | STRING | Audit |

---

## Tác động lên các Module hiện tại

| Module | Tác động | Hành động |
|--------|---------|----------|
| `transform/schema_contract.py` | Thêm tham số `contract_dir` vào `__init__` và `load()` | Đã hỗ trợ sẵn qua `resolve_path(table_name, contract_dir)` — chỉ cần truyền đúng path |
| `transform/contract_conformer.py` | Không đổi — `conform()` tự handle missing columns bằng `F.lit(None)` | Không cần sửa |
| `config.json` (field list trong body) | Cần thu gọn field list trong `"fields"` của mỗi endpoint | Giảm payload API response, tăng tốc extraction |
| `storage/trino_ddl_generator.py` | DDL tự sinh từ contract → tự động gọn theo pruned contract | Không cần sửa |
| `storage/spark_sql_ddl_generator.py` | Tương tự Trino DDL | Không cần sửa |
| `transform/docstring_registry.py` | dbt schema.yml tự sinh từ DataFrame columns | Tự động gọn |
| `transform/genbi_context_packer.py` | Context pack sinh từ columns metadata | Tự động gọn, chính xác hơn |
| `prefect_flow.py` | Truyền `contract_dir` vào `SchemaContract.load()` | Thêm config param |

---

## Acceptance Criteria

- [x] Thư mục `data_type_pruned/` chứa 5 file `*_dataType.sql` mới với đúng tập cột DA + kỹ thuật
- [x] `config.json` và `SchemaContract` tự động ưu tiên nạp schema tinh gọn qua `DEFAULT_CONTRACT_DIR`
- [x] Pipeline chạy được với pruned contract: `read_conformed → conform → dedup → sink` không lỗi
- [x] DDL Trino và Spark SQL sinh ra từ pruned contract match đúng schema mới
- [x] GenBI context pack và dbt schema.yml tương thích với pruned scope
- [x] File `data_type/` gốc không bị sửa — vẫn giữ nguyên làm reference
- [x] Test: chạy `SchemaContract.load("projects")` load đúng 21 cột thay vì 364
- [x] Documentation: Đã tạo `data_type_pruned/README.md` giải thích chi tiết cột và cơ chế switch contract
