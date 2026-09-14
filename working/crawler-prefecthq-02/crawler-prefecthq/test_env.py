import pandas as pd
import yaml
import re

def normalize_key(text):
    """
    Hàm chuẩn hóa tên trường thông minh: 
    Xử lý tốt cả PascalCase, camelCase, snake_case, khoảng trắng hay dấu gạch ngang.
    Ví dụ: 'C_Assignor' -> 'c_assignor'
           'C_EssentialWeightTotalPercent' -> 'c_essential_weight_total_percent'
    """
    if pd.isna(text):
        return ""
    s = str(text).strip()
    # Tách chuỗi theo dấu gạch dưới, khoảng trắng, dấu gạch ngang hoặc ranh giới chữ hoa/chữ thường
    parts = re.split(r'[_.\s\-]+|(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])', s)
    return '_'.join([p.lower() for p in parts if p])

# ==========================================
# 1. ĐỊNH NGHĨA 5 MẢNG CẤU TRÚC CỘT TỪ FILE SQL
# ==========================================
schema_definitions = {
    "assignments": [
        "sysid", "name", "description", "created_by", "c_assignor", "c_assignee", 
        "c_start_date", "c_end_date", "c_parent_assignment", "c_department", 
        "entity_owner", "c_achievement_rate", "c_total_weight", "c_sum_target_weight_percent", 
        "last_updated_on", "last_updated_by_system_on", "created_on", "external_id", 
        "entity_type", "c_total_assignment_weight_result"
    ],
    "objective": [
        "sysid", "name", "description", "created_by", "c_assignee", "start_date", 
        "end_date", "parent_objective", "state", "status", "c_department", 
        "c_objective_type", "entity_owner", "last_updated_on", "last_updated_by_system_on", 
        "created_on", "weight", "percent_of_plan", "actual_percent", "planned_percent", 
        "external_id", "entity_type"
    ],
    "projects": [
        "sysid", "name", "project_type", "track_status", "percent_completed", 
        "c_department", "c_assignee", "created_by", "project_manager", "c_action_resources", 
        "state", "parent", "parent_project", "manager", "c_associated_objective", 
        "last_updated_on", "last_updated_by_system_on", "created_on", "entity_owner", 
        "external_id", "entity_type"
    ],
    "targets": [
        "sysid", "name", "description", "c_update_description", "unit", "associated_objective", 
        "c_associated_assignment", "parent_target", "target_type", "c_assignee", "created_by", 
        "c_department", "c_teams", "c_target_value_m", "c_target_date_m", "c_target_result_value_m", 
        "c_target_value_n", "c_target_date_n", "c_target_result_value_n", "c_target_value_s", 
        "c_target_date_s", "c_target_result_value_s", "state", "status", "associated_item", 
        "percent_completed", "weight", "last_updated_on", "last_updated_by_system_on", 
        "created_on", "entity_owner", "external_id", "entity_type", "c_assignment_weight"
    ],
    "tasks": [
        "sysid", "name", "task_type", "description", "entity_owner", "parent_project", 
        "parent", "external_id", "work", "duration", "priority", "start_date", "due_date", 
        "state", "project", "percent_completed", "actual_duration", "actual_effort", 
        "actual_start_date", "actual_end_date", "track_status", "last_updated_on", 
        "created_by", "created_on", "last_updated_by_system_on"
    ]
}

# ==========================================
# 2. ĐỌC FILE MÔ TẢ CHI TIẾT (File Excel)
# ==========================================
detail_excel_path = 'data/da_more/EPM_Data_Dictionary_Summary.xlsx' 
detail_sheets = pd.read_excel(detail_excel_path, sheet_name=None)

mapping_descriptions = {}

for sheet_name, df in detail_sheets.items():
    tbl_name = str(sheet_name).strip().lower().replace(' ', '_')
    # Xử lý nếu tên sheet là 'targets' nhưng bảng là 'target' (chuẩn hóa tương đối)
    if tbl_name.endswith('s'):
        tbl_name_alt = tbl_name[:-1]
    else:
        tbl_name_alt = tbl_name + 's'
        
    # Tìm linh hoạt tên các cột chứa tên trường và ý nghĩa nghiệp vụ
    col_name_key = next((c for c in df.columns if 'trường' in c.lower() or 'field' in c.lower() or 'name' in c.lower()), 'Tên trường')
    col_desc_key = next((c for c in df.columns if 'ý nghĩa' in c.lower() or 'mô tả' in c.lower() or 'description' in c.lower()), 'Ý nghĩa nghiệp vụ')
    
    if col_name_key in df.columns and col_desc_key in df.columns:
        for _, row in df.iterrows():
            raw_field = row[col_name_key]
            raw_desc = row[col_desc_key]
            
            if not pd.isna(raw_field):
                norm_field = normalize_key(raw_field)
                desc_text = str(raw_desc).strip() if not pd.isna(raw_desc) else ""
                
                # Lưu vào mapping theo cả tên sheet chuẩn và tên sheet thay thế (số ít/số nhiều)
                mapping_descriptions[(tbl_name, norm_field)] = desc_text
                mapping_descriptions[(tbl_name_alt, norm_field)] = desc_text
                # Lưu thêm dạng global (bất kể tên bảng) để phòng hờ lệch tên sheet
                mapping_descriptions[norm_field] = desc_text

# ==========================================
# 3. XỬ LÝ VÀ ÁNH XẠ DỰA TRÊN 5 MẢNG CỐ ĐỊNH
# ==========================================
tables_list = []

for table_name, columns in schema_definitions.items():
    columns_list = []
    
    for col_name in columns:
        norm_col_name = normalize_key(col_name)
        
        # 1. Thử tìm chính xác theo (table_name, norm_col_name)
        matched_desc = mapping_descriptions.get((table_name, norm_col_name))
        
        # 2. Nếu không thấy, tìm theo tên trường độc lập (global)
        if not matched_desc:
            matched_desc = mapping_descriptions.get(norm_col_name)
        
        # 3. Fallback cuối cùng nếu trong file Excel hoàn toàn không có
        if not matched_desc:
            matched_desc = f"Column {col_name} in {table_name}"
            
        col_dict = {
            'name': col_name,
            'description': matched_desc
        }
        
        # Tự động gán test not_null và unique cho trường sysid
        if col_name == 'sysid':
            col_dict['tests'] = ['not_null', 'unique']
            
        columns_list.append(col_dict)
        
    table_data = {
        'name': table_name,
        'description': f"This table stores detailed information for {table_name}.",
        'columns': columns_list
    }
    
    tables_list.append(table_data)

# ==========================================
# 4. XUẤT RA FILE YAML CHUẨN MULTI-TABLES
# ==========================================
yaml_data = {
    'version': 2,
    'sources': [
        {
            'name': 'bi_silver__other',
            'database': 'hive',
            'schema': "{{ env_var('DBT_TRINO_SCHEMA', 'bi_silver') }}",
            'description': 'Silver layer sources for domain: other',
            'tables': tables_list
        }
    ]
}

output_filename = 'all_tables_mapped.yml'
with open(output_filename, 'w', encoding='utf-8') as f:
    yaml.dump(yaml_data, f, allow_unicode=True, sort_keys=False, default_flow_style=False)
    
print(f"✅ Đã cập nhật lại code và tạo thành công file YAML chính xác mô tả: {output_filename}")