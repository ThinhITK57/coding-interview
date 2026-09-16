import os
import zipfile
import uuid
from datetime import datetime
from pathlib import Path

def zip_current_folder(ignore_list=None):
    if ignore_list is None:
        ignore_list = {'.git', 'venv', '__pycache__', '.env', '.pytest_cache', '.idea', '.vscode'}

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    zip_name = f"{timestamp}_{unique_id}.zip"
    
    parent_dir = os.path.dirname(os.getcwd())
    zip_path = os.path.join(parent_dir, zip_name)

    print(f"📦 Bắt đầu nén thư mục hiện tại vào: {zip_path}")

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk('.'):
            dirs[:] = [d for d in dirs if d not in ignore_list]
            
            for file in files:
                if file in ignore_list or file.endswith('.zip'):
                    continue
                
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, '.')
                
                zipf.write(file_path, arcname)
                print(f"  + Thêm: {arcname}")

    print(f"✅ Đã tạo xong: {zip_path}")
    return zip_path

if __name__ == "__main__":
    my_ignores = {'.git', 'venv', '__pycache__', 'node_modules', 'large_data_folder', 'pack_folder.py', '.env', 'docs'}
    path_to_upload = zip_current_folder(ignore_list=my_ignores)
    
    print(f"Sẵn sàng upload: {path_to_upload}")