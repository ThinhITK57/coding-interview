import bcrypt
import os
import sys

def manage_password(file_path, username, password):
    hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt(10)).decode()
    new_entry = f"{username}:{hashed_password}"
    
    lines = []
    user_found = False
    
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            lines = f.readlines()

    new_lines = []
    for line in lines:
        if line.startswith(f"{username}:"):
            new_lines.append(new_entry + "\n")
            user_found = True
            print(f"[*] Đã cập nhật mật khẩu cho user: {username}")
        else:
            if line.strip(): # Bỏ qua dòng trống
                new_lines.append(line)

    if not user_found:
        new_lines.append(new_entry + "\n")
        print(f"[+] Đã thêm user mới: {username}")

    # Ghi lại vào file
    with open(file_path, 'w') as f:
        f.writelines(new_lines)
    
    try:
        os.chmod(file_path, 0o600)
    except:
        pass

if __name__ == "__main__":
    path = "etc/password.db" 
    
    user = input("Nhập username: ").strip()
    pwd = input(f"Nhập mật khẩu cho '{user}': ").strip()
    
    if user and pwd:
        manage_password(path, user, pwd)
    else:
        print("[!] Username hoặc mật khẩu không được để trống.")