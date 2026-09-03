# HƯỚNG DẪN CÀI ĐẶT APACHE SPARK TRÊN WINDOWS (ĐÃ CÓ SẴN JAVA)

> Tài liệu hướng dẫn chi tiết từng bước cài đặt local Spark trên hệ điều hành Windows để học tập, phát triển và chạy thử PySpark/Spark-Shell.

---

## BƯỚC 1: KIỂM TRA PHIÊN BẢN JAVA HIỆN TẠI
Spark 3.x tương thích tốt nhất với **Java 8, 11 hoặc 17**. 
1. Mở Terminal (Command Prompt hoặc PowerShell).
2. Chạy lệnh:
   ```cmd
   java -version
   ```
3. Đảm bảo bạn nhìn thấy thông tin phiên bản Java (ví dụ: `build 11.0.x` hoặc `build 1.8.x`).
4. Lấy đường dẫn thư mục cài đặt Java (Ví dụ: `C:\Program Files\Java\jdk-11`). Đây chính là **`JAVA_HOME`**.

---

## BƯỚC 2: TẢI VÀ GIẢI NÉN APACHE SPARK
1. Truy cập trang chủ: [spark.apache.org/downloads](https://spark.apache.org/downloads.html).
2. Chọn phiên bản:
   * **Choose a Spark release:** `3.5.x` (hoặc bản mới nhất).
   * **Choose a package type:** `Pre-built for Apache Hadoop 3.3 and later`.
3. Click vào link download file `.tgz` (Ví dụ: `spark-3.5.1-bin-hadoop3.tgz`).
4. Giải nén file `.tgz` bằng phần mềm 7-Zip hoặc WinRAR.
5. Di chuyển thư mục đã giải nén vào ổ `C:\` và đổi tên ngắn gọn thành: **`C:\spark`**.

---

## BƯỚC 3: CÀI ĐẶT HADOOP WINUTILS (BẮT BUỘC TRÊN WINDOWS)
Spark cần các thư viện hệ thống của Hadoop để chạy trên Windows. Nếu không có `winutils.exe`, Spark sẽ báo lỗi `java.io.IOException: Could not locate executable null\bin\winutils.exe`.

1. Tạo một thư mục mới tại ổ `C:\` có tên: **`C:\hadoop`**.
2. Tạo thư mục **`bin`** bên trong: **`C:\hadoop\bin`**.
3. Truy cập vào kho lưu trữ winutils uy tín trên GitHub: [github.com/cdarlint/winutils](https://github.com/cdarlint/winutils).
4. Tìm đến thư mục tương ứng với phiên bản Hadoop của bản Spark bạn vừa tải (ví dụ: **`hadoop-3.3.0`** hoặc tương đương) và vào thư mục `bin`.
5. Tải file **`winutils.exe`** và **`hadoop.dll`** về.
6. Copy cả 2 file này vào thư mục **`C:\hadoop\bin`**.

---

## BƯỚC 4: THIẾT LẬP BIẾN MÔI TRƯỜNG (ENVIRONMENT VARIABLES)
1. Nhấn nút **Windows**, gõ tìm kiếm `env` và chọn **Edit the system environment variables**.
2. Click vào nút **Environment Variables...** ở góc dưới.
3. Tại ô **System variables** (phía dưới), click **New...** để tạo 3 biến sau:

| Variable Name | Variable Value (Đường dẫn thực tế trên máy) |
|:---|:---|
| **`SPARK_HOME`** | `C:\spark` |
| **`HADOOP_HOME`** | `C:\hadoop` |
| **`JAVA_HOME`** | `C:\Program Files\Java\jdk-11` *(Trỏ đúng thư mục JDK của bạn)* |

4. Tìm biến **`Path`** trong ô **System variables**, chọn nó và click **Edit...**.
5. Click **New** và thêm vào 2 dòng sau:
   * `%SPARK_HOME%\bin`
   * `%HADOOP_HOME%\bin`
6. Click **OK** để lưu lại toàn bộ cấu hình.

---

## BƯỚC 5: KIỂM TRA HOẠT ĐỘNG (VERIFY)
Mở một cửa sổ Command Prompt hoặc PowerShell **mới** (để cập nhật biến môi trường mới):

### 1. Kiểm tra Spark Shell (Scala API)
Chạy lệnh:
```cmd
spark-shell
```
*Kết quả:* Màn hình chào mừng của Spark hiện lên cùng ký tự `scala>` tức là thành công. Gõ `:q` để thoát.

### 2. Kiểm tra PySpark (Python API)
Chạy lệnh:
```cmd
pyspark
```
*Kết quả:* Trình thông dịch Python của Spark hiện lên cùng ký tự `>>>`. Gõ `quit()` để thoát.

---

## MẸO XỬ LÝ LỖI PHỔ BIẾN TRÊN WINDOWS
*   **Lỗi: `JAVA_HOME is not set`**  
    *Khắc phục:* Kiểm tra lại xem đường dẫn `JAVA_HOME` có khoảng trắng hay không hoặc viết sai chính tả. Đảm bảo thư mục cài đặt Java chứa thư mục con `bin`.
*   **Lỗi: `Illegal character in path`**  
    *Khắc phục:* Đảm bảo không có ký tự lạ hoặc khoảng trắng trong đường dẫn của `SPARK_HOME` hay `HADOOP_HOME`. Tránh đặt trong các thư mục như `C:\Program Files\`. Nên đặt trực tiếp ngoài `C:\`.
