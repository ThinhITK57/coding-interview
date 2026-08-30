# HƯỚNG DẪN CÀI ĐẶT APACHE SPARK TRÊN UBUNTU (ĐÃ CÓ SẴN JAVA)

> Tài liệu hướng dẫn cài đặt Apache Spark trên máy ảo chạy Ubuntu/Linux nhanh chóng, chuẩn hóa thư mục `/opt/spark` và cấu hình biến môi trường qua `.bashrc`.

---

## BƯỚC 1: XÁC ĐỊNH ĐƯỜNG DẪN JAVA (JAVA_HOME)
Vì Ubuntu đã có sẵn Java, bạn cần tìm đường dẫn cài đặt của nó để cấu hình.
1. Mở Terminal trên Ubuntu.
2. Kiểm tra Java đã cài đặt chưa:
   ```bash
   java -version
   ```
3. Chạy lệnh sau để tìm đường dẫn của Java:
   ```bash
   readlink -f $(which java)
   ```
   *Kết quả mẫu:* `/usr/lib/jvm/java-11-openjdk-amd64/bin/java`
4. Bỏ phần `/bin/java` ở cuối, bạn sẽ có đường dẫn **`JAVA_HOME`**.  
   *Ví dụ:* `/usr/lib/jvm/java-11-openjdk-amd64`

---

## BƯỚC 2: TẢI VÀ GIẢI NÉN APACHE SPARK
1. Tải bản Spark mới nhất (ví dụ bản `3.5.1` pre-built cho Hadoop 3.3) trực tiếp bằng `wget`:
   ```bash
   wget https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
   ```
2. Giải nén tệp vừa tải:
   ```bash
   tar -xzf spark-3.5.1-bin-hadoop3.tgz
   ```
3. Di chuyển thư mục giải nén vào thư mục quản lý phần mềm hệ thống `/opt/spark`:
   ```bash
   sudo mv spark-3.5.1-bin-hadoop3 /opt/spark
   ```

---

## BƯỚC 3: THIẾT LẬP BIẾN MÔI TRƯỜNG TRÊN UBUNTU
1. Mở file cấu hình shell `.bashrc` bằng nano hoặc vi:
   ```bash
   nano ~/.bashrc
   ```
2. Di chuyển xuống cuối file và thêm vào các dòng sau (thay thế đường dẫn `JAVA_HOME` bằng đường dẫn thực tế của bạn ở Bước 1):
   ```bash
   # Cấu hình Java & Spark
   export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
   export SPARK_HOME=/opt/spark
   export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
   ```
3. Nhấn `Ctrl + O` rồi `Enter` để lưu, `Ctrl + X` để thoát nano.
4. Áp dụng các thay đổi ngay lập tức cho session hiện tại:
   ```bash
   source ~/.bashrc
   ```

---

## BƯỚC 4: KIỂM TRA HOẠT ĐỘNG (VERIFY)

### 1. Khởi chạy Spark Shell (Scala API)
```bash
spark-shell
```
*Kết quả:* Màn hình hiển thị logo Spark kèm ký tự đợi `scala>` tức là thành công. Gõ `:q` để thoát.

### 2. Khởi chạy PySpark (Python API)
```bash
pyspark
```
*Kết quả:* Trình thông dịch Python của Spark hiển thị cùng ký tự `>>>`. Gõ `quit()` để thoát.

*Lưu ý:* Nếu chạy `pyspark` báo thiếu Python, bạn cần chỉ định rõ phiên bản Python trong `.bashrc`:
```bash
export PYSPARK_PYTHON=python3
```
