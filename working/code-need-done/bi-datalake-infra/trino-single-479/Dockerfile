USER root

# Tạo thư mục chứa lib nếu chưa có (thường bản 438 đã có sẵn thư mục này)
RUN mkdir -p /usr/lib/trino/plugin/hive

# Tải Hadoop AWS (Giao diện S3A)
RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    -o /usr/lib/trino/plugin/hive/hadoop-aws.jar

# Tải AWS SDK Bundle (Chứa các Class cần thiết để gọi API S3)
RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    -o /usr/lib/trino/plugin/hive/aws-java-sdk-bundle.jar

# Phân quyền cho user trino (Bắt buộc để Trino nạp được Class)
RUN chown -R trino:trino /usr/lib/trino/plugin/hive/ && \
    chmod 644 /usr/lib/trino/plugin/hive/*.jar

USER trino