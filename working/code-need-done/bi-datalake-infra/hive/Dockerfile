FROM apache/hive:4.2.0

USER root

RUN curl -L https://jdbc.postgresql.org/download/postgresql-42.7.2.jar -o /opt/hive/lib/postgresql-jdbc.jar
# RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar  -o /opt/hive/lib/hadoop-aws.jar
# RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /opt/hive/lib/aws-java-sdk-bundle.jar

# RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o /opt/hive/lib/hadoop-aws-3.4.1.jar
# RUN curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar -o /opt/hive/lib/aws-java-sdk-bundle-1.12.767.jar


RUN curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o /opt/hive/lib/hadoop-aws-3.4.1.jar
RUN curl -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.16/bundle-2.25.16.jar -o /opt/hive/lib/aws-bundle-2.25.16.jar

COPY hive-site.xml /opt/hive/conf/hive-site.xml
RUN chmod 664 /opt/hive/conf/hive-site.xml

USER hive