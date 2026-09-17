#!/bin/bash

chmod 664 hive-site.xml

curl -k -x http://10.255.249.100:3128 https://jdbc.postgresql.org/download/postgresql-42.7.2.jar --output postgresql-42.7.2.jar
# curl  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar  -o hadoop-aws-3.3.4.jar
# curl https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o aws-java-sdk-bundle-1.12.262.jar 

# curl -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o hadoop-aws-3.4.1.jar
# curl -L https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.767/aws-java-sdk-bundle-1.12.767.jar -o aws-java-sdk-bundle-1.12.767.jar

curl -k -x http://10.255.249.100:3128 -L https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar -o hadoop-aws-3.4.1.jar
curl -k -x http://10.255.249.100:3128 -L https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.25.16/bundle-2.25.16.jar -o aws-bundle-2.25.16.jar
# curl -k -x http://10.255.249.100:3128 -L https://repo.anaconda.com/archive/Anaconda3-2025.12-2-Linux-x86_64.sh -o Anaconda3-2025.12-2-Linux-x86_64.sh