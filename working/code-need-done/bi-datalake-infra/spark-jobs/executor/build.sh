#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# apache-maven-3.9.14
# openjdk 21.0.9 2025-10-21 LTS
# OpenJDK Runtime Environment (build 21.0.9+11-LTS)
# OpenJDK 64-Bit Server VM (build 21.0.9+11-LTS, mixed mode, sharing)
# mvn clean package -DskipTests

# docker build -t spark-client .

# docker-compose up -d --build
docker-compose up -d 