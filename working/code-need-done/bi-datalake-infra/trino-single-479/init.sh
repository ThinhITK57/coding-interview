#!/bin/bash

sudo chown -R 1000:1000 /data/single-trino
chmod 664 /data/single-trino
sudo chmod -R 777 /data/single-trino

# wget https://github.com/trinodb/trino/releases/download/438/trino-server-438.tar.gz
wget https://download.oracle.com/java/21/archive/jdk-21.0.9_linux-x64_bin.tar.gz