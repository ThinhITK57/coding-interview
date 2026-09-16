#!/bin/bash

# ENV HTTP_PROXY=http://10.255.249.100:3128
# ENV HTTPS_PROXY=http://10.255.249.100:3128
# ENV http_proxy=http://10.255.249.100:3128
# ENV https_proxy=http://10.255.249.100:3128


docker build --network=host --no-cache \
  --build-arg HTTP_PROXY=http://192.168.5.8:3128 \
  --build-arg HTTPS_PROXY=http://192.168.5.8:3128 \
  --build-arg http_proxy=http://192.168.5.8:3128 \
  --build-arg https_proxy=http://192.168.5.8:3128 \
  -t ingestions:latest  -f Dockerfile.base .

# docker build -t ingestions:latest -f Dockerfile.base .
docker save ingestions:latest | gzip > ingestions_latest.tar.gz

scp namtv40@192.168.47.128:/home/namtv40/prefect-external/ingestions/ingestions_latest.tar.gz ingestions_latest.tar.gz
# gunzip -c ingestions_latest.tar.gz | docker load
# docker load -i  ingestions_latest.tar