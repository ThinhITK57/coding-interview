#!/bin/bash

# mkdir -p nginx/certs
# openssl req -x509 -nodes -days 365 \
#   -newkey rsa:2048 \
#   -keyout nginx/certs/privkey.pem \
#   -out nginx/certs/fullchain.pem \
#   -subj "/CN=prefect-external.viettelcyber.com"


# chmod +x extract-ca-from-fullchain.sh
# ./extract-ca-from-fullchain.sh fullchain.pem

# cp ./certs/fullchain.pem cp ./certs/ca.crt

# COPY ca.crt /usr/local/share/ca-certificates/prefect-local.crt
# RUN update-ca-certificates


# openssl genrsa -out prefect.key 2048

# openssl req -new \
#   -key prefect.key \
#   -out prefect.csr \
#   -config san.cnf


# openssl x509 -req \
#   -in prefect.csr \
#   -CA rootCA.crt \
#   -CAkey rootCA.key \
#   -CAcreateserial \
#   -out prefect.crt \
#   -days 825 -sha256 \
#   -extensions req_ext \
#   -extfile san.cnf

openssl req -x509 -nodes -days 365 \
  -newkey rsa:2048 \
  -keyout certs/privkey.pem \
  -out certs/fullchain.pem \
  -config san.cnf
