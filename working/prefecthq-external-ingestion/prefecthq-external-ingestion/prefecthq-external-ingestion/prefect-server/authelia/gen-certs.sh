#!/bin/bash

# docker run --rm authelia/authelia:latest authelia hash-password "Welcome1"
# docker run --rm authelia/authelia:latest authelia crypto hash generate argon2  --password "Welcome1"

mkdir -p secrets

openssl rand -hex 32 > secrets/jwt_secret
openssl rand -hex 32 > secrets/session_secret
openssl rand -hex 32 > secrets/storage_encryption_key
