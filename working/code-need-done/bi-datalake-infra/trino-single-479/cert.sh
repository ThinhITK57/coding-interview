#!/bin/bash

keytool -genkeypair \
        -alias trino \
        -keyalg RSA \
        -keystore etc/keystore.jks \
        -validity 365 \
        -storepass Welcome1 \
        -keypass Welcome1 \
        -dname "CN=localhost, OU=Data, O=Mining, L=Hanoi, S=VN, C=VN"