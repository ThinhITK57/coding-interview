#!/bin/bash

# apt-get update && apt-get install -y apache2-utils

htpasswd -b -B -C 10 etc/password.db admin Welcome1