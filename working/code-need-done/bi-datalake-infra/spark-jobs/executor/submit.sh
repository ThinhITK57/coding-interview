#!/bin/bash
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

# Example: docker exec -it spark-client entrypoint.sh shell
# docker exec -it spark-client entrypoint.sh bash


docker exec -it spark-client entrypoint.sh run jobs/example.py '{"only_even": true}'