#!/bin/bash
set -e

# Usage:    
# 1. Export image: bash docker-image.sh export spark-client spark-client.tar.gz
# 2. Import image: bash docker-image.sh import spark-client spark-client.tar.gz

CMD=$1
IMAGE=$2
FILE=${3:-image.tar}

if [ -z "$CMD" ] || [ -z "$IMAGE" ]; then
  echo "Usage:"
  echo "  $0 export <image_name> [file.tar.gz]"
  echo "  $0 import <image_name> [file.tar.gz]"
  exit 1
fi

case "$CMD" in

  export)
    echo "📦 Exporting image: $IMAGE → $FILE"
    #docker save -o "$FILE" "$IMAGE"
    docker save "$IMAGE" | gzip > "$FILE"
    echo "✅ Done"
    ;;

  import)
    echo "📥 Importing image from: $FILE"

    if [ ! -f "$FILE" ]; then
      echo "❌ File not found: $FILE"
      exit 1
    fi

    # docker load -i "$FILE"
    gzip -dc "$FILE" | docker load
    echo "✅ Done"
    ;;

  *)
    echo "❌ Unknown command: $CMD"
    exit 1
    ;;

esac